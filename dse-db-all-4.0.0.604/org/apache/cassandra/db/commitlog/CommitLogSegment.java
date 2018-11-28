package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.CRC32;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IntegerInterval;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.jctools.maps.NonBlockingHashMap;

public abstract class CommitLogSegment {
   private static final long idBase;
   private CommitLogSegment.CDCState cdcState;
   Object cdcStateLock;
   private static final AtomicInteger nextId = new AtomicInteger(1);
   private static long replayLimitId;
   public static final int ENTRY_OVERHEAD_SIZE = 12;
   static final int SYNC_MARKER_SIZE = 8;
   private final OpOrder appendOrder;
   private final AtomicInteger allocatePosition;
   private volatile int lastSyncedOffset;
   private int endOfBuffer;
   private final WaitQueue syncComplete;
   private final NonBlockingHashMap<TableId, IntegerInterval> tableDirty;
   private final ConcurrentHashMap<TableId, IntegerInterval.Set> tableClean;
   public final long id;
   final File logFile;
   final FileChannel channel;
   final int fd;
   protected final AbstractCommitLogSegmentManager manager;
   ByteBuffer buffer;
   private volatile boolean headerWritten;
   public final CommitLogDescriptor descriptor;

   static CommitLogSegment createSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      CommitLog.Configuration config = commitLog.configuration;
      CommitLogSegment segment = config.useEncryption()?new EncryptedSegment(commitLog, manager):(config.useCompression()?new CompressedSegment(commitLog, manager):(DatabaseDescriptor.getCommitlogAccessMode() == Config.AccessMode.standard?new UncompressedSegment(commitLog, manager):new MemoryMappedSegment(commitLog, manager)));
      ((CommitLogSegment)segment).writeLogHeader();
      return (CommitLogSegment)segment;
   }

   static boolean usesBufferPool(CommitLog commitLog) {
      return true;
   }

   static long getNextId() {
      return idBase + (long)nextId.getAndIncrement();
   }

   CommitLogSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      this.cdcState = CommitLogSegment.CDCState.PERMITTED;
      this.cdcStateLock = new Object();
      this.appendOrder = TPC.newOpOrder(this);
      this.allocatePosition = new AtomicInteger();
      this.syncComplete = new WaitQueue();
      this.tableDirty = new NonBlockingHashMap(1024);
      this.tableClean = new ConcurrentHashMap();
      this.manager = manager;
      this.id = getNextId();
      this.descriptor = new CommitLogDescriptor(this.id, commitLog.configuration.getCompressorClass(), commitLog.configuration.getEncryptionContext());
      this.logFile = new File(manager.storageDirectory, this.descriptor.fileName());

      try {
         this.channel = FileChannel.open(this.logFile.toPath(), new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE});
         this.fd = NativeLibrary.getfd(this.channel);
      } catch (IOException var4) {
         throw new FSWriteError(var4, this.logFile);
      }

      this.buffer = this.createBuffer(commitLog);
   }

   void writeLogHeader() {
      CommitLogDescriptor.writeHeader(this.buffer, this.descriptor, this.additionalHeaderParameters());
      this.endOfBuffer = this.buffer.capacity();
      this.lastSyncedOffset = this.buffer.position();
      this.allocatePosition.set(this.lastSyncedOffset + 8);
      this.headerWritten = true;
   }

   protected Map<String, String> additionalHeaderParameters() {
      return Collections.emptyMap();
   }

   abstract ByteBuffer createBuffer(CommitLog var1);

   CommitLogSegment.Allocation allocate(Mutation mutation, int size) {
      OpOrder.Group opGroup = this.appendOrder.start();

      try {
         int position = this.allocate(size);
         if(position < 0) {
            opGroup.close();
            return null;
         } else {
            this.markDirty(mutation, position);
            return new CommitLogSegment.Allocation(this, opGroup, position, (ByteBuffer)this.buffer.duplicate().position(position).limit(position + size));
         }
      } catch (Throwable var5) {
         opGroup.close();
         throw var5;
      }
   }

   static boolean shouldReplay(String name) {
      return CommitLogDescriptor.fromFileName(name).id < replayLimitId;
   }

   static void resetReplayLimit() {
      replayLimitId = getNextId();
   }

   private int allocate(int size) {
      int prev;
      int next;
      do {
         prev = this.allocatePosition.get();
         next = prev + size;
         if(next >= this.endOfBuffer) {
            return -1;
         }
      } while(!this.allocatePosition.compareAndSet(prev, next));

      assert this.buffer != null;

      return prev;
   }

   void discardUnusedTail() {
      OpOrder.Group group = this.appendOrder.start();
      Throwable var2 = null;

      while(true) {
         try {
            int prev = this.allocatePosition.get();
            int next = this.endOfBuffer + 1;
            if(prev >= next) {
               assert this.buffer == null || prev == this.buffer.capacity() + 1;

               return;
            }

            if(!this.allocatePosition.compareAndSet(prev, next)) {
               continue;
            }

            this.endOfBuffer = prev;

            assert this.buffer != null && next == this.buffer.capacity() + 1;
         } catch (Throwable var14) {
            var2 = var14;
            throw var14;
         } finally {
            if(group != null) {
               if(var2 != null) {
                  try {
                     group.close();
                  } catch (Throwable var13) {
                     var2.addSuppressed(var13);
                  }
               } else {
                  group.close();
               }
            }

         }

         return;
      }
   }

   void waitForModifications() {
      this.appendOrder.awaitNewBarrier();
   }

   synchronized void sync() {
      if(!this.headerWritten) {
         throw new IllegalStateException("commit log header has not been written");
      } else {
         boolean close = false;
         if(this.allocatePosition.get() > this.lastSyncedOffset + 8) {
            assert this.buffer != null;

            int startMarker = this.lastSyncedOffset;
            int nextMarker = this.allocate(8);
            if(nextMarker < 0) {
               this.discardUnusedTail();
               close = true;
               nextMarker = this.buffer.capacity();
            }

            this.waitForModifications();
            int sectionEnd = close?this.endOfBuffer:nextMarker;
            this.write(startMarker, sectionEnd);
            this.lastSyncedOffset = nextMarker;
            if(close) {
               this.internalClose();
            }

            this.syncComplete.signalAll();
         }
      }
   }

   protected static void writeSyncMarker(long id, ByteBuffer buffer, int offset, int filePos, int nextMarker) {
      if(filePos > nextMarker) {
         throw new IllegalArgumentException(String.format("commit log sync marker's current file position %d is greater than next file position %d", new Object[]{Integer.valueOf(filePos), Integer.valueOf(nextMarker)}));
      } else {
         CRC32 crc = new CRC32();
         FBUtilities.updateChecksumInt(crc, (int)(id & 4294967295L));
         FBUtilities.updateChecksumInt(crc, (int)(id >>> 32));
         FBUtilities.updateChecksumInt(crc, filePos);
         buffer.putInt(offset, nextMarker);
         buffer.putInt(offset + 4, (int)crc.getValue());
      }
   }

   abstract void write(int var1, int var2);

   public boolean isStillAllocating() {
      return this.allocatePosition.get() < this.endOfBuffer;
   }

   void discard(boolean deleteFile) {
      this.close();
      if(deleteFile) {
         FileUtils.deleteWithConfirm(this.logFile);
      }

      this.manager.addSize(-this.onDiskSize());
   }

   public CommitLogPosition getCurrentCommitLogPosition() {
      return new CommitLogPosition(this.id, this.allocatePosition.get());
   }

   public String getPath() {
      return this.logFile.getPath();
   }

   public String getName() {
      return this.logFile.getName();
   }

   void waitForFinalSync() {
      while(true) {
         WaitQueue.Signal signal = this.syncComplete.register(Thread.currentThread());
         if(this.lastSyncedOffset >= this.endOfBuffer) {
            signal.cancel();
            return;
         }

         signal.awaitUninterruptibly();
      }
   }

   void waitForSync(int position, Timer waitingOnCommit) {
      while(this.lastSyncedOffset < position) {
         WaitQueue.Signal signal = waitingOnCommit != null?this.syncComplete.register(Thread.currentThread(), waitingOnCommit.timer()):this.syncComplete.register(Thread.currentThread());
         if(this.lastSyncedOffset < position) {
            signal.awaitUninterruptibly();
         } else {
            signal.cancel();
         }
      }

   }

   synchronized void close() {
      this.discardUnusedTail();
      this.sync();

      assert this.buffer == null;

   }

   protected void internalClose() {
      try {
         this.channel.close();
         this.buffer = null;
      } catch (IOException var2) {
         throw new FSWriteError(var2, this.getPath());
      }
   }

   public static <K> void coverInMap(ConcurrentMap<K, IntegerInterval> map, K key, int value) {
      IntegerInterval i = (IntegerInterval)map.get(key);
      if(i == null) {
         i = (IntegerInterval)map.putIfAbsent(key, new IntegerInterval(value, value));
         if(i == null) {
            return;
         }
      }

      i.expandToCover(value);
   }

   void markDirty(Mutation mutation, int allocatedPosition) {
      Iterator var3 = mutation.getPartitionUpdates().iterator();

      while(var3.hasNext()) {
         PartitionUpdate update = (PartitionUpdate)var3.next();
         coverInMap(this.tableDirty, update.metadata().id, allocatedPosition);
      }

   }

   public synchronized void markClean(TableId tableId, CommitLogPosition startPosition, CommitLogPosition endPosition) {
      if(startPosition.segmentId <= this.id && endPosition.segmentId >= this.id) {
         if(this.tableDirty.containsKey(tableId)) {
            int start = startPosition.segmentId == this.id?startPosition.position:0;
            int end = endPosition.segmentId == this.id?endPosition.position:2147483647;
            ((IntegerInterval.Set)this.tableClean.computeIfAbsent(tableId, (k) -> {
               return new IntegerInterval.Set();
            })).add(start, end);
            this.removeCleanFromDirty();
         }
      }
   }

   private void removeCleanFromDirty() {
      if(!this.isStillAllocating()) {
         Iterator iter = this.tableClean.entrySet().iterator();

         while(iter.hasNext()) {
            Entry<TableId, IntegerInterval.Set> clean = (Entry)iter.next();
            TableId tableId = (TableId)clean.getKey();
            IntegerInterval.Set cleanSet = (IntegerInterval.Set)clean.getValue();
            IntegerInterval dirtyInterval = (IntegerInterval)this.tableDirty.get(tableId);
            if(dirtyInterval != null && cleanSet.covers(dirtyInterval)) {
               this.tableDirty.remove(tableId);
               iter.remove();
            }
         }

      }
   }

   public synchronized Collection<TableId> getDirtyTableIds() {
      if (this.tableClean.isEmpty() || this.tableDirty.isEmpty()) {
         return this.tableDirty.keySet();
      }
      ArrayList<TableId> r = new ArrayList<TableId>(this.tableDirty.size());
      for (Map.Entry dirty : this.tableDirty.entrySet()) {
         TableId tableId = (TableId)dirty.getKey();
         IntegerInterval dirtyInterval = (IntegerInterval)dirty.getValue();
         IntegerInterval.Set cleanSet = this.tableClean.get(tableId);
         if (cleanSet != null && cleanSet.covers(dirtyInterval)) continue;
         r.add((TableId)dirty.getKey());
      }
      return r;
   }


   public synchronized boolean isUnused() {
      if(this.isStillAllocating()) {
         return false;
      } else {
         this.removeCleanFromDirty();
         return this.tableDirty.isEmpty();
      }
   }

   public boolean contains(CommitLogPosition context) {
      return context.segmentId == this.id;
   }

   public String dirtyString() {
      StringBuilder sb = new StringBuilder();
      Iterator var2 = this.getDirtyTableIds().iterator();

      while(var2.hasNext()) {
         TableId tableId = (TableId)var2.next();
         TableMetadata m = Schema.instance.getTableMetadata(tableId);
         sb.append(m == null?"<deleted>":m.name).append(" (").append(tableId).append(", dirty: ").append(this.tableDirty.get(tableId)).append(", clean: ").append(this.tableClean.get(tableId)).append("), ");
      }

      return sb.toString();
   }

   public abstract long onDiskSize();

   public long contentSize() {
      return (long)this.lastSyncedOffset;
   }

   public long availableSize() {
      return (long)(this.endOfBuffer - this.allocatePosition.get());
   }

   public String toString() {
      return "CommitLogSegment(" + this.getPath() + ')';
   }

   public CommitLogSegment.CDCState getCDCState() {
      return this.cdcState;
   }

   public void setCDCState(CommitLogSegment.CDCState newState) {
      if(newState != this.cdcState) {
         Object var2 = this.cdcStateLock;
         synchronized(this.cdcStateLock) {
            if(this.cdcState == CommitLogSegment.CDCState.CONTAINS && newState != CommitLogSegment.CDCState.CONTAINS) {
               throw new IllegalArgumentException("Cannot transition from CONTAINS to any other state.");
            } else if(this.cdcState == CommitLogSegment.CDCState.FORBIDDEN && newState != CommitLogSegment.CDCState.PERMITTED) {
               throw new IllegalArgumentException("Only transition from FORBIDDEN to PERMITTED is allowed.");
            } else {
               this.cdcState = newState;
            }
         }
      }
   }

   static {
      long maxId = -9223372036854775808L;
      File[] var2 = (new File(DatabaseDescriptor.getCommitLogLocation())).listFiles();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File file = var2[var4];
         if(CommitLogDescriptor.isValid(file.getName())) {
            maxId = Math.max(CommitLogDescriptor.fromFileName(file.getName()).id, maxId);
         }
      }

      replayLimitId = idBase = Math.max(System.currentTimeMillis(), maxId + 1L);
   }

   protected static class Allocation {
      private final CommitLogSegment segment;
      private final OpOrder.Group appendOp;
      private final int position;
      private final ByteBuffer buffer;

      Allocation(CommitLogSegment segment, OpOrder.Group appendOp, int position, ByteBuffer buffer) {
         this.segment = segment;
         this.appendOp = appendOp;
         this.position = position;
         this.buffer = buffer;
      }

      CommitLogSegment getSegment() {
         return this.segment;
      }

      ByteBuffer getBuffer() {
         return this.buffer;
      }

      void markWritten() {
         this.appendOp.close();
      }

      void awaitDiskSync(Timer waitingOnCommit) {
         this.segment.waitForSync(this.position, waitingOnCommit);
      }

      public String toString() {
         return String.format("Segment id %d, position %d, limit: %d", new Object[]{Long.valueOf(this.segment.id), Integer.valueOf(this.buffer.position()), Integer.valueOf(this.buffer.limit())});
      }

      public CommitLogPosition getCommitLogPosition() {
         return new CommitLogPosition(this.segment.id, this.buffer.limit());
      }
   }

   public static class CommitLogSegmentFileComparator implements Comparator<File> {
      public CommitLogSegmentFileComparator() {
      }

      public int compare(File f, File f2) {
         CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(f.getName());
         CommitLogDescriptor desc2 = CommitLogDescriptor.fromFileName(f2.getName());
         return Long.compare(desc.id, desc2.id);
      }
   }

   public static enum CDCState {
      PERMITTED,
      FORBIDDEN,
      CONTAINS;

      private CDCState() {
      }
   }
}
