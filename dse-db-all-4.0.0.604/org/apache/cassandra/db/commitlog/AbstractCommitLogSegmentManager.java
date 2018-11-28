package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Single;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCommitLogSegmentManager {
   static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogSegmentManager.class);
   private volatile CommitLogSegment allocatingFrom = null;
   private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue();
   private final AtomicReference<CommitLogSegment> availableSegment = new AtomicReference((Object)null);
   private final AtomicReference<Runnable> segmentPreparedCallback = new AtomicReference();
   private final AtomicReference<AbstractCommitLogSegmentManager.SegmentAdvancer> activeAdvanceRequest = new AtomicReference();
   final String storageDirectory;
   private final AtomicLong size = new AtomicLong();
   private Thread managerThread;
   protected final CommitLog commitLog;
   private volatile boolean shutdown;
   private final BooleanSupplier managerThreadWaitCondition = () -> {
      return this.availableSegment.get() == null && !this.atSegmentBufferLimit() || this.shutdown;
   };
   private final WaitQueue managerThreadWaitQueue = new WaitQueue();
   private static final SimpleCachedBufferPool bufferPool = new SimpleCachedBufferPool(DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool(), DatabaseDescriptor.getCommitLogSegmentSize());

   AbstractCommitLogSegmentManager(CommitLog commitLog, String storageDirectory) {
      this.commitLog = commitLog;
      this.storageDirectory = storageDirectory;
   }

   void start() {
      Runnable runnable = new WrappedRunnable() {
         public void runMayThrow() throws Exception {
            while(!AbstractCommitLogSegmentManager.this.shutdown) {
               try {
                  AbstractCommitLogSegmentManager.logger.trace("No segments in reserve; creating a fresh one");
                  CommitLogSegment prev = (CommitLogSegment)AbstractCommitLogSegmentManager.this.availableSegment.getAndSet(AbstractCommitLogSegmentManager.this.createSegment());

                  assert prev == null : "Only management thread can construct segments.";

                  if(AbstractCommitLogSegmentManager.this.shutdown) {
                     AbstractCommitLogSegmentManager.this.discardAvailableSegment();
                     return;
                  }

                  Runnable callback = (Runnable)AbstractCommitLogSegmentManager.this.segmentPreparedCallback.getAndSet(null);
                  if(callback != null) {
                     callback.run();
                  }

                  if(AbstractCommitLogSegmentManager.this.availableSegment.get() == null && !AbstractCommitLogSegmentManager.this.atSegmentBufferLimit()) {
                     continue;
                  }

                  AbstractCommitLogSegmentManager.this.maybeFlushToReclaim();
               } catch (Throwable var3) {
                  AbstractCommitLogSegmentManager.SegmentAdvancer advancer = (AbstractCommitLogSegmentManager.SegmentAdvancer)AbstractCommitLogSegmentManager.this.activeAdvanceRequest.get();

                  assert advancer != null : "The main thread should have requested the first segment before starting this thread";

                  if(advancer.oldSegment == null) {
                     advancer.completeExceptionally(var3);
                  }

                  if(!CommitLog.handleCommitError("Failed managing commit log segments", var3)) {
                     return;
                  }

                  Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
               }

               WaitQueue.waitOnCondition(AbstractCommitLogSegmentManager.this.managerThreadWaitCondition, AbstractCommitLogSegmentManager.this.managerThreadWaitQueue);
            }

         }
      };
      this.shutdown = false;
      this.managerThread = NamedThreadFactory.createThread(runnable, "COMMIT-LOG-ALLOCATOR");
      this.managerThread.setDaemon(true);
      CompletableFuture<Void> fut = this.advanceAllocatingFrom((CommitLogSegment)null);
      this.managerThread.start();
      fut.join();
   }

   private boolean atSegmentBufferLimit() {
      return CommitLogSegment.usesBufferPool(this.commitLog) && bufferPool.atLimit();
   }

   private void maybeFlushToReclaim() {
      long unused = this.unusedCapacity();
      if(unused < 0L) {
         long flushingSize = 0L;
         List<CommitLogSegment> segmentsToRecycle = new ArrayList();
         Iterator var6 = this.activeSegments.iterator();

         while(var6.hasNext()) {
            CommitLogSegment segment = (CommitLogSegment)var6.next();
            if(segment == this.allocatingFrom) {
               break;
            }

            flushingSize += segment.onDiskSize();
            segmentsToRecycle.add(segment);
            if(flushingSize + unused >= 0L) {
               break;
            }
         }

         this.flushDataFrom(segmentsToRecycle, false);
      }

   }

   public abstract Single<CommitLogSegment.Allocation> allocate(Mutation var1, int var2);

   abstract void handleReplayedSegment(File var1);

   abstract CommitLogSegment createSegment();

   abstract void discard(CommitLogSegment var1, boolean var2);

   CompletableFuture<Void> advanceAllocatingFrom(CommitLogSegment old) {
      AbstractCommitLogSegmentManager.SegmentAdvancer activeAdvancer;
      AbstractCommitLogSegmentManager.SegmentAdvancer ourAdvancer;
      do {
         activeAdvancer = (AbstractCommitLogSegmentManager.SegmentAdvancer)this.activeAdvanceRequest.get();
         if(activeAdvancer != null && activeAdvancer.oldSegment == old) {
            return activeAdvancer;
         }

         if(this.allocatingFrom != old) {
            return CompletableFuture.completedFuture(null);
         }

         ourAdvancer = new AbstractCommitLogSegmentManager.SegmentAdvancer(old);
      } while(!this.activeAdvanceRequest.compareAndSet(activeAdvancer, ourAdvancer));

      if(this.availableSegment.get() != null) {
         ourAdvancer.run();
      } else {
         this.runWhenSegmentIsAvailable(ourAdvancer);
      }

      return ourAdvancer;
   }

   void runWhenSegmentIsAvailable(Runnable runnable) {
      Runnable prev;
      Runnable combined;
      Runnable runnable2;
      assert (runnable != null);
      do {
         Runnable other = prev = this.segmentPreparedCallback.get();
         if (prev != null) {
            runnable2 = () -> {
               other.run();
               runnable.run();
            };
            continue;
         }
         runnable2 = runnable;
      } while (!this.segmentPreparedCallback.compareAndSet(prev, combined = runnable2));

      if(this.availableSegment.get() != null && this.segmentPreparedCallback.compareAndSet(combined, null)) {
         combined.run();
      }

   }

   void forceRecycleAll(Iterable<TableId> droppedTables) {
      List<CommitLogSegment> segmentsToRecycle = new ArrayList(this.activeSegments);
      CommitLogSegment last = (CommitLogSegment)segmentsToRecycle.get(segmentsToRecycle.size() - 1);
      this.advanceAllocatingFrom(last).join();
      last.waitForModifications();
      Keyspace.writeOrder.awaitNewBarrier();
      Iterable flushes = this.flushDataFrom(segmentsToRecycle, true);

      try {
         FBUtilities.waitOnFutures(flushes);
         Iterator var5 = this.activeSegments.iterator();

         CommitLogSegment segment;
         while(var5.hasNext()) {
            segment = (CommitLogSegment)var5.next();
            Iterator var7 = droppedTables.iterator();

            while(var7.hasNext()) {
               TableId tableId = (TableId)var7.next();
               segment.markClean(tableId, CommitLogPosition.NONE, segment.getCurrentCommitLogPosition());
            }
         }

         var5 = this.activeSegments.iterator();

         while(var5.hasNext()) {
            segment = (CommitLogSegment)var5.next();
            if(segment.isUnused()) {
               this.archiveAndDiscard(segment);
            }
         }

         CommitLogSegment first;
         if((first = (CommitLogSegment)this.activeSegments.peek()) != null && first.id <= last.id) {
            logger.error("Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
         }
      } catch (Throwable var9) {
         logger.error("Failed waiting for a forced recycle of in-use commit log segments", var9);
      }

   }

   void archiveAndDiscard(CommitLogSegment segment) {
      boolean archiveSuccess = this.commitLog.archiver.maybeWaitForArchiving(segment.getName());
      if(this.activeSegments.remove(segment)) {
         logger.debug("Segment {} is no longer active and will be deleted {}", segment, archiveSuccess?"now":"by the archive script");
         this.discard(segment, archiveSuccess);
      }
   }

   void addSize(long addedSize) {
      this.size.addAndGet(addedSize);
   }

   public long onDiskSize() {
      return this.size.get();
   }

   private long unusedCapacity() {
      long total = DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024L * 1024L;
      long currentSize = this.size.get();
      logger.trace("Total active commitlog segment space used is {} out of {}", Long.valueOf(currentSize), Long.valueOf(total));
      return total - currentSize;
   }

   private Iterable<CompletableFuture<CommitLogPosition>> flushDataFrom(List<CommitLogSegment> segments, boolean force) {
      if(segments.isEmpty()) {
         return Collections.emptyList();
      } else {
         CommitLogPosition maxCommitLogPosition = ((CommitLogSegment)segments.get(segments.size() - 1)).getCurrentCommitLogPosition();
         Map<TableId, CompletableFuture<CommitLogPosition>> flushes = new LinkedHashMap();
         Iterator var5 = segments.iterator();

         while(var5.hasNext()) {
            CommitLogSegment segment = (CommitLogSegment)var5.next();
            Iterator var7 = segment.getDirtyTableIds().iterator();

            while(var7.hasNext()) {
               TableId dirtyTableId = (TableId)var7.next();
               TableMetadata metadata = Schema.instance.getTableMetadata(dirtyTableId);
               if(metadata == null) {
                  logger.trace("Marking clean CF {} that doesn't exist anymore", dirtyTableId);
                  segment.markClean(dirtyTableId, CommitLogPosition.NONE, segment.getCurrentCommitLogPosition());
               } else if(!flushes.containsKey(dirtyTableId)) {
                  ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(dirtyTableId);
                  flushes.put(dirtyTableId, force?cfs.forceFlush():cfs.forceFlush(maxCommitLogPosition));
               }
            }
         }

         return flushes.values();
      }
   }

   public void stopUnsafe(boolean deleteSegments) {
      logger.debug("CLSM closing and clearing existing commit log segments...");
      this.shutdown();

      try {
         this.awaitTermination();
      } catch (InterruptedException var4) {
         throw new RuntimeException(var4);
      }

      Iterator var2 = this.activeSegments.iterator();

      while(var2.hasNext()) {
         CommitLogSegment segment = (CommitLogSegment)var2.next();
         this.closeAndDeleteSegmentUnsafe(segment, deleteSegments);
      }

      this.activeSegments.clear();
      this.size.set(0L);
      logger.trace("CLSM done with closing and clearing existing commit log segments.");
   }

   void awaitManagementTasksCompletion() {
      if(this.availableSegment.get() == null && !this.atSegmentBufferLimit()) {
         CountDownLatch latch = new CountDownLatch(1);
         this.runWhenSegmentIsAvailable(latch::countDown);
         Uninterruptibles.awaitUninterruptibly(latch);
      }

   }

   private void closeAndDeleteSegmentUnsafe(CommitLogSegment segment, boolean delete) {
      try {
         this.discard(segment, delete);
      } catch (AssertionError var4) {
         ;
      }

   }

   public void shutdown() {
      assert !this.shutdown;

      this.shutdown = true;
      this.discardAvailableSegment();
      this.wakeManager();
   }

   private void discardAvailableSegment() {
      CommitLogSegment next = (CommitLogSegment)this.availableSegment.getAndSet(null);
      if(next != null) {
         next.discard(true);
      }

   }

   public void awaitTermination() throws InterruptedException {
      this.managerThread.join();
      this.managerThread = null;
      Iterator var1 = this.activeSegments.iterator();

      while(var1.hasNext()) {
         CommitLogSegment segment = (CommitLogSegment)var1.next();
         segment.close();
      }

      bufferPool.shutdown();
   }

   @VisibleForTesting
   public Collection<CommitLogSegment> getActiveSegments() {
      return Collections.unmodifiableCollection(this.activeSegments);
   }

   CommitLogPosition getCurrentPosition() {
      return this.allocatingFrom.getCurrentCommitLogPosition();
   }

   public void sync() throws IOException {
      CommitLogSegment current = this.allocatingFrom;
      Iterator var2 = this.getActiveSegments().iterator();

      while(var2.hasNext()) {
         CommitLogSegment segment = (CommitLogSegment)var2.next();
         if(segment.id > current.id) {
            return;
         }

         segment.sync();
      }

   }

   SimpleCachedBufferPool getBufferPool() {
      return bufferPool;
   }

   void wakeManager() {
      this.managerThreadWaitQueue.signalAll();
   }

   void notifyBufferFreed() {
      this.wakeManager();
   }

   CommitLogSegment allocatingFrom() {
      return this.allocatingFrom;
   }

   class SegmentAdvancer extends CompletableFuture<Void> implements Runnable {
      final CommitLogSegment oldSegment;

      SegmentAdvancer(CommitLogSegment oldSegment) {
         this.oldSegment = oldSegment;
      }

      public void run() {
         assert AbstractCommitLogSegmentManager.this.allocatingFrom == this.oldSegment;

         CommitLogSegment next = (CommitLogSegment)AbstractCommitLogSegmentManager.this.availableSegment.getAndSet(null);
         if(next == null) {
            AbstractCommitLogSegmentManager.logger.warn("Available segment callback without available segment. This is only expected to happen while running commit log tests.");
            AbstractCommitLogSegmentManager.this.runWhenSegmentIsAvailable(this);
         } else {
            AbstractCommitLogSegmentManager.this.activeSegments.add(next);
            AbstractCommitLogSegmentManager.this.allocatingFrom = next;
            AbstractCommitLogSegmentManager.this.wakeManager();
            this.complete(null);
            if(this.oldSegment != null) {
               AbstractCommitLogSegmentManager.this.commitLog.archiver.maybeArchive(this.oldSegment);
               this.oldSegment.discardUnusedTail();
            }

            AbstractCommitLogSegmentManager.this.commitLog.requestExtraSync();
         }
      }
   }
}
