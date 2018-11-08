package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;

public class RangeAwareSSTableWriter implements SSTableMultiWriter {
   private final List<PartitionPosition> boundaries;
   private final List<Directories.DataDirectory> directories;
   private final int sstableLevel;
   private final long estimatedKeys;
   private final long repairedAt;
   private final UUID pendingRepair;
   private final SSTableFormat.Type format;
   private final SerializationHeader header;
   private final LifecycleTransaction txn;
   private int currentIndex = -1;
   public final ColumnFamilyStore cfs;
   private final List<SSTableMultiWriter> finishedWriters = new ArrayList();
   private final List<SSTableReader> finishedReaders = new ArrayList();
   private SSTableMultiWriter currentWriter = null;

   public RangeAwareSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, UUID pendingRepair, SSTableFormat.Type format, int sstableLevel, long totalSize, LifecycleTransaction txn, SerializationHeader header) throws IOException {
      DiskBoundaries db = cfs.getDiskBoundaries();
      this.directories = db.directories;
      this.sstableLevel = sstableLevel;
      this.cfs = cfs;
      this.estimatedKeys = estimatedKeys / (long)this.directories.size();
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.format = format;
      this.txn = txn;
      this.header = header;
      this.boundaries = db.positions;
      if(this.boundaries == null) {
         Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
         if(localDir == null) {
            throw new IOException(String.format("Insufficient disk space to store %s", new Object[]{FBUtilities.prettyPrintMemory(totalSize)}));
         }

         Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(localDir), format);
         this.currentWriter = cfs.createSSTableMultiWriter(desc, estimatedKeys, repairedAt, pendingRepair, sstableLevel, header, txn);
      }

   }

   private void maybeSwitchWriter(DecoratedKey key) {
      if(this.boundaries != null) {
         boolean switched;
         for(switched = false; this.currentIndex < 0 || key.compareTo((PartitionPosition)this.boundaries.get(this.currentIndex)) > 0; ++this.currentIndex) {
            switched = true;
         }

         if(switched) {
            if(this.currentWriter != null) {
               this.finishedWriters.add(this.currentWriter);
            }

            Descriptor desc = this.cfs.newSSTableDescriptor(this.cfs.getDirectories().getLocationForDisk((Directories.DataDirectory)this.directories.get(this.currentIndex)), this.format);
            this.currentWriter = this.cfs.createSSTableMultiWriter(desc, this.estimatedKeys, this.repairedAt, this.pendingRepair, this.sstableLevel, this.header, this.txn);
         }

      }
   }

   public boolean append(UnfilteredRowIterator partition) {
      this.maybeSwitchWriter(partition.partitionKey());
      return this.currentWriter.append(partition);
   }

   public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult) {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;
      Iterator var6 = this.finishedWriters.iterator();

      while(var6.hasNext()) {
         SSTableMultiWriter writer = (SSTableMultiWriter)var6.next();
         if(writer.getFilePointer() > 0L) {
            this.finishedReaders.addAll(writer.finish(repairedAt, maxDataAge, openResult));
         } else {
            SSTableMultiWriter.abortOrDie(writer);
         }
      }

      return this.finishedReaders;
   }

   public Collection<SSTableReader> finish(boolean openResult) {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;
      Iterator var2 = this.finishedWriters.iterator();

      while(var2.hasNext()) {
         SSTableMultiWriter writer = (SSTableMultiWriter)var2.next();
         if(writer.getFilePointer() > 0L) {
            this.finishedReaders.addAll(writer.finish(openResult));
         } else {
            SSTableMultiWriter.abortOrDie(writer);
         }
      }

      return this.finishedReaders;
   }

   public Collection<SSTableReader> finished() {
      return this.finishedReaders;
   }

   public SSTableMultiWriter setOpenResult(boolean openResult) {
      this.finishedWriters.forEach((w) -> {
         w.setOpenResult(openResult);
      });
      this.currentWriter.setOpenResult(openResult);
      return this;
   }

   public String getFilename() {
      return String.join("/", new CharSequence[]{this.cfs.keyspace.getName(), this.cfs.getTableName()});
   }

   public long getFilePointer() {
      return this.currentWriter.getFilePointer();
   }

   public TableId getTableId() {
      return this.currentWriter.getTableId();
   }

   public Throwable commit(Throwable accumulate) {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;

      SSTableMultiWriter writer;
      for(Iterator var2 = this.finishedWriters.iterator(); var2.hasNext(); accumulate = writer.commit(accumulate)) {
         writer = (SSTableMultiWriter)var2.next();
      }

      return accumulate;
   }

   public Throwable abort(Throwable accumulate) {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;

      SSTableMultiWriter finishedWriter;
      for(Iterator var2 = this.finishedWriters.iterator(); var2.hasNext(); accumulate = finishedWriter.abort(accumulate)) {
         finishedWriter = (SSTableMultiWriter)var2.next();
      }

      return accumulate;
   }

   public void prepareToCommit() {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;
      this.finishedWriters.forEach(Transactional::prepareToCommit);
   }

   public void close() {
      if(this.currentWriter != null) {
         this.finishedWriters.add(this.currentWriter);
      }

      this.currentWriter = null;
      this.finishedWriters.forEach(Transactional::close);
   }
}
