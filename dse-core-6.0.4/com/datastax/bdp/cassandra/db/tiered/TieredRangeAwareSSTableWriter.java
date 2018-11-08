package com.datastax.bdp.cassandra.db.tiered;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataCopier;

public class TieredRangeAwareSSTableWriter extends RangeAwareWriter {
   private final ColumnFamilyStore cfs;
   private final Directories directories;
   private final long keyCount;
   private final long repairedAt;
   private final UUID pendingRepair;
   private final MetadataCollector meta;
   private final SerializationHeader header;
   private final LifecycleTransaction txn;
   private final Collection<Index> indexes;
   private SSTableWriter currentWriter = null;
   private Set<SSTableWriter> finished = new HashSet();

   public TieredRangeAwareSSTableWriter(ColumnFamilyStore cfs, Directories directories, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, LifecycleTransaction txn, Collection<Index> indexes) {
      super(cfs, directories);
      this.cfs = cfs;
      this.directories = directories;
      this.keyCount = keyCount;
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.meta = meta;
      this.header = header;
      this.txn = txn;
      this.indexes = indexes;
   }

   public String toString() {
      return "TieredRangeAwareSSTableWriter{" + this.cfs.metadata.keyspace + "." + this.cfs.metadata.name + "currentWriter=" + this.currentWriter + ", finished=" + this.finished + '}';
   }

   protected boolean realAppend(UnfilteredRowIterator partition) {
      return this.currentWriter.append(partition) != null;
   }

   private void finishCurrentWriter() {
      if(this.currentWriter != null) {
         this.finished.add(this.currentWriter);
      }

      this.currentWriter = null;
   }

   protected void switchWriteLocation(DataDirectory directory) {
      this.finishCurrentWriter();
      Descriptor descriptor = this.cfs.newSSTableDescriptor(this.directories.getLocationForDisk(directory));
      MetadataCollector newCollector = MetadataCopier.copy(this.cfs.metadata(), this.meta);
      LifecycleTransaction var4 = this.txn;
      synchronized(this.txn) {
         this.currentWriter = SSTableWriter.create(descriptor, Long.valueOf(this.keyCount), Long.valueOf(this.repairedAt), this.pendingRepair, this.cfs.metadata, newCollector, this.header, this.indexes, this.txn);
      }
   }

   public Throwable abort(Throwable accumulate) {
      this.finishCurrentWriter();

      SSTableWriter writer;
      for(Iterator var2 = this.finished.iterator(); var2.hasNext(); accumulate = writer.abort(accumulate)) {
         writer = (SSTableWriter)var2.next();
      }

      return accumulate;
   }

   public Throwable commit(Throwable accumulate) {
      this.finishCurrentWriter();

      SSTableWriter writer;
      for(Iterator var2 = this.finished.iterator(); var2.hasNext(); accumulate = writer.commit(accumulate)) {
         writer = (SSTableWriter)var2.next();
      }

      return accumulate;
   }

   public void prepareToCommit() {
      this.finishCurrentWriter();
      Iterator var1 = this.finished.iterator();

      while(var1.hasNext()) {
         SSTableWriter writer = (SSTableWriter)var1.next();
         writer.setOpenResult(this.openResult).prepareToCommit();
      }

   }

   public void close() {
      this.finishCurrentWriter();
      Iterator var1 = this.finished.iterator();

      while(var1.hasNext()) {
         SSTableWriter writer = (SSTableWriter)var1.next();
         writer.close();
      }

   }

   public Collection<SSTableReader> finish(boolean openResult) {
      this.finishCurrentWriter();
      return (Collection)this.finished.stream().map((s) -> {
         return s.finish(openResult);
      }).collect(Collectors.toSet());
   }

   public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult) {
      this.finishCurrentWriter();
      return (Collection)this.finished.stream().map((s) -> {
         return s.finish(repairedAt, maxDataAge, openResult);
      }).collect(Collectors.toSet());
   }

   public Collection<SSTableReader> finished() {
      return (Collection)this.finished.stream().map(SSTableWriter::finished).collect(Collectors.toSet());
   }

   public long getFilePointer() {
      long ptr = 0L;

      SSTableWriter writer;
      for(Iterator var3 = this.finished.iterator(); var3.hasNext(); ptr += writer.getFilePointer()) {
         writer = (SSTableWriter)var3.next();
      }

      if(this.currentWriter != null) {
         ptr += this.currentWriter.getFilePointer();
      }

      return ptr;
   }

   public Collection<String> getFilenames() {
      List<String> names = new ArrayList(this.finished.size() + 1);
      Iterator var2 = this.finished.iterator();

      while(var2.hasNext()) {
         SSTableWriter writer = (SSTableWriter)var2.next();
         names.add(writer.getFilename());
      }

      if(this.currentWriter != null) {
         names.add(this.currentWriter.getFilename());
      }

      return names;
   }
}
