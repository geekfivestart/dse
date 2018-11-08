package com.datastax.bdp.cassandra.db.tiered;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;

public class TieredSSTableMultiWriter implements SSTableMultiWriter {
   private final TieredStorageStrategy strategy;
   private final TieredRowWriter rowWriter;
   private final ColumnFamilyStore cfs;
   private final long keyCount;
   private final long repairedAt;
   private final UUID pendingRepair;
   private final MetadataCollector meta;
   private final SerializationHeader header;
   private final LifecycleTransaction txn;
   private final Collection<Index> indexes;

   public TieredSSTableMultiWriter(ColumnFamilyStore cfs, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn, TieredStorageStrategy strategy) {
      this.cfs = cfs;
      this.header = header;
      this.keyCount = keyCount;
      this.meta = meta;
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.strategy = strategy;
      this.txn = txn;
      this.indexes = indexes;
      this.rowWriter = new TieredSSTableMultiWriter.RowWriter(strategy);
   }

   public boolean append(UnfilteredRowIterator row) {
      return this.rowWriter.append(row);
   }

   private Iterable<TieredRangeAwareSSTableWriter> getRangeWriters() {
      return () -> {
         return new Iterator<TieredRangeAwareSSTableWriter>() {
            Iterator iter;

            {
               this.iter = TieredSSTableMultiWriter.this.rowWriter.writersForTxn().iterator();
            }

            public boolean hasNext() {
               return this.iter.hasNext();
            }

            public TieredRangeAwareSSTableWriter next() {
               RangeAwareWriter n = (RangeAwareWriter)this.iter.next();

               assert n instanceof TieredRangeAwareSSTableWriter;

               return (TieredRangeAwareSSTableWriter)n;
            }
         };
      };
   }

   public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult) {
      List<SSTableReader> sstables = new ArrayList(this.rowWriter.numTiers);
      Iterator var7 = this.getRangeWriters().iterator();

      while(var7.hasNext()) {
         TieredRangeAwareSSTableWriter writer = (TieredRangeAwareSSTableWriter)var7.next();
         sstables.addAll(writer.finish(repairedAt, maxDataAge, openResult));
      }

      return sstables;
   }

   public Collection<SSTableReader> finish(boolean openResult) {
      List<SSTableReader> sstables = new ArrayList(this.rowWriter.numTiers);
      Iterator var3 = this.getRangeWriters().iterator();

      while(var3.hasNext()) {
         TieredRangeAwareSSTableWriter writer = (TieredRangeAwareSSTableWriter)var3.next();
         sstables.addAll(writer.finish(openResult));
      }

      return sstables;
   }

   public Collection<SSTableReader> finished() {
      List<SSTableReader> sstables = new ArrayList(this.rowWriter.numTiers);
      Iterator var2 = this.getRangeWriters().iterator();

      while(var2.hasNext()) {
         TieredRangeAwareSSTableWriter writer = (TieredRangeAwareSSTableWriter)var2.next();
         sstables.addAll(writer.finished());
      }

      return sstables;
   }

   public SSTableMultiWriter setOpenResult(boolean openResult) {
      this.rowWriter.writersForTxn().forEach((writer) -> {
         writer.setOpenResult(openResult);
      });
      return this;
   }

   public String getFilename() {
      ArrayList<String> filenames = new ArrayList();
      Iterator var2 = this.getRangeWriters().iterator();

      while(var2.hasNext()) {
         TieredRangeAwareSSTableWriter writer = (TieredRangeAwareSSTableWriter)var2.next();
         filenames.addAll(writer.getFilenames());
      }

      return Joiner.on(", ").join(filenames);
   }

   public long getFilePointer() {
      long ptr = 0L;

      TieredRangeAwareSSTableWriter writer;
      for(Iterator var3 = this.getRangeWriters().iterator(); var3.hasNext(); ptr += writer.getFilePointer()) {
         writer = (TieredRangeAwareSSTableWriter)var3.next();
      }

      return ptr;
   }

   public TableId getTableId() {
      return this.strategy.getCfs().metadata.id;
   }

   public Throwable commit(Throwable throwable) {
      return this.rowWriter.commit(throwable);
   }

   public Throwable abort(Throwable throwable) {
      return this.rowWriter.abort(throwable);
   }

   public void prepareToCommit() {
      this.rowWriter.prepareToCommit();
   }

   public void close() {
      Iterator var1 = this.rowWriter.writersForTxn().iterator();

      while(var1.hasNext()) {
         RangeAwareWriter writer = (RangeAwareWriter)var1.next();
         writer.close();
      }

   }

   private class RowWriter extends TieredRowWriter {
      RowWriter(TieredStorageStrategy strategy) {
         super(strategy);
      }

      protected RangeAwareWriter createRangeAwareWriterForTier(TieredStorageStrategy.Tier tier) {
         return new TieredRangeAwareSSTableWriter(TieredSSTableMultiWriter.this.cfs, tier.getDirectories(), TieredSSTableMultiWriter.this.keyCount, TieredSSTableMultiWriter.this.repairedAt, TieredSSTableMultiWriter.this.pendingRepair, TieredSSTableMultiWriter.this.meta, TieredSSTableMultiWriter.this.header, TieredSSTableMultiWriter.this.txn, TieredSSTableMultiWriter.this.indexes);
      }
   }
}
