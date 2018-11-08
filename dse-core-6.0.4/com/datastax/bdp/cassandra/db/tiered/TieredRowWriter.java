package com.datastax.bdp.cassandra.db.tiered;

import com.google.common.hash.Hasher;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional;
import org.apache.commons.lang3.concurrent.BasicThreadFactory.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TieredRowWriter extends AbstractTransactional implements Transactional {
   private static final Logger logger = LoggerFactory.getLogger(TieredRowWriter.class);
   private static final int TIER_ROW_QUEUE_SIZE = 10;
   private static final Unfiltered CLOSE_SENTINEL = new Unfiltered() {
      public Kind kind() {
         throw new UnsupportedOperationException();
      }

      public void digest(Hasher hasher) {
         throw new UnsupportedOperationException();
      }

      public void validateData(TableMetadata cfMetaData) {
         throw new UnsupportedOperationException();
      }

      public String toString(TableMetadata cfMetaData) {
         return "Unfiltered[CLOSE_SENTINEL]";
      }

      public String toString(TableMetadata cfMetaData, boolean b) {
         return "Unfiltered[CLOSE_SENTINEL]";
      }

      public String toString(TableMetadata metadata, boolean includeClusterKeys, boolean fullDetails) {
         return "Unfiltered[CLOSE_SENTINEL]";
      }

      public Unfiltered purge(DeletionPurger purger, int nowInSec, boolean b) {
         throw new UnsupportedOperationException();
      }

      public boolean isEmpty() {
         return true;
      }

      public ClusteringPrefix clustering() {
         throw new UnsupportedOperationException();
      }
   };
   protected final TieredStorageStrategy strategy;
   protected final int numTiers;
   protected final ExecutorService executor;
   protected final RangeAwareWriter[] rangeWriters;
   protected Future[] futures;
   protected TieredRowWriter.RowIterator[] rowIterators;
   protected final TieredStorageStrategy.Context context;

   public TieredRowWriter(TieredStorageStrategy strategy) {
      this.strategy = strategy;
      this.numTiers = strategy.getTiers().size();
      ThreadFactory threadFactory = (new Builder()).namingPattern("TieredRowWriter-%d").build();
      this.executor = Executors.newFixedThreadPool(this.numTiers, threadFactory);
      this.rangeWriters = new RangeAwareWriter[this.numTiers];
      this.context = strategy.newContext();
   }

   protected abstract RangeAwareWriter createRangeAwareWriterForTier(TieredStorageStrategy.Tier var1);

   private RangeAwareWriter getOrCreateRangeWriter(int tier) {
      if(this.rangeWriters[tier] == null) {
         this.rangeWriters[tier] = this.createRangeAwareWriterForTier(this.strategy.getTier(tier));
      }

      return this.rangeWriters[tier];
   }

   protected TieredRowWriter.TierPartitionConsumer getRowConsumer(int tier) {
      return TieredRowWriter.TierPartitionConsumer.create(this.getOrCreateRangeWriter(tier));
   }

   protected boolean waitOnFutures(Future[] futures) {
      int finishedWriters = 0;
      boolean wasWritten = false;
      Future[] var4 = futures;
      int var5 = futures.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         Future future = var4[var6];
         if(future != null) {
            try {
               wasWritten |= ((Boolean)future.get()).booleanValue();
            } catch (InterruptedException var9) {
               logger.error("Error waiting on futures", var9);
               throw new AssertionError(var9);
            } catch (ExecutionException var10) {
               logger.error("Error waiting on futures", var10);
               throw new RuntimeException(var10);
            }

            ++finishedWriters;
         }
      }

      assert finishedWriters > 0;

      return wasWritten;
   }

   protected void appendToTier(Unfiltered row, int tier) {
      TieredRowWriter.RowIterator rowIterator = this.rowIterators[tier];
      if(this.futures[tier] == null) {
         TieredRowWriter.PartitionWriter partitionWriter = new TieredRowWriter.PartitionWriter(this.getRowConsumer(tier), rowIterator);
         this.futures[tier] = this.executor.submit(partitionWriter);
      }

      rowIterator.put(row);
      if(logger.isDebugEnabled()) {
         logger.debug("Writing {} to tier {}", row.toString(this.strategy.cfs.metadata(), true), Integer.valueOf(tier));
      }

   }

   public boolean append(UnfilteredRowIterator partition) {
      if(partition.isEmpty()) {
         return false;
      } else {
         this.futures = new Future[this.numTiers];
         this.rowIterators = new TieredRowWriter.RowIterator[this.numTiers];

         int tier;
         Row row;
         TieredRowWriter.RowIterator rowIterator;
         for(tier = 0; tier < this.numTiers; ++tier) {
            DeletionTime deletion = partition.partitionLevelDeletion();
            deletion = !deletion.isLive() && this.strategy.getTierForDeletion(deletion, this.context) == tier?deletion:DeletionTime.LIVE;
            row = partition.staticRow();
            row = row != Rows.EMPTY_STATIC_ROW && this.strategy.getTierForRow(row, this.context) == tier?row:Rows.EMPTY_STATIC_ROW;
            rowIterator = new TieredRowWriter.RowIterator(partition, deletion, row);
            this.rowIterators[tier] = rowIterator;
            if(!deletion.isLive() || row != Rows.EMPTY_STATIC_ROW) {
               TieredRowWriter.PartitionWriter partitionWriter = new TieredRowWriter.PartitionWriter(this.getRowConsumer(tier), rowIterator);
               this.futures[tier] = this.executor.submit(partitionWriter);
            }
         }

         while(partition.hasNext()) {
            Unfiltered unfiltered = (Unfiltered)partition.next();
            switch(null.$SwitchMap$org$apache$cassandra$db$rows$Unfiltered$Kind[unfiltered.kind().ordinal()]) {
            case 1:
               row = (Row)unfiltered;
               tier = this.strategy.getTierForRow(row, this.context);
               this.appendToTier(row, tier);
               break;
            case 2:
               if(unfiltered instanceof RangeTombstoneBoundaryMarker) {
                  RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)unfiltered;
                  RangeTombstoneBoundMarker closeBound = new RangeTombstoneBoundMarker(boundary.closeBound(false), boundary.endDeletionTime());
                  RangeTombstoneBoundMarker openBound = new RangeTombstoneBoundMarker(boundary.openBound(false), boundary.startDeletionTime());
                  int closeTier = this.strategy.getTierForDeletion(closeBound.deletionTime(), this.context);
                  int openTier = this.strategy.getTierForDeletion(openBound.deletionTime(), this.context);
                  if(closeTier == openTier) {
                     this.appendToTier(boundary, closeTier);
                  } else {
                     this.appendToTier(closeBound, closeTier);
                     this.appendToTier(openBound, openTier);
                  }
               } else {
                  if(!(unfiltered instanceof RangeTombstoneBoundMarker)) {
                     throw new AssertionError("Unhandled range tombstone marker type: " + unfiltered.getClass().getName());
                  }

                  RangeTombstoneBoundMarker bound = (RangeTombstoneBoundMarker)unfiltered;
                  tier = this.strategy.getTierForDeletion(bound.deletionTime(), this.context);
                  this.appendToTier(unfiltered, tier);
               }
               break;
            default:
               throw new AssertionError("Unhandled range tombstone marker type: " + unfiltered.getClass().getName());
            }
         }

         TieredRowWriter.RowIterator[] var10 = this.rowIterators;
         int var12 = var10.length;

         for(int var13 = 0; var13 < var12; ++var13) {
            rowIterator = var10[var13];
            if(rowIterator != null) {
               rowIterator.close();
            }
         }

         return this.waitOnFutures(this.futures);
      }
   }

   public Collection<RangeAwareWriter> writersForTxn() {
      Set<RangeAwareWriter> writerSet = new HashSet(this.numTiers);
      RangeAwareWriter[] var2 = this.rangeWriters;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         RangeAwareWriter writer = var2[var4];
         if(writer != null) {
            writerSet.add(writer);
         }
      }

      return writerSet;
   }

   private Throwable stopExecutor(Throwable accumulate) {
      try {
         this.executor.shutdown();
      } catch (Throwable var3) {
         accumulate = Throwables.merge(accumulate, var3);
      }

      return accumulate;
   }

   protected Throwable doAbort(Throwable accumulate) {
      accumulate = this.stopExecutor(accumulate);

      RangeAwareWriter writer;
      for(Iterator var2 = this.writersForTxn().iterator(); var2.hasNext(); accumulate = writer.abort(accumulate)) {
         writer = (RangeAwareWriter)var2.next();
         logger.debug("aborting {}", writer);
      }

      return accumulate;
   }

   protected void doPrepare() {
      Iterator var1 = this.writersForTxn().iterator();

      while(var1.hasNext()) {
         RangeAwareWriter writer = (RangeAwareWriter)var1.next();
         logger.debug("preparing to commit {}", writer);
         writer.setOpenResult(true).prepareToCommit();
      }

   }

   protected Throwable doCommit(Throwable accumulate) {
      accumulate = this.stopExecutor(accumulate);

      RangeAwareWriter writer;
      for(Iterator var2 = this.writersForTxn().iterator(); var2.hasNext(); accumulate = writer.commit(accumulate)) {
         writer = (RangeAwareWriter)var2.next();
         logger.debug("committing {}", writer);
      }

      return accumulate;
   }

   public Set<SSTableReader> finish() {
      super.finish();
      Set<SSTableReader> readers = new HashSet();
      Iterator var2 = this.writersForTxn().iterator();

      while(var2.hasNext()) {
         RangeAwareWriter writer = (RangeAwareWriter)var2.next();
         readers.addAll(writer.finish(true));
      }

      return readers;
   }

   class PartitionWriter implements Callable<Boolean> {
      private final TieredRowWriter.TierPartitionConsumer consumer;
      private final TieredRowWriter.RowIterator rowIterator;

      public PartitionWriter(TieredRowWriter.TierPartitionConsumer consumer, TieredRowWriter.RowIterator rowIterator) {
         this.consumer = consumer;
         this.rowIterator = rowIterator;
      }

      public Boolean call() throws Exception {
         return Boolean.valueOf(this.consumer.append(this.rowIterator));
      }
   }

   public interface TierPartitionConsumer {
      boolean append(UnfilteredRowIterator var1);

      static default TieredRowWriter.TierPartitionConsumer create(RangeAwareWriter writer) {
         return writer::append;
      }
   }

   class RowIterator extends AbstractUnfilteredRowIterator {
      private final BlockingQueue<Unfiltered> queue = new ArrayBlockingQueue(10);

      public RowIterator(UnfilteredRowIterator wrapped, DeletionTime partitionDeletion, Row staticRow) {
         super(TieredRowWriter.this.strategy.cfs.metadata(), wrapped.partitionKey(), partitionDeletion, wrapped.columns(), staticRow, wrapped.isReverseOrder(), wrapped.stats());
      }

      protected Unfiltered computeNext() {
         try {
            Unfiltered next = (Unfiltered)this.queue.take();
            return next != TieredRowWriter.CLOSE_SENTINEL?next:(Unfiltered)this.endOfData();
         } catch (InterruptedException var2) {
            throw new AssertionError();
         }
      }

      public void put(Unfiltered row) {
         try {
            this.queue.put(row);
         } catch (InterruptedException var3) {
            throw new AssertionError();
         }
      }

      public void close() {
         try {
            this.queue.put(TieredRowWriter.CLOSE_SENTINEL);
         } catch (InterruptedException var2) {
            throw new AssertionError();
         }
      }
   }
}
