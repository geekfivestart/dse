package org.apache.cassandra.db;

import com.google.common.collect.UnmodifiableIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeEstimatesRecorder extends SchemaChangeListener implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(SizeEstimatesRecorder.class);
   public static final SizeEstimatesRecorder instance = new SizeEstimatesRecorder();

   private SizeEstimatesRecorder() {
      Schema.instance.registerListener(this);
   }

   public void run() {
      TokenMetadata metadata = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap();
      if(!metadata.isMember(FBUtilities.getBroadcastAddress())) {
         logger.debug("Node is not part of the ring; not recording size estimates");
      } else {
         logger.trace("Recording size estimates");
         Iterator var2 = Keyspace.nonLocalStrategy().iterator();

         while(var2.hasNext()) {
            Keyspace keyspace = (Keyspace)var2.next();
            Collection<Range<Token>> localRanges = StorageService.instance.getPrimaryRangesForEndpoint(keyspace.getName(), FBUtilities.getBroadcastAddress());
            Iterator var5 = keyspace.getColumnFamilyStores().iterator();

            while(var5.hasNext()) {
               ColumnFamilyStore table = (ColumnFamilyStore)var5.next();
               long start = System.nanoTime();
               this.recordSizeEstimates(table, localRanges);
               long passed = System.nanoTime() - start;
               logger.trace("Spent {} milliseconds on estimating {}.{} size", new Object[]{Long.valueOf(TimeUnit.NANOSECONDS.toMillis(passed)), table.metadata.keyspace, table.metadata.name});
            }
         }

      }
   }

   private void recordSizeEstimates(ColumnFamilyStore table, Collection<Range<Token>> localRanges) {
      Map<Range<Token>, Pair<Long, Long>> estimates = new HashMap(localRanges.size());
      Iterator var4 = localRanges.iterator();

      while(var4.hasNext()) {
         Range<Token> localRange = (Range)var4.next();

         Range unwrappedRange;
         long partitionsCount;
         long meanPartitionSize;
         for(UnmodifiableIterator var6 = localRange.unwrap().iterator(); var6.hasNext(); estimates.put(unwrappedRange, Pair.create(Long.valueOf(partitionsCount), Long.valueOf(meanPartitionSize)))) {
            unwrappedRange = (Range)var6.next();
            Refs refs = null;

            try {
               while(refs == null) {
                  Iterable<SSTableReader> sstables = table.getTracker().getView().select(SSTableSet.CANONICAL);
                  SSTableIntervalTree tree = SSTableIntervalTree.build(sstables);
                  Range<PartitionPosition> r = Range.makeRowRange(unwrappedRange);
                  Iterable<SSTableReader> canonicalSSTables = View.sstablesInBounds((PartitionPosition)r.left, (PartitionPosition)r.right, tree);
                  refs = Refs.tryRef((Iterable)canonicalSSTables);
               }

               partitionsCount = this.estimatePartitionsCount(refs, unwrappedRange);
               meanPartitionSize = this.estimateMeanPartitionSize(refs);
            } finally {
               if(refs != null) {
                  refs.release();
               }

            }
         }
      }

      TPCUtils.blockingAwait(SystemKeyspace.updateSizeEstimates(table.metadata.keyspace, table.metadata.name, estimates));
   }

   private long estimatePartitionsCount(Collection<SSTableReader> sstables, Range<Token> range) {
      long count = 0L;

      SSTableReader sstable;
      for(Iterator var5 = sstables.iterator(); var5.hasNext(); count += sstable.estimatedKeysForRanges(Collections.singleton(range))) {
         sstable = (SSTableReader)var5.next();
      }

      return count;
   }

   private long estimateMeanPartitionSize(Collection<SSTableReader> sstables) {
      long sum = 0L;
      long count = 0L;

      long n;
      for(Iterator var6 = sstables.iterator(); var6.hasNext(); count += n) {
         SSTableReader sstable = (SSTableReader)var6.next();
         n = sstable.getEstimatedPartitionSize().count();
         sum += sstable.getEstimatedPartitionSize().mean() * n;
      }

      return count > 0L?sum / count:0L;
   }

   public void onDropTable(String keyspace, String table) {
      TPCUtils.blockingAwait(SystemKeyspace.clearSizeEstimates(keyspace, table));
   }
}
