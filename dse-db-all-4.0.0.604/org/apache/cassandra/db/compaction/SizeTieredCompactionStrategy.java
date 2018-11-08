package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy {
   private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);
   private static final Comparator<Pair<List<SSTableReader>, Double>> bucketsByHotnessComparator = new Comparator<Pair<List<SSTableReader>, Double>>() {
      public int compare(Pair<List<SSTableReader>, Double> o1, Pair<List<SSTableReader>, Double> o2) {
         int comparison = Double.compare(((Double)o1.right).doubleValue(), ((Double)o2.right).doubleValue());
         return comparison != 0?comparison:Long.compare(this.avgSize((List)o1.left), this.avgSize((List)o2.left));
      }

      private long avgSize(List<SSTableReader> sstables) {
         long n = 0L;

         SSTableReader sstable;
         for(Iterator var4 = sstables.iterator(); var4.hasNext(); n += sstable.bytesOnDisk()) {
            sstable = (SSTableReader)var4.next();
         }

         return n / (long)sstables.size();
      }
   };
   protected SizeTieredCompactionStrategyOptions sizeTieredOptions;
   protected volatile int estimatedRemainingTasks = 0;
   @VisibleForTesting
   protected final Set<SSTableReader> sstables = new HashSet();

   public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      super(cfs, options);
      this.sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
   }

   private synchronized List<SSTableReader> getNextBackgroundSSTables(int gcBefore) {
      int minThreshold = this.cfs.getMinimumCompactionThreshold();
      int maxThreshold = this.cfs.getMaximumCompactionThreshold();
      Iterable var10000 = this.cfs.getUncompactingSSTables();
      Set var10001 = this.sstables;
      this.sstables.getClass();
      Iterable<SSTableReader> candidates = filterSuspectSSTables(Iterables.filter(var10000, var10001::contains));
      List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(candidates), this.sizeTieredOptions.bucketHigh, this.sizeTieredOptions.bucketLow, this.sizeTieredOptions.minSSTableSize);
      logger.trace("Compaction buckets are {}", buckets);
      this.estimatedRemainingTasks = getEstimatedCompactionsByTasks(this.cfs, buckets);
      this.cfs.getCompactionStrategyManager().compactionLogger.pending(this, this.estimatedRemainingTasks);
      List<SSTableReader> mostInteresting = mostInterestingBucket(buckets, minThreshold, maxThreshold);
      if(!mostInteresting.isEmpty()) {
         return mostInteresting;
      } else {
         List<SSTableReader> sstablesWithTombstones = new ArrayList();
         Iterator var8 = candidates.iterator();

         while(var8.hasNext()) {
            SSTableReader sstable = (SSTableReader)var8.next();
            if(this.worthDroppingTombstones(sstable, gcBefore)) {
               sstablesWithTombstones.add(sstable);
            }
         }

         if(sstablesWithTombstones.isEmpty()) {
            return Collections.emptyList();
         } else {
            return Collections.singletonList(Collections.max(sstablesWithTombstones, SSTableReader.sizeComparator));
         }
      }
   }

   public static List<SSTableReader> mostInterestingBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold) {
      List<Pair<List<SSTableReader>, Double>> prunedBucketsAndHotness = new ArrayList(buckets.size());
      Iterator var4 = buckets.iterator();

      while(var4.hasNext()) {
         List<SSTableReader> bucket = (List)var4.next();
         Pair<List<SSTableReader>, Double> bucketAndHotness = trimToThresholdWithHotness(bucket, maxThreshold);
         if(bucketAndHotness != null && ((List)bucketAndHotness.left).size() >= minThreshold) {
            prunedBucketsAndHotness.add(bucketAndHotness);
         }
      }

      if(prunedBucketsAndHotness.isEmpty()) {
         return Collections.emptyList();
      } else {
         Pair<List<SSTableReader>, Double> hottest = (Pair)Collections.max(prunedBucketsAndHotness, bucketsByHotnessComparator);
         return (List)hottest.left;
      }
   }

   @VisibleForTesting
   static Pair<List<SSTableReader>, Double> trimToThresholdWithHotness(List<SSTableReader> bucket, int maxThreshold) {
      final Map<SSTableReader, Double> hotnessSnapshot = getHotnessMap(bucket);
      Collections.sort(bucket, new Comparator<SSTableReader>() {
         public int compare(SSTableReader o1, SSTableReader o2) {
            return -1 * Double.compare(((Double)hotnessSnapshot.get(o1)).doubleValue(), ((Double)hotnessSnapshot.get(o2)).doubleValue());
         }
      });
      List<SSTableReader> prunedBucket = bucket.subList(0, Math.min(bucket.size(), maxThreshold));
      double bucketHotness = 0.0D;

      SSTableReader sstr;
      for(Iterator var6 = prunedBucket.iterator(); var6.hasNext(); bucketHotness += hotness(sstr)) {
         sstr = (SSTableReader)var6.next();
      }

      return Pair.create(prunedBucket, Double.valueOf(bucketHotness));
   }

   private static Map<SSTableReader, Double> getHotnessMap(Collection<SSTableReader> sstables) {
      Map<SSTableReader, Double> hotness = new HashMap(sstables.size());
      Iterator var2 = sstables.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         hotness.put(sstable, Double.valueOf(hotness(sstable)));
      }

      return hotness;
   }

   private static double hotness(SSTableReader sstr) {
      return sstr.getReadMeter() == null?0.0D:sstr.getReadMeter().twoHourRate() / (double)sstr.estimatedKeys();
   }

   public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      List previousCandidate = null;

      while(true) {
         List<SSTableReader> hottestBucket = this.getNextBackgroundSSTables(gcBefore);
         if(hottestBucket.isEmpty()) {
            return null;
         }

         if(hottestBucket.equals(previousCandidate)) {
            logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se,unless it happens frequently, in which case it must be reported. Will retry later.", hottestBucket);
            return null;
         }

         LifecycleTransaction transaction = this.cfs.getTracker().tryModify((Iterable)hottestBucket, OperationType.COMPACTION);
         if(transaction != null) {
            return new CompactionTask(this.cfs, transaction, gcBefore);
         }

         previousCandidate = hottestBucket;
      }
   }

   public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
      Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(this.sstables);
      if(Iterables.isEmpty(filteredSSTables)) {
         return null;
      } else {
         LifecycleTransaction txn = this.cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
         return txn == null?null:(splitOutput?Arrays.asList(new AbstractCompactionTask[]{new SizeTieredCompactionStrategy.SplittingCompactionTask(this.cfs, txn, gcBefore)}):Arrays.asList(new AbstractCompactionTask[]{new CompactionTask(this.cfs, txn, gcBefore)}));
      }
   }

   public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
      assert !sstables.isEmpty();

      LifecycleTransaction transaction = this.cfs.getTracker().tryModify((Iterable)sstables, OperationType.COMPACTION);
      if(transaction == null) {
         logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
         return null;
      } else {
         return (new CompactionTask(this.cfs, transaction, gcBefore)).setUserDefined(true);
      }
   }

   public int getEstimatedRemainingTasks() {
      return this.estimatedRemainingTasks;
   }

   public static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Iterable<SSTableReader> sstables) {
      List<Pair<SSTableReader, Long>> sstableLengthPairs = new ArrayList(Iterables.size(sstables));
      Iterator var2 = sstables.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         sstableLengthPairs.add(Pair.create(sstable, Long.valueOf(sstable.onDiskLength())));
      }

      return sstableLengthPairs;
   }

   public static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, double bucketHigh, double bucketLow, long minSSTableSize) {
      List<Pair<T, Long>> sortedFiles = new ArrayList(files);
      Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>() {
         public int compare(Pair<T, Long> p1, Pair<T, Long> p2) {
            return ((Long)p1.right).compareTo((Long)p2.right);
         }
      });
      Map<Long, List<T>> buckets = new HashMap();
      Iterator var9 = sortedFiles.iterator();

      while(true) {
         label28:
         while(var9.hasNext()) {
            Pair<T, Long> pair = (Pair)var9.next();
            long size = ((Long)pair.right).longValue();
            Iterator var13 = buckets.entrySet().iterator();

            while(var13.hasNext()) {
               Entry<Long, List<T>> entry = (Entry)var13.next();
               List<T> bucket = (List)entry.getValue();
               long oldAverageSize = ((Long)entry.getKey()).longValue();
               if((double)size > (double)oldAverageSize * bucketLow && (double)size < (double)oldAverageSize * bucketHigh || size < minSSTableSize && oldAverageSize < minSSTableSize) {
                  buckets.remove(Long.valueOf(oldAverageSize));
                  long totalSize = (long)bucket.size() * oldAverageSize;
                  long newAverageSize = (totalSize + size) / (long)(bucket.size() + 1);
                  bucket.add(pair.left);
                  buckets.put(Long.valueOf(newAverageSize), bucket);
                  continue label28;
               }
            }

            ArrayList<T> bucket = new ArrayList();
            bucket.add(pair.left);
            buckets.put(Long.valueOf(size), bucket);
         }

         return new ArrayList(buckets.values());
      }
   }

   public static int getEstimatedCompactionsByTasks(ColumnFamilyStore cfs, List<List<SSTableReader>> tasks) {
      int n = 0;
      Iterator var3 = tasks.iterator();

      while(var3.hasNext()) {
         List<SSTableReader> bucket = (List)var3.next();
         if(bucket.size() >= cfs.getMinimumCompactionThreshold()) {
            n = (int)((double)n + Math.ceil((double)bucket.size() / (double)cfs.getMaximumCompactionThreshold()));
         }
      }

      return n;
   }

   public long getMaxSSTableBytes() {
      return 9223372036854775807L;
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
      uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
      uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
      uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
      return uncheckedOptions;
   }

   public boolean shouldDefragment() {
      return true;
   }

   public void addSSTable(SSTableReader added) {
      this.sstables.add(added);
   }

   public void removeSSTable(SSTableReader sstable) {
      this.sstables.remove(sstable);
   }

   protected Set<SSTableReader> getSSTables() {
      return ImmutableSet.copyOf(this.sstables);
   }

   public String toString() {
      return String.format("SizeTieredCompactionStrategy[%s/%s]", new Object[]{Integer.valueOf(this.cfs.getMinimumCompactionThreshold()), Integer.valueOf(this.cfs.getMaximumCompactionThreshold())});
   }

   private static class SplittingCompactionTask extends CompactionTask {
      public SplittingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore) {
         super(cfs, txn, gcBefore);
      }

      public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
         return new SplittingSizeTieredCompactionWriter(cfs, directories, txn, nonExpiredSSTables);
      }
   }
}
