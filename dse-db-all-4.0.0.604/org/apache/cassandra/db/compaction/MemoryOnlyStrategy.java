package org.apache.cassandra.db.compaction;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.db.mos.MemoryOnlyStrategyOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryOnlyStrategy extends AbstractCompactionStrategy {
   private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategy.class);
   private final Set<SSTableReader> sstables = Collections.newSetFromMap(new ConcurrentHashMap());
   protected MemoryOnlyStrategyOptions options;
   private static final Comparator<SSTableReader> maxSizeComparator = new Comparator<SSTableReader>() {
      public int compare(SSTableReader o1, SSTableReader o2) {
         return Long.compare(o2.onDiskLength(), o1.onDiskLength());
      }
   };

   public MemoryOnlyStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      super(cfs, options);
      if(!cfs.isIndex() && cfs.metadata.get().params.caching.cacheKeys()) {
         logger.error("Table {}.{} uses MemoryOnlyStrategy and should have `caching` set to {'keys':'NONE', 'rows_per_partition':'NONE'} (got {})", new Object[]{cfs.keyspace.getName(), cfs.getTableName(), cfs.metadata.get().params.caching});
      }

      this.options = new MemoryOnlyStrategyOptions(options);
   }

   private List<SSTableReader> getNextBackgroundSSTables(int gcBefore) {
      if(Iterables.isEmpty(this.cfs.getSSTables(SSTableSet.LIVE))) {
         return Collections.emptyList();
      } else {
         MemoryOnlyStrategy.CompactionProgress progress = new MemoryOnlyStrategy.CompactionProgress(this.cfs.metadata(), null);
         if(progress.numActiveCompactions >= this.options.maxActiveCompactions) {
            return Collections.emptyList();
         } else {
            long maxCompactionSize = progress.minRemainingBytes / 16L * 15L;
            Iterator<SSTableReader> it = Iterables.filter(this.cfs.getUncompactingSSTables(), (sstable) -> {
               return this.sstables.contains(sstable) && !sstable.isMarkedSuspect();
            }).iterator();
            if(!it.hasNext()) {
               return Collections.emptyList();
            } else {
               List<SSTableReader> cands = Lists.newArrayList(it);
               if(cands.size() < this.options.minCompactionThreshold) {
                  return Collections.emptyList();
               } else {
                  Collections.sort(cands, maxSizeComparator);
                  long totalSize = 0L;
                  int candIndex = 0;

                  for(int maxCandIndex = Math.min(this.options.maxCompactionThreshold, cands.size()); candIndex < maxCandIndex && (totalSize += ((SSTableReader)cands.get(candIndex)).onDiskLength()) <= maxCompactionSize; ++candIndex) {
                     ;
                  }

                  return cands.subList(0, candIndex);
               }
            }
         }
      }
   }

   public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      LifecycleTransaction transaction;
      do {
         List<SSTableReader> toCompact = this.getNextBackgroundSSTables(gcBefore);
         if(toCompact.isEmpty()) {
            return null;
         }

         transaction = this.cfs.getTracker().tryModify((Iterable)toCompact, OperationType.COMPACTION);
      } while(transaction == null);

      return new CompactionTask(this.cfs, transaction, gcBefore);
   }

   public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
      Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(this.sstables);
      if(Iterables.isEmpty(filteredSSTables)) {
         return null;
      } else {
         LifecycleTransaction txn = this.cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
         return txn == null?null:Arrays.asList(new AbstractCompactionTask[]{new CompactionTask(this.cfs, txn, gcBefore)});
      }
   }

   public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
      assert !sstables.isEmpty();

      LifecycleTransaction transaction = this.cfs.getTracker().tryModify((Iterable)sstables, OperationType.COMPACTION);
      if(transaction == null) {
         logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
         return null;
      } else {
         return (new CompactionTask(this.cfs, transaction, gcBefore)).setUserDefined(true);
      }
   }

   public int getEstimatedRemainingTasks() {
      int theSize = this.sstables.size();
      return theSize >= this.cfs.getMinimumCompactionThreshold()?theSize / this.cfs.getMaximumCompactionThreshold() + 1:0;
   }

   public long getMaxSSTableBytes() {
      return 9223372036854775807L;
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
      uncheckedOptions = MemoryOnlyStrategyOptions.validateOptions(options, uncheckedOptions);
      return uncheckedOptions;
   }

   public boolean shouldDefragment() {
      return false;
   }

   public void addSSTable(SSTableReader added) {
      if(!this.sstables.contains(added)) {
         added.lock(MemoryOnlyStatus.instance);
         this.sstables.add(added);
      }
   }

   public void removeSSTable(SSTableReader removed) {
      if(this.sstables.contains(removed)) {
         removed.unlock(MemoryOnlyStatus.instance);
         this.sstables.remove(removed);
      }
   }

   public void shutdown() {
      this.sstables.forEach((sstable) -> {
         sstable.unlock(MemoryOnlyStatus.instance);
      });
      this.sstables.clear();
      super.shutdown();
   }

   public Set<SSTableReader> getSSTables() {
      return ImmutableSet.copyOf(this.sstables);
   }

   public String toString() {
      return String.format("MemoryOnlyStrategy[%s/%s/%s]", new Object[]{Integer.valueOf(this.cfs.getMinimumCompactionThreshold()), Integer.valueOf(this.cfs.getMaximumCompactionThreshold()), Long.valueOf(MemoryOnlyStatus.instance.getMaxAvailableBytes())});
   }

   private static final class CompactionProgress {
      long minRemainingBytes;
      int numActiveCompactions;

      private CompactionProgress(TableMetadata metadata) {
         this.minRemainingBytes = 9223372036854775807L;
         this.numActiveCompactions = 0;
         Iterator var2 = CompactionMetrics.getCompactions().iterator();

         while(var2.hasNext()) {
            CompactionInfo.Holder compaction = (CompactionInfo.Holder)var2.next();
            CompactionInfo info = compaction.getCompactionInfo();
            if(null != info.getTableMetadata() && info.getTableMetadata().id.equals(metadata.id)) {
               ++this.numActiveCompactions;
               this.minRemainingBytes = Math.min(info.getTotal() - info.getCompleted(), this.minRemainingBytes);
            }
         }

      }
   }
}
