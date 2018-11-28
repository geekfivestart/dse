package org.apache.cassandra.db.partitions;

import com.google.common.hash.Hasher;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.flow.Flow;

public abstract class UnfilteredPartitionIterators {
   private static final Comparator<UnfilteredRowIterator> partitionComparator = Comparator.comparing(BaseRowIterator::partitionKey);

   private UnfilteredPartitionIterators() {
   }

   public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand command) {
      UnfilteredRowIterator toReturn = iter.hasNext()?(UnfilteredRowIterator)iter.next():EmptyIterators.unfilteredRow(command.metadata(), command.partitionKey(), command.clusteringIndexFilter().isReversed());
      class Close extends Transformation {
         Close() {
         }

         public void onPartitionClose() {
            boolean hadNext = iter.hasNext();
            iter.close();

            assert !hadNext;

         }
      }

      return Transformation.apply((UnfilteredRowIterator)toReturn, new Close());
   }

   public static UnfilteredPartitionIterator concat(final List<UnfilteredPartitionIterator> iterators, TableMetadata metadata) {
      class Extend implements MorePartitions<UnfilteredPartitionIterator> {
         int i = 1;

         Extend() {
         }

         public UnfilteredPartitionIterator moreContents() {
            return this.i >= iterators.size()?null:(UnfilteredPartitionIterator)iterators.get(this.i++);
         }
      }

      return iterators.isEmpty()?EmptyIterators.unfilteredPartition(metadata):(iterators.size() == 1?(UnfilteredPartitionIterator)iterators.get(0):MorePartitions.extend((UnfilteredPartitionIterator)((UnfilteredPartitionIterator)iterators.get(0)), new Extend()));
   }

   public static Single<UnfilteredPartitionIterator> concat(Iterable<Single<UnfilteredPartitionIterator>> iterators) {
      Iterator<Single<UnfilteredPartitionIterator>> it = iterators.iterator();

      assert it.hasNext();

      Single<UnfilteredPartitionIterator> it0 = (Single)it.next();
      return !it.hasNext()?it0:it0.map((i) -> {
         class Extend implements MorePartitions<UnfilteredPartitionIterator> {
            Extend() {
            }

            public UnfilteredPartitionIterator moreContents() {
               return !it.hasNext()?null:(UnfilteredPartitionIterator)((Single)it.next()).blockingGet();
            }
         }

         return MorePartitions.extend((UnfilteredPartitionIterator)i, new Extend());
      });
   }

   public static PartitionIterator filter(UnfilteredPartitionIterator iterator, int nowInSec) {
      return FilteredPartitions.filter(iterator, nowInSec);
   }

   public static UnfilteredPartitionIterator merge(List<? extends UnfilteredPartitionIterator> iterators, int nowInSec, final UnfilteredPartitionIterators.MergeListener listener) {
      assert listener != null;

      assert !iterators.isEmpty();

      final TableMetadata metadata = ((UnfilteredPartitionIterator)iterators.get(0)).metadata();
      UnfilteredPartitionIterators.UnfilteredMergeReducer reducer = new UnfilteredPartitionIterators.UnfilteredMergeReducer(metadata, iterators.size(), nowInSec, listener);
      final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, reducer);
      return new AbstractUnfilteredPartitionIterator() {
         public TableMetadata metadata() {
            return metadata;
         }

         public boolean hasNext() {
            return merged.hasNext();
         }

         public UnfilteredRowIterator next() {
            return (UnfilteredRowIterator)merged.next();
         }

         public void close() {
            merged.close();
            listener.close();
         }
      };
   }

   public static Completable digest(UnfilteredPartitionIterator iterator, Hasher hasher, DigestVersion version) {
      return Completable.fromAction(() -> {
         UnfilteredPartitionIterator iter = iterator;
         Throwable var4 = null;

         try {
            while(iter.hasNext()) {
               UnfilteredRowIterator partition = (UnfilteredRowIterator)iter.next();
               Throwable var6 = null;

               try {
                  UnfilteredRowIterators.digest(partition, hasher, version);
               } catch (Throwable var29) {
                  var6 = var29;
                  throw var29;
               } finally {
                  if(partition != null) {
                     if(var6 != null) {
                        try {
                           partition.close();
                        } catch (Throwable var28) {
                           var6.addSuppressed(var28);
                        }
                     } else {
                        partition.close();
                     }
                  }

               }
            }
         } catch (Throwable var31) {
            var4 = var31;
            throw var31;
         } finally {
            if(iter != null) {
               if(var4 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var27) {
                     var4.addSuppressed(var27);
                  }
               } else {
                  iter.close();
               }
            }

         }

      });
   }

   public static Flow<Void> digest(Flow<FlowableUnfilteredPartition> partitions, Hasher hasher, DigestVersion version) {
      return partitions.flatProcess((partition) -> {
         return UnfilteredRowIterators.digest(partition, hasher, version);
      });
   }

   public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails) {
      class Logging extends Transformation<UnfilteredRowIterator> {
         Logging() {
         }

         public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition) {
            return UnfilteredRowIterators.loggingIterator(partition, id, fullDetails);
         }
      }

      return Transformation.apply((UnfilteredPartitionIterator)iterator, new Logging());
   }

   static class UnfilteredMergeReducer extends Reducer<UnfilteredRowIterator, UnfilteredRowIterator> {
      private final int numIterators;
      private final int nowInSec;
      private final UnfilteredPartitionIterators.MergeListener listener;
      private final List<UnfilteredRowIterator> toMerge;
      private final UnfilteredRowIterator emptyIterator;
      private DecoratedKey partitionKey;
      private boolean isReverseOrder;

      UnfilteredMergeReducer(TableMetadata metadata, int numIterators, int nowInSec, UnfilteredPartitionIterators.MergeListener listener) {
         this.numIterators = numIterators;
         this.nowInSec = nowInSec;
         this.listener = listener;
         this.emptyIterator = EmptyIterators.unfilteredRow(metadata, (DecoratedKey)null, false);
         this.toMerge = new ArrayList(numIterators);
      }

      public void reduce(int idx, UnfilteredRowIterator current) {
         this.partitionKey = current.partitionKey();
         this.isReverseOrder = current.isReverseOrder();
         this.toMerge.set(idx, current);
      }

      public UnfilteredRowIterator getReduced() {
         UnfilteredRowIterators.MergeListener rowListener = this.listener.getRowMergeListener(this.partitionKey, this.toMerge);
         ((EmptyIterators.EmptyUnfilteredRowIterator)this.emptyIterator).reuse(this.partitionKey, this.isReverseOrder, DeletionTime.LIVE);
         UnfilteredRowIterator nonEmptyRowIterator = null;
         int numNonEmptyRowIterators = 0;
         int i = 0;

         for(int length = this.toMerge.size(); i < length; ++i) {
            UnfilteredRowIterator element = (UnfilteredRowIterator)this.toMerge.get(i);
            if(element == null) {
               this.toMerge.set(i, this.emptyIterator);
            } else {
               ++numNonEmptyRowIterators;
               nonEmptyRowIterator = element;
            }
         }

         return numNonEmptyRowIterators == 1 && !this.listener.callOnTrivialMerge()?nonEmptyRowIterator:UnfilteredRowIterators.merge(this.toMerge, this.nowInSec, rowListener);
      }

      public void onKeyChange() {
         this.toMerge.clear();
         int i = 0;

         for(int length = this.numIterators; i < length; ++i) {
            this.toMerge.add(null);
         }

      }
   }

   public interface MergeListener {
      UnfilteredPartitionIterators.MergeListener NONE = new UnfilteredPartitionIterators.MergeListener() {
         public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions) {
            return null;
         }

         public boolean callOnTrivialMerge() {
            return false;
         }

         public void close() {
         }
      };

      UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey var1, List<UnfilteredRowIterator> var2);

      default boolean callOnTrivialMerge() {
         return true;
      }

      void close();
   }
}
