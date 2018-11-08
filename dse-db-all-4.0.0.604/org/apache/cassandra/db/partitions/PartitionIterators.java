package org.apache.cassandra.db.partitions;

import java.util.List;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.RowIterators;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.AbstractIterator;

public abstract class PartitionIterators {
   private PartitionIterators() {
   }

   public static RowIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadCommand command) {
      RowIterator toReturn = iter.hasNext()?(RowIterator)iter.next():EmptyIterators.row(command.metadata(), command.partitionKey(), command.clusteringIndexFilter().isReversed());
      class Close extends Transformation {
         Close() {
         }

         public void onPartitionClose() {
            boolean hadNext = iter.hasNext();
            iter.close();

            assert !hadNext;

         }
      }

      return Transformation.apply((RowIterator)toReturn, new Close());
   }

   public static PartitionIterator concat(final List<PartitionIterator> iterators) {
      class Extend implements MorePartitions<PartitionIterator> {
         int i = 1;

         Extend() {
         }

         public PartitionIterator moreContents() {
            return this.i >= iterators.size()?null:(PartitionIterator)iterators.get(this.i++);
         }
      }

      return iterators.size() == 1?(PartitionIterator)iterators.get(0):MorePartitions.extend((PartitionIterator)((PartitionIterator)iterators.get(0)), new Extend());
   }

   public static PartitionIterator singletonIterator(RowIterator iterator) {
      return new PartitionIterators.SingletonPartitionIterator(iterator);
   }

   public static void consume(PartitionIterator iterator) {
      label83:
      while(true) {
         if(iterator.hasNext()) {
            RowIterator partition = (RowIterator)iterator.next();
            Throwable var2 = null;

            try {
               while(true) {
                  if(!partition.hasNext()) {
                     continue label83;
                  }

                  partition.next();
               }
            } catch (Throwable var11) {
               var2 = var11;
               throw var11;
            } finally {
               if(partition != null) {
                  if(var2 != null) {
                     try {
                        partition.close();
                     } catch (Throwable var10) {
                        var2.addSuppressed(var10);
                     }
                  } else {
                     partition.close();
                  }
               }

            }
         }

         return;
      }
   }

   public static PartitionIterator loggingIterator(PartitionIterator iterator, final String id) {
      class Logger extends Transformation<RowIterator> {
         Logger() {
         }

         public RowIterator applyToPartition(RowIterator partition) {
            return RowIterators.loggingIterator(partition, id);
         }
      }

      return Transformation.apply((PartitionIterator)iterator, new Logger());
   }

   private static class SingletonPartitionIterator extends AbstractIterator<RowIterator> implements PartitionIterator {
      private final RowIterator iterator;
      private boolean returned;

      private SingletonPartitionIterator(RowIterator iterator) {
         this.returned = false;
         this.iterator = iterator;
      }

      protected RowIterator computeNext() {
         if(this.returned) {
            return (RowIterator)this.endOfData();
         } else {
            this.returned = true;
            return this.iterator;
         }
      }

      public void close() {
         if(!this.returned) {
            this.iterator.close();
         }

      }
   }
}
