package org.apache.cassandra.db.rows;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class ThrottledUnfilteredIterator extends AbstractIterator<UnfilteredRowIterator> implements CloseableIterator<UnfilteredRowIterator> {
   private final UnfilteredRowIterator origin;
   private final int throttle;
   private UnfilteredRowIterator throttledItr;
   private Iterator<Unfiltered> overflowed = Collections.emptyIterator();

   @VisibleForTesting
   ThrottledUnfilteredIterator(UnfilteredRowIterator origin, int throttle) {
      assert origin != null;

      assert throttle > 1 : "Throttle size must be higher than 1 to properly support open and close tombstone boundaries.";

      this.origin = origin;
      this.throttle = throttle;
      this.throttledItr = null;
   }

   protected UnfilteredRowIterator computeNext() {
      while(this.throttledItr != null && this.throttledItr.hasNext()) {
         this.throttledItr.next();
      }

      if(!this.origin.hasNext()) {
         if(this.throttledItr != null) {
            return (UnfilteredRowIterator)this.endOfData();
         } else {
            return this.throttledItr = this.origin;
         }
      } else {
         this.throttledItr = new WrappingUnfilteredRowIterator(this.origin) {
            private int count = 0;
            private boolean isFirst;
            private RangeTombstoneMarker openMarker;
            private RangeTombstoneMarker closeMarker;

            {
               this.isFirst = ThrottledUnfilteredIterator.this.throttledItr == null;
               this.closeMarker = null;
            }

            public boolean hasNext() {
               return this.withinLimit() && this.wrapped.hasNext() || this.closeMarker != null;
            }

            public Unfiltered next() {
               if(this.closeMarker != null) {
                  assert this.count == ThrottledUnfilteredIterator.this.throttle;

                  Unfiltered toReturn = this.closeMarker;
                  this.closeMarker = null;
                  return toReturn;
               } else {
                  assert this.withinLimit();

                  Unfiltered next;
                  if(ThrottledUnfilteredIterator.this.overflowed.hasNext()) {
                     next = (Unfiltered)ThrottledUnfilteredIterator.this.overflowed.next();
                  } else {
                     next = (Unfiltered)this.wrapped.next();
                  }

                  this.recordNext(next);
                  return next;
               }
            }

            private void recordNext(Unfiltered unfiltered) {
               ++this.count;
               if(unfiltered.isRangeTombstoneMarker()) {
                  this.updateMarker((RangeTombstoneMarker)unfiltered);
               }

               if(this.count == ThrottledUnfilteredIterator.this.throttle && this.openMarker != null) {
                  assert this.wrapped.hasNext();

                  this.closeOpenMarker((Unfiltered)this.wrapped.next());
               }

            }

            private boolean withinLimit() {
               return this.count < ThrottledUnfilteredIterator.this.throttle;
            }

            private void updateMarker(RangeTombstoneMarker marker) {
               this.openMarker = marker.isOpen(this.isReverseOrder())?marker:null;
            }

            private void closeOpenMarker(Unfiltered next) {
               assert this.openMarker != null;

               if(next.isRangeTombstoneMarker()) {
                  RangeTombstoneMarker marker = (RangeTombstoneMarker)next;
                  if(marker.isBoundary()) {
                     RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
                     this.closeMarker = boundary.createCorrespondingCloseMarker(this.isReverseOrder());
                     ThrottledUnfilteredIterator.this.overflowed = Collections.singleton(boundary.createCorrespondingOpenMarker(this.isReverseOrder())).iterator();
                  } else {
                     assert marker.isClose(this.isReverseOrder());

                     this.updateMarker(marker);
                     this.closeMarker = marker;
                  }
               } else {
                  DeletionTime openDeletion = this.openMarker.openDeletionTime(this.isReverseOrder());
                  ByteBuffer[] buffers = next.clustering().getRawValues();
                  this.closeMarker = RangeTombstoneBoundMarker.exclusiveClose(this.isReverseOrder(), buffers, openDeletion);
                  ThrottledUnfilteredIterator.this.overflowed = Arrays.asList(new Unfiltered[]{RangeTombstoneBoundMarker.inclusiveOpen(this.isReverseOrder(), buffers, openDeletion), next}).iterator();
               }

            }

            public DeletionTime partitionLevelDeletion() {
               return this.isFirst?this.wrapped.partitionLevelDeletion():DeletionTime.LIVE;
            }

            public Row staticRow() {
               return this.isFirst?this.wrapped.staticRow():Rows.EMPTY_STATIC_ROW;
            }

            public void close() {
            }
         };
         return this.throttledItr;
      }
   }

   public void close() {
      if(this.origin != null) {
         this.origin.close();
      }

   }

   public static CloseableIterator<UnfilteredRowIterator> throttle(UnfilteredRowIterator iterator, int maxBatchSize) {
      return new ThrottledUnfilteredIterator(iterator, maxBatchSize);
   }

   public static CloseableIterator<UnfilteredRowIterator> throttle(final UnfilteredPartitionIterator partitionIterator, final int maxBatchSize) {
      return (CloseableIterator)(maxBatchSize == 0?partitionIterator:new AbstractIterator<UnfilteredRowIterator>() {
         ThrottledUnfilteredIterator current = null;

         protected UnfilteredRowIterator computeNext() {
            if(this.current != null && !this.current.hasNext()) {
               this.current.close();
               this.current = null;
            }

            if(this.current == null && partitionIterator.hasNext()) {
               this.current = new ThrottledUnfilteredIterator((UnfilteredRowIterator)partitionIterator.next(), maxBatchSize);
            }

            return this.current != null && this.current.hasNext()?(UnfilteredRowIterator)this.current.next():(UnfilteredRowIterator)this.endOfData();
         }

         public void close() {
            if(this.current != null) {
               this.current.close();
            }

         }
      });
   }
}
