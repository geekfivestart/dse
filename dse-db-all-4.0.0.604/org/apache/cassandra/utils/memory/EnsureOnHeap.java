package org.apache.cassandra.utils.memory;

import java.util.Iterator;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.SearchIterator;

public class EnsureOnHeap extends Transformation {
   public EnsureOnHeap() {
   }

   protected BaseRowIterator<?> applyToPartition(BaseRowIterator partition) {
      return (BaseRowIterator)(partition instanceof UnfilteredRowIterator?Transformation.apply((UnfilteredRowIterator)((UnfilteredRowIterator)partition), this):Transformation.apply((RowIterator)((RowIterator)partition), this));
   }

   public DecoratedKey applyToPartitionKey(DecoratedKey key) {
      return new BufferDecoratedKey(key.getToken(), HeapAllocator.instance.clone(key.getKey()));
   }

   public Row applyToRow(Row row) {
      return row != null && row != Rows.EMPTY_STATIC_ROW?Rows.copy(row, HeapAllocator.instance.cloningRowBuilder(row.size())).build():row;
   }

   public Row applyToStatic(Row row) {
      return row == Rows.EMPTY_STATIC_ROW?row:this.applyToRow(row);
   }

   public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
      return marker.copy(HeapAllocator.instance);
   }

   public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition) {
      return Transformation.apply((UnfilteredRowIterator)partition, this);
   }

   public SearchIterator<Clustering, Row> applyToPartition(final SearchIterator<Clustering, Row> partition) {
      return new SearchIterator<Clustering, Row>() {
         public Row next(Clustering key) {
            return EnsureOnHeap.this.applyToRow((Row)partition.next(key));
         }

         public void rewind() {
            throw new UnsupportedOperationException();
         }

         public int indexOfCurrent() {
            throw new UnsupportedOperationException();
         }
      };
   }

   public Iterator<Row> applyToPartition(final Iterator<Row> partition) {
      return new Iterator<Row>() {
         public boolean hasNext() {
            return partition.hasNext();
         }

         public Row next() {
            return EnsureOnHeap.this.applyToRow((Row)partition.next());
         }

         public void remove() {
            partition.remove();
         }
      };
   }

   public DeletionInfo applyToDeletionInfo(DeletionInfo deletionInfo) {
      return deletionInfo.copy(HeapAllocator.instance);
   }
}
