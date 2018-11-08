package org.apache.cassandra.db.transform;

import java.util.Iterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public abstract class BaseRows<R extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>> extends BaseIterator<Unfiltered, I, R> implements BaseRowIterator<R> {
   private Row staticRow;
   private DecoratedKey partitionKey;

   public BaseRows(I input) {
      super((Iterator)input);
      this.staticRow = input.staticRow();
      this.partitionKey = input.partitionKey();
   }

   BaseRows(BaseRows<?, ? extends I> copyFrom) {
      super((BaseIterator)copyFrom);
      this.staticRow = copyFrom.staticRow;
      this.partitionKey = copyFrom.partitionKey();
   }

   public TableMetadata metadata() {
      return ((BaseRowIterator)this.input).metadata();
   }

   public boolean isReverseOrder() {
      return ((BaseRowIterator)this.input).isReverseOrder();
   }

   public RegularAndStaticColumns columns() {
      return ((BaseRowIterator)this.input).columns();
   }

   public DecoratedKey partitionKey() {
      return ((BaseRowIterator)this.input).partitionKey();
   }

   public Row staticRow() {
      return this.staticRow == null?Rows.EMPTY_STATIC_ROW:this.staticRow;
   }

   protected Throwable runOnClose(int length) {
      Throwable fail = null;
      Transformation[] fs = this.stack;

      for(int i = 0; i < length; ++i) {
         try {
            fs[i].onPartitionClose();
         } catch (Throwable var6) {
            fail = Throwables.merge(fail, var6);
         }
      }

      return fail;
   }

   public void add(Transformation transformation) {
      super.add(transformation);
      if(this.staticRow != null) {
         this.staticRow = transformation.applyToStatic(this.staticRow);
      }

      this.next = this.applyOne((Unfiltered)this.next, transformation);
      this.partitionKey = transformation.applyToPartitionKey(this.partitionKey);
   }

   protected Unfiltered applyOne(Unfiltered value, Transformation transformation) {
      return (Unfiltered)(value == null?null:(value instanceof Row?transformation.applyToRow((Row)value):transformation.applyToMarker((RangeTombstoneMarker)value)));
   }

   public final boolean hasNext() {
      label54:
      while(this.next == null) {
         Transformation[] fs = this.stack;
         int len = this.length;

         Object next;
         do {
            if(!((BaseRowIterator)this.input).hasNext()) {
               if(this.hasMoreContents()) {
                  continue label54;
               }

               return false;
            }

            Unfiltered next = (Unfiltered)((BaseRowIterator)this.input).next();
            int i;
            if(next.isRow()) {
               Row row = (Row)next;

               for(i = 0; row != null && i < len; ++i) {
                  row = fs[i].applyToRow(row);
               }

               next = row;
            } else {
               RangeTombstoneMarker rtm = (RangeTombstoneMarker)next;

               for(i = 0; rtm != null && i < len; ++i) {
                  rtm = fs[i].applyToMarker(rtm);
               }

               next = rtm;
            }
         } while(next == null);

         this.next = next;
         return true;
      }

      return true;
   }
}
