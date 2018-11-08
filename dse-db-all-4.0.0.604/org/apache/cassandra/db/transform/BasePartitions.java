package org.apache.cassandra.db.transform;

import java.util.Collections;
import java.util.Iterator;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.utils.Throwables;

public abstract class BasePartitions<R extends BaseRowIterator<?>, I extends BasePartitionIterator<? extends BaseRowIterator<?>>> extends BaseIterator<BaseRowIterator<?>, I, R> implements BasePartitionIterator<R> {
   public BasePartitions(I input) {
      super((Iterator)input);
   }

   BasePartitions(BasePartitions<?, ? extends I> copyFrom) {
      super((BaseIterator)copyFrom);
   }

   protected BaseRowIterator<?> applyOne(BaseRowIterator<?> value, Transformation transformation) {
      return value == null?null:transformation.applyToPartition(value);
   }

   public void add(Transformation transformation) {
      super.add(transformation);
      this.next = this.applyOne((BaseRowIterator)this.next, transformation);
   }

   protected Throwable runOnClose(int length) {
      Throwable fail = null;
      Transformation[] fs = this.stack;

      for(int i = 0; i < length; ++i) {
         try {
            fs[i].onClose();
         } catch (Throwable var6) {
            fail = Throwables.merge(fail, var6);
         }
      }

      return fail;
   }

   public final boolean hasNext() {
      BaseRowIterator next = null;

      try {
         do {
            if(this.next != null) {
               return true;
            }

            Transformation[] fs = this.stack;
            int len = this.length;

            while(((BasePartitionIterator)this.input).hasNext()) {
               next = (BaseRowIterator)((BasePartitionIterator)this.input).next();

               for(int i = 0; next != null & i < len; ++i) {
                  next = fs[i].applyToPartition(next);
               }

               if(next != null) {
                  this.next = next;
                  return true;
               }
            }
         } while(this.hasMoreContents());

         return false;
      } catch (Throwable var5) {
         if(next != null) {
            Throwables.close(var5, Collections.singleton(next));
         }

         throw var5;
      }
   }
}
