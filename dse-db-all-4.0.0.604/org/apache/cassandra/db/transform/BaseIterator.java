package org.apache.cassandra.db.transform;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

public abstract class BaseIterator<V, I extends Iterator<? extends V>, O extends V> extends Stack implements AutoCloseable, Iterator<O> {
   I input;
   V next;

   BaseIterator(BaseIterator<? extends V, ? extends I, ?> copyFrom) {
      super(copyFrom);
      this.input = copyFrom.input;
      this.next = copyFrom.next;
   }

   BaseIterator(I input) {
      this.input = input;
   }

   protected abstract Throwable runOnClose(int var1);

   protected abstract V applyOne(V var1, Transformation var2);

   public final void close() {
      Throwable fail = this.runOnClose(this.length);
      if(this.next instanceof AutoCloseable) {
         try {
            ((AutoCloseable)this.next).close();
         } catch (Throwable var4) {
            fail = Throwables.merge(fail, var4);
         }
      }

      try {
         if(this.input instanceof CloseableIterator) {
            ((CloseableIterator)this.input).close();
         }
      } catch (Throwable var3) {
         fail = Throwables.merge(fail, var3);
      }

      if(this.moreContents.length > 0) {
         fail = Throwables.perform(fail, Arrays.stream(this.moreContents).map((m) -> {
            return () -> {
               m.close();
            };
         }));
      }

      Throwables.maybeFail(fail);
   }

   public final O next() {
      if(this.next == null && !this.hasNext()) {
         throw new NoSuchElementException();
      } else {
         O next = (O)this.next;
         this.next = null;
         return next;
      }
   }

   protected final boolean hasMoreContents() {
      return this.moreContents.length > 0 && this.tryGetMoreContents();
   }

   private boolean tryGetMoreContents() {
      for(int i = 0; i < this.moreContents.length; ++i) {
         Stack.MoreContentsHolder holder = this.moreContents[i];
         MoreContents provider = holder.moreContents;
         I newContents = (I)provider.moreContents();
         if(newContents != null) {
            if(this.input instanceof CloseableIterator) {
               ((CloseableIterator)this.input).close();
            }

            this.input = newContents;
            Stack prefix = EMPTY;
            if(newContents instanceof BaseIterator) {
               BaseIterator abstr = (BaseIterator)newContents;
               prefix = abstr;
               this.input = (I)abstr.input;
               this.next = this.apply((V)abstr.next, holder.length);
            }

            Throwables.maybeFail(this.runOnClose(holder.length));
            this.refill((Stack)prefix, holder, i);
            if(this.next != null || this.input.hasNext()) {
               return true;
            }

            i = -1;
         }
      }

      return false;
   }

   private V apply(V next, int from) {
      while(next != null & from < this.length) {
         next = this.applyOne(next, this.stack[from++]);
      }

      return next;
   }
}
