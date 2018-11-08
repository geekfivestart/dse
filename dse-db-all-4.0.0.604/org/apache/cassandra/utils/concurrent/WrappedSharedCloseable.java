package org.apache.cassandra.utils.concurrent;

import java.util.Arrays;
import org.apache.cassandra.utils.Throwables;

public abstract class WrappedSharedCloseable extends SharedCloseableImpl {
   final AutoCloseable[] wrapped;

   public WrappedSharedCloseable(AutoCloseable closeable) {
      this(new AutoCloseable[]{closeable});
   }

   public WrappedSharedCloseable(AutoCloseable[] closeable) {
      super((RefCounted.Tidy)(new WrappedSharedCloseable.Tidy(closeable)));
      this.wrapped = closeable;
   }

   protected WrappedSharedCloseable(WrappedSharedCloseable copy) {
      super((SharedCloseableImpl)copy);
      this.wrapped = copy.wrapped;
   }

   static final class Tidy implements RefCounted.Tidy {
      final AutoCloseable[] closeable;

      Tidy(AutoCloseable[] closeable) {
         this.closeable = closeable;
      }

      public void tidy() throws Exception {
         Throwable fail = null;
         AutoCloseable[] var2 = this.closeable;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            AutoCloseable c = var2[var4];

            try {
               c.close();
            } catch (Throwable var7) {
               fail = Throwables.merge(fail, var7);
            }
         }

         Throwables.maybeFail(fail);
      }

      public String name() {
         return Arrays.toString(this.closeable);
      }
   }
}
