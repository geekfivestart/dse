package org.apache.cassandra.utils.btree;

import com.google.common.base.Function;
import java.util.function.BiFunction;

public interface UpdateFunction<K, V> extends Function<K, V> {
   UpdateFunction.Simple<Object> noOp = UpdateFunction.Simple.of((a, b) -> {
      return a;
   });

   V apply(V var1, K var2);

   boolean abortEarly();

   void allocated(long var1);

   static default <K> UpdateFunction<K, K> noOp() {
      return noOp;
   }

   public static final class Simple<V> implements UpdateFunction<V, V> {
      private final BiFunction<V, V, V> wrapped;

      public Simple(BiFunction<V, V, V> wrapped) {
         this.wrapped = wrapped;
      }

      public V apply(V v) {
         return v;
      }

      public V apply(V replacing, V update) {
         return this.wrapped.apply(replacing, update);
      }

      public boolean abortEarly() {
         return false;
      }

      public void allocated(long heapSize) {
      }

      public static <V> UpdateFunction.Simple<V> of(BiFunction<V, V, V> f) {
         return new UpdateFunction.Simple(f);
      }
   }
}
