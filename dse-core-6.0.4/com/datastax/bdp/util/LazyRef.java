package com.datastax.bdp.util;

import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LazyRef<T> implements Supplier<T> {
   final Supplier<T> delegate;
   transient volatile boolean initialized;
   transient volatile T value;

   public static <T> LazyRef<T> of(Supplier<T> delegate) {
      Preconditions.checkNotNull(delegate);
      return new LazyRef(delegate);
   }

   private LazyRef(Supplier<T> delegate) {
      this.delegate = delegate;
   }

   public void doIfInitialized(Consumer<T> consumer) {
      if(this.initialized) {
         consumer.accept(this.value);
      } else {
         synchronized(this) {
            if(this.initialized) {
               consumer.accept(this.value);
            }
         }
      }

   }

   public T get() {
      if(!this.initialized) {
         synchronized(this) {
            if(!this.initialized) {
               T t = this.delegate.get();
               this.value = t;
               this.initialized = true;
               return t;
            }
         }
      }

      return this.value;
   }

   public String toString() {
      return this.initialized?"Uninitialized LazyRef":this.value.toString();
   }
}
