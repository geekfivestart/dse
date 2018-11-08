package org.apache.cassandra.utils;

public class Wrapped<T> {
   private T value;

   public static <V> Wrapped<V> create(V initial) {
      return new Wrapped(initial);
   }

   private Wrapped(T initial) {
      this.value = initial;
   }

   public T get() {
      return this.value;
   }

   public void set(T value) {
      this.value = value;
   }
}
