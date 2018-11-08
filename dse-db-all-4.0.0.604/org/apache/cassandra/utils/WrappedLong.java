package org.apache.cassandra.utils;

public class WrappedLong {
   private long value;

   public WrappedLong(long initial) {
      this.value = initial;
   }

   public long get() {
      return this.value;
   }

   public void set(long value) {
      this.value = value;
   }

   public void increment() {
      ++this.value;
   }

   public void decrement() {
      --this.value;
   }

   public void add(long value) {
      this.value += value;
   }

   public void min(long l) {
      this.value = Math.min(l, this.value);
   }

   public void max(long l) {
      this.value = Math.max(l, this.value);
   }
}
