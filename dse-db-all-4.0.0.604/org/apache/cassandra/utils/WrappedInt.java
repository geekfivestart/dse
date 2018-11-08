package org.apache.cassandra.utils;

public class WrappedInt {
   private int value;

   public WrappedInt(int initial) {
      this.value = initial;
   }

   public int get() {
      return this.value;
   }

   public void set(int value) {
      this.value = value;
   }

   public void increment() {
      ++this.value;
   }

   public void decrement() {
      --this.value;
   }

   public void add(int value) {
      this.value += value;
   }
}
