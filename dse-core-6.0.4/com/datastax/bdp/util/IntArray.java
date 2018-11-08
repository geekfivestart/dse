package com.datastax.bdp.util;

import java.util.Arrays;

public class IntArray {
   private int[] array;
   private int size = 0;

   public IntArray(int capacity) {
      this.array = new int[capacity];
   }

   public int size() {
      return this.size;
   }

   public void add(int value) {
      if(this.size >= this.array.length) {
         int[] newArray = new int[this.array.length * 2];
         System.arraycopy(this.array, 0, newArray, 0, this.array.length);
         this.array = newArray;
      }

      this.array[this.size++] = value;
   }

   public void copyTo(IntSet set) {
      for(int i = 0; i < this.size; ++i) {
         set.add(this.array[i]);
      }

   }

   public void clear() {
      this.size = 0;
   }

   public String toString() {
      return Arrays.toString(Arrays.copyOfRange(this.array, 0, this.size));
   }
}
