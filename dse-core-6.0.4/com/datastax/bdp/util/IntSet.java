package com.datastax.bdp.util;

public final class IntSet implements Cloneable {
   public static final int NULL = -2147483648;
   public static final int DELETED = -2147483647;
   private int size = 0;
   private int mask;
   private int[] table;

   public IntSet(int capacity) {
      int length = Integer.highestOneBit(capacity) << 2;

      for(this.mask = 1; this.mask < length - 1; this.mask = this.mask << 1 | 1) {
         ;
      }

      this.table = new int[length];

      for(int i = 0; i < this.table.length; ++i) {
         this.table[i] = -2147483648;
      }

   }

   public IntSet clone() {
      try {
         IntSet copy = (IntSet)super.clone();
         copy.table = (int[])this.table.clone();
         return copy;
      } catch (CloneNotSupportedException var2) {
         throw new RuntimeException(var2);
      }
   }

   public void add(int element) {
      assert element != -2147483648 : "Cannot add NULL element to IntSet";

      assert element != -2147483647 : "Cannot add DELETED element to IntSet";

      int pos = this.find(element);
      if(this.table[pos] == -2147483648) {
         ++this.size;
      }

      this.table[pos] = element;
      if(this.size > this.table.length >> 1) {
         this.grow();
      }

   }

   private void grow() {
      throw new UnsupportedOperationException("Dynamic resizing of IntSet is not supported yet. Please reserve enough initial capacity.");
   }

   public void remove(int element) {
      int pos = this.find(element);
      if(this.table[pos] == element) {
         --this.size;
      }

      this.table[pos] = -2147483648;
      this.rehash(pos + 1 & this.mask);
   }

   public void removeFast(int element) {
      int pos = this.find(element);
      if(this.table[pos] == element) {
         --this.size;
      }

      this.table[pos] = -2147483647;
   }

   private void rehash(int pos) {
      for(; this.table[pos] != -2147483648; pos = pos + 1 & this.mask) {
         if((this.table[pos] & this.mask) != pos) {
            int element = this.table[pos];
            int newPos = this.find(element);
            this.table[pos] = -2147483648;
            this.table[newPos] = element;
         }
      }

   }

   public boolean contains(int element) {
      int pos = this.find(element);
      return this.table[pos] == element;
   }

   public int size() {
      return this.size;
   }

   public boolean isEmpty() {
      return this.size == 0;
   }

   public boolean isValid(int element) {
      return element > -2147483647;
   }

   private int find(int element) {
      int pos;
      for(pos = this.hash(element); this.table[pos] != -2147483648 && this.table[pos] != element; pos = pos + 1 & this.mask) {
         ;
      }

      return pos;
   }

   private int hash(int element) {
      return element & this.mask;
   }

   public int[] rawArray() {
      return this.table;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      boolean first = true;
      int[] var3 = this.table;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         int value = var3[var5];
         if(value != -2147483648) {
            if(!first) {
               sb.append(", ");
            }

            sb.append(value);
            first = false;
         }
      }

      sb.append("}");
      return sb.toString();
   }

   public IntSet.Iterator iterator() {
      return new IntSet.Iterator();
   }

   public class Iterator {
      private int pos = -1;

      public Iterator() {
      }

      public int next() {
         do {
            ++this.pos;
            if(this.pos == IntSet.this.table.length) {
               return -2147483648;
            }
         } while(IntSet.this.table[this.pos] == -2147483648);

         return IntSet.this.table[this.pos];
      }
   }
}
