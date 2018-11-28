package com.datastax.bdp.util;

import java.util.BitSet;

public final class IntSetArray {
   private final int maxValue;
   private final int maxSetSize;
   private final int numberOfSets;
   private final IntSetArray.SetStorage storage;

   public IntSetArray(int numberOfSets, int maxSetSize, int maxValue) {
      this.maxValue = maxValue;
      this.maxSetSize = maxSetSize;
      this.numberOfSets = numberOfSets;
      this.storage = this.initStorage(maxSetSize, maxValue);
   }

   private IntSetArray.SetStorage initStorage(int maxSetSize, int maxValue) {
      int sizeOfBitSet = (maxValue + 7) / 8;
      int sizeOfArraySetEntry = maxValue <= 255?1:(maxValue <= '\uffff'?2:4);
      int sizeOfArraySet = sizeOfArraySetEntry * maxSetSize;
      Object storage;
      if(sizeOfBitSet < sizeOfArraySet) {
         storage = new IntSetArray.BitSetStorage();
      } else {
         switch(sizeOfArraySetEntry) {
         case 1:
            storage = new IntSetArray.ByteArrayStorage();
            break;
         case 2:
            storage = new IntSetArray.ShortArrayStorage();
            break;
         case 3:
         default:
            throw new UnsupportedOperationException("Unsupported size of array entry: " + sizeOfArraySetEntry);
         case 4:
            storage = new IntSetArray.IntArrayStorage();
         }
      }

      return (IntSetArray.SetStorage)storage;
   }

   public void add(int index, int value) {
      assert index >= 0 : "IntSetArray index must not be negative: " + index;

      assert index < this.numberOfSets : "IntSetArray index " + index + " out of bounds. Must be < " + this.numberOfSets;

      assert value >= 0 : "IntSetArray does not support storing negative numbers: " + value;

      assert value <= this.maxValue : "Value too big: " + value + ". Must be <= " + this.maxValue;

      this.storage.add(index, value);
   }

   public int get(int index, int[] target) {
      assert index >= 0 : "IntSetArray index cannot be less than zero: " + index;

      assert index < this.numberOfSets : "IntSetArray " + index + " out of bounds. Must be < " + this.numberOfSets;

      assert target.length >= this.maxSetSize : "Target array too small: " + target.length + ". Required length: " + this.maxSetSize;

      return this.storage.get(index, target);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      int[] temp = new int[this.maxSetSize];

      for(int i = 0; i < this.numberOfSets; ++i) {
         sb.append("{");
         int count = this.get(i, temp);

         for(int j = 0; j < count; ++j) {
            if(j > 0) {
               sb.append(", ");
            }

            sb.append(temp[i]);
         }

         sb.append("}");
      }

      sb.append("]");
      return sb.toString();
   }

   private final class IntArrayStorage extends IntSetArray.ArrayStorage {
      private final int[] array;

      private IntArrayStorage() {
         super();
         this.array = new int[IntSetArray.this.numberOfSets * IntSetArray.this.maxSetSize];
         this.initArray();
      }

      protected int getArrayEntry(int index) {
         return this.array[index] + 2147483647;
      }

      protected void setArrayEntry(int index, int value) {
         this.array[index] = (short)(value - 2147483647);
      }
   }

   private final class ShortArrayStorage extends IntSetArray.ArrayStorage {
      private final short[] array;

      private ShortArrayStorage() {
         super();
         this.array = new short[IntSetArray.this.numberOfSets * IntSetArray.this.maxSetSize];
         this.initArray();
      }

      protected int getArrayEntry(int index) {
         return this.array[index] + 32767;
      }

      protected void setArrayEntry(int index, int value) {
         this.array[index] = (short)(value - 32767);
      }
   }

   private final class ByteArrayStorage extends IntSetArray.ArrayStorage {
      private final byte[] array;

      private ByteArrayStorage() {
         super();
         this.array = new byte[IntSetArray.this.numberOfSets * IntSetArray.this.maxSetSize];
         this.initArray();
      }

      protected int getArrayEntry(int index) {
         return this.array[index] + 127;
      }

      protected void setArrayEntry(int index, int value) {
         this.array[index] = (byte)(value - 127);
      }
   }

   private abstract class ArrayStorage implements IntSetArray.SetStorage {
      private ArrayStorage() {
      }

      protected abstract int getArrayEntry(int var1);

      protected abstract void setArrayEntry(int var1, int var2);

      protected void initArray() {
         for(int i = 0; i < IntSetArray.this.numberOfSets * IntSetArray.this.maxSetSize; ++i) {
            this.setArrayEntry(i, -1);
         }

      }

      public void add(int index, int value) {
         for(int i = index * IntSetArray.this.maxSetSize; i < (index + 1) * IntSetArray.this.maxSetSize; ++i) {
            int entry = this.getArrayEntry(i);
            if(entry < 0) {
               this.setArrayEntry(i, value);
               return;
            }

            if(entry == value) {
               return;
            }
         }

         throw new IllegalStateException("Set " + index + " is full. Cannot add more elements.");
      }

      public int get(int index, int[] target) {
         int i = 0;

         for(int j = index * IntSetArray.this.maxSetSize; i < IntSetArray.this.maxSetSize; ++j) {
            int entry = this.getArrayEntry(j);
            target[i] = entry;
            if(entry < 0) {
               break;
            }

            ++i;
         }

         return i;
      }
   }

   private final class BitSetStorage implements IntSetArray.SetStorage {
      private final int bitsPerBitSet;
      private final BitSet bitSet;

      private BitSetStorage() {
         this.bitsPerBitSet = IntSetArray.this.maxValue;
         this.bitSet = new BitSet(IntSetArray.this.numberOfSets * this.bitsPerBitSet);
      }

      public void add(int index, int value) {
         this.bitSet.set(index * this.bitsPerBitSet + value);
      }

      public int get(int index, int[] target) {
         int firstBit = index * this.bitsPerBitSet;
         int lastBit = firstBit + this.bitsPerBitSet;
         int pos = 0;

         for(int bit = this.bitSet.nextSetBit(firstBit); bit >= 0 && bit < lastBit && pos < target.length; bit = this.bitSet.nextSetBit(bit + 1)) {
            target[pos++] = bit - firstBit;
         }

         return pos;
      }
   }

   private interface SetStorage {
      void add(int var1, int var2);

      int get(int var1, int[] var2);
   }
}
