package org.apache.cassandra.utils.concurrent;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.UnsafeAccess;

public class LongAdder {
   private static final int FALSE_SHARING_PAD = 128;
   private static final int COUNTER_OFFSET_SCALE = Integer.numberOfTrailingZeros(128);
   private static final int SIZEOF_LONG = 8;
   private static final long COUNTER_ARRAY_BASE;
   private final int numCores = TPC.getNumCores();
   private final long[] values;

   public LongAdder() {
      this.values = new long[(this.numCores + 2) * 16];
   }

   public void add(long x) {
      int coreId = TPC.getCoreId();
      long offset = this.counterOffset(coreId);
      if(coreId < this.numCores) {
         long value = UnsafeAccess.UNSAFE.getLong(this.values, offset);
         UnsafeAccess.UNSAFE.putOrderedLong(this.values, offset, value + x);
      } else {
         UnsafeAccess.UNSAFE.getAndAddLong(this.values, offset, x);
      }

   }

   private long counterOffset(int coreId) {
      return COUNTER_ARRAY_BASE + (long)(coreId << COUNTER_OFFSET_SCALE);
   }

   public void increment() {
      this.add(1L);
   }

   public void decrement() {
      this.add(-1L);
   }

   public long sum() {
      long sum = 0L;
      long[] values = this.values;

      for(int i = 0; i <= this.numCores; ++i) {
         sum += UnsafeAccess.UNSAFE.getLongVolatile(values, this.counterOffset(i));
      }

      return sum;
   }

   public void reset() {
      long[] values = this.values;

      for(int i = 0; i <= this.numCores; ++i) {
         UnsafeAccess.UNSAFE.getAndSetLong(values, this.counterOffset(i), 0L);
      }

   }

   public String toString() {
      return this.values == null?"0":Long.toString(this.sum());
   }

   public long longValue() {
      return this.sum();
   }

   public int intValue() {
      return (int)this.sum();
   }

   public float floatValue() {
      return (float)this.sum();
   }

   public double doubleValue() {
      return (double)this.sum();
   }

   static {
      try {
         COUNTER_ARRAY_BASE = (long)(UnsafeAccess.UNSAFE.arrayBaseOffset(long[].class) + 128);
      } catch (Exception var1) {
         throw new AssertionError(var1);
      }
   }
}
