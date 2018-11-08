package org.apache.cassandra.gms;

class ArrayBackedBoundedStats {
   private final long[] arrivalIntervals;
   private long sum = 0L;
   private int index = 0;
   private boolean isFilled = false;
   private volatile double mean = 0.0D;

   public ArrayBackedBoundedStats(int size) {
      this.arrivalIntervals = new long[size];
   }

   public void add(long interval) {
      if(this.index == this.arrivalIntervals.length) {
         this.isFilled = true;
         this.index = 0;
      }

      if(this.isFilled) {
         this.sum -= this.arrivalIntervals[this.index];
      }

      this.arrivalIntervals[this.index++] = interval;
      this.sum += interval;
      this.mean = (double)this.sum / (double)this.size();
   }

   private int size() {
      return this.isFilled?this.arrivalIntervals.length:this.index;
   }

   public double mean() {
      return this.mean;
   }

   public long[] getArrivalIntervals() {
      return this.arrivalIntervals;
   }
}
