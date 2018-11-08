package org.apache.cassandra.io.util;

class SpinningDiskOptimizationStrategy extends DiskOptimizationStrategy {
   SpinningDiskOptimizationStrategy(int minBufferSize, int maxBufferSize) {
      super(minBufferSize, maxBufferSize);
   }

   public String diskType() {
      return "Spinning disks (non-SSD)";
   }

   public int bufferSize(long recordSize) {
      return this.roundBufferSize(recordSize + (long)this.minBufferSize);
   }

   public int readAheadSizeKb() {
      return 64;
   }
}
