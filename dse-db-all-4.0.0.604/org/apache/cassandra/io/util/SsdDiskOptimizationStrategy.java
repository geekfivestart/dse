package org.apache.cassandra.io.util;

class SsdDiskOptimizationStrategy extends DiskOptimizationStrategy {
   private final double diskOptimizationPageCrossChance;

   SsdDiskOptimizationStrategy(int minBufferSize, int maxBufferSize, double diskOptimizationPageCrossChance) {
      super(minBufferSize, maxBufferSize);
      this.diskOptimizationPageCrossChance = diskOptimizationPageCrossChance;
   }

   public String diskType() {
      return "Solid-State drives (SSD)";
   }

   public int bufferSize(long recordSize) {
      double pageCrossProbability = (double)(recordSize % (long)this.minBufferSize) / (double)this.minBufferSize;
      if(pageCrossProbability - this.diskOptimizationPageCrossChance > -1.0E-16D) {
         recordSize += (long)this.minBufferSize;
      }

      return this.roundBufferSize(recordSize);
   }

   public int readAheadSizeKb() {
      return 32;
   }
}
