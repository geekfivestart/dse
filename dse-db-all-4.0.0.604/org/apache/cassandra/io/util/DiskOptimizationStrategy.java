package org.apache.cassandra.io.util;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DiskOptimizationStrategy {
   private static final String MIN_BUFFER_SIZE_NAME = "dse.min_buffer_size";
   private static final String MAX_BUFFER_SIZE_NAME = "dse.max_buffer_size";
   private static final int MIN_BUFFER_SIZE = Integer.parseInt(System.getProperty("dse.min_buffer_size", "0"));
   private static final int MAX_BUFFER_SIZE = Integer.parseInt(System.getProperty("dse.max_buffer_size", "0"));
   private static final Logger logger = LoggerFactory.getLogger(DiskOptimizationStrategy.class);
   final int minBufferSize;
   final int minBufferSizeMask;
   final int maxBufferSize;

   DiskOptimizationStrategy(int minBufferSize, int maxBufferSize) {
      assert Integer.bitCount(minBufferSize) == 1 : String.format("%d must be a power of two", new Object[]{Integer.valueOf(minBufferSize)});

      assert Integer.bitCount(maxBufferSize) == 1 : String.format("%d must be a power of two", new Object[]{Integer.valueOf(maxBufferSize)});

      this.minBufferSize = minBufferSize;
      this.minBufferSizeMask = minBufferSize - 1;
      this.maxBufferSize = maxBufferSize;
      logger.info("Disk optimization strategy for {} using min buffer size of {} bytes and max buffer size of {} bytes", new Object[]{this.diskType(), Integer.valueOf(minBufferSize), Integer.valueOf(maxBufferSize)});
   }

   public static DiskOptimizationStrategy create(Config conf) {
      DiskOptimizationStrategy ret;
      int minBufferSize;
      int maxBufferSize;
      int n = minBufferSize = MIN_BUFFER_SIZE > 0 ? MIN_BUFFER_SIZE : 4096;
      if (Integer.bitCount(minBufferSize) != 1) {
         throw new ConfigurationException(String.format("Min buffer size must be a power of two, instead got %d", minBufferSize));
      }
      int n2 = maxBufferSize = MAX_BUFFER_SIZE > 0 ? MAX_BUFFER_SIZE : 65536;
      if (Integer.bitCount(maxBufferSize) != 1) {
         throw new ConfigurationException(String.format("Max buffer size must be a power of two, instead got %d", maxBufferSize));
      }
      if (minBufferSize > maxBufferSize) {
         throw new ConfigurationException(String.format("Max buffer size %d must be >= than min buffer size %d", maxBufferSize, minBufferSize));
      }
      if (maxBufferSize > 65536) {
         logger.warn("Buffers larger than 64k ({}) are currently not supported by the buffer pool. This will cause longer allocation times for each buffer read from disk, consider lowering -D{} but make sure it is still a power of two and >= -D{}.", new Object[]{maxBufferSize, MAX_BUFFER_SIZE_NAME, MIN_BUFFER_SIZE_NAME});
      }
      switch (conf.disk_optimization_strategy) {
         case ssd: {
            ret = new SsdDiskOptimizationStrategy(minBufferSize, maxBufferSize, conf.disk_optimization_page_cross_chance);
            break;
         }
         case spinning: {
            ret = new SpinningDiskOptimizationStrategy(minBufferSize, maxBufferSize);
            break;
         }
         default: {
            throw new ConfigurationException("Unknown disk optimization strategy: " + (Object)((Object)conf.disk_optimization_strategy));
         }
      }
      return ret;
   }

   public abstract String diskType();

   public abstract int bufferSize(long var1);

   int roundBufferSize(long size) {
      if(size <= (long)this.minBufferSize) {
         return this.minBufferSize;
      } else {
         size = size + (long)this.minBufferSizeMask & (long)(~this.minBufferSizeMask);
         return (int)Math.min(size, (long)this.maxBufferSize);
      }
   }

   public int roundForCaching(int size, boolean roundUp) {
      if(size <= 2) {
         return 2;
      } else {
         int ret = roundUp?1 << 32 - Integer.numberOfLeadingZeros(size - 1):Integer.highestOneBit(size);
         return Math.min(this.maxBufferSize, ret);
      }
   }

   public abstract int readAheadSizeKb();
}
