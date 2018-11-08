package org.apache.cassandra.cache;

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Function;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.CacheMissMetrics;

public class ChunkCache implements CacheSize {
   private static final double INDEX_TO_DATA_RATIO = Double.parseDouble(System.getProperty("dse.cache.index_to_data_ratio", "0.4"));
   private static final int INFLIGHT_DATA_OVERHEAD_IN_MB = Math.max(Integer.getInteger("dse.cache.inflight_data_overhead_in_mb", (int)((double)DatabaseDescriptor.getFileCacheSizeInMB() * 0.05D)).intValue(), 32);
   private static final long cacheSize;
   private static final boolean roundUp;
   private static final DiskOptimizationStrategy diskOptimizationStrategy;
   public static final ChunkCache instance;
   private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;
   public final CacheMissMetrics metrics = new CacheMissMetrics("ChunkCache", this);
   private final ChunkCacheImpl indexPageCache;
   private final ChunkCacheImpl dataPageCache;
   private final int INDEX_BLOCK_SIZE = 4096;

   public ChunkCache() {
      long indexCacheSize = (long)(INDEX_TO_DATA_RATIO * (double)cacheSize);
      this.indexPageCache = new ChunkCacheImpl(this.metrics, indexCacheSize);
      this.dataPageCache = new ChunkCacheImpl(this.metrics, cacheSize - indexCacheSize);
   }

   public RebuffererFactory wrap(ChunkReader file) {
      int size = file.chunkSize();
      return size == 4096?this.indexPageCache.wrap(file):this.dataPageCache.wrap(file);
   }

   public RebuffererFactory maybeWrap(ChunkReader file) {
      return (RebuffererFactory)this.wrapper.apply(file);
   }

   @VisibleForTesting
   public void enable(boolean enabled) {
      this.wrapper = enabled?this::wrap:(x) -> {
         return x;
      };
      this.indexPageCache.reset();
      this.dataPageCache.reset();
   }

   @VisibleForTesting
   public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor) {
      Function<ChunkReader, RebuffererFactory> prevWrapper = this.wrapper;
      this.wrapper = (rdr) -> {
         return (RebuffererFactory)interceptor.apply(prevWrapper.apply(rdr));
      };
   }

   public static int roundForCaching(int bufferSize) {
      return diskOptimizationStrategy.roundForCaching(bufferSize, roundUp);
   }

   @VisibleForTesting
   public void invalidateFile(String fileName) {
      this.indexPageCache.invalidateFile(fileName);
      this.dataPageCache.invalidateFile(fileName);
   }

   public static void invalidatePosition(FileHandle dfile, long position) {
      if(dfile.rebuffererFactory() instanceof ChunkCacheImpl.CachingRebufferer) {
         ((ChunkCacheImpl.CachingRebufferer)dfile.rebuffererFactory()).invalidate(position);
      }
   }

   public long capacity() {
      return this.indexPageCache.capacity() + this.dataPageCache.capacity();
   }

   public void setCapacity(long capacity) {
      throw new AssertionError("Can't set capacity of chunk cache");
   }

   public int size() {
      return this.indexPageCache.size() + this.dataPageCache.size();
   }

   public long weightedSize() {
      return this.indexPageCache.weightedSize() + this.dataPageCache.weightedSize();
   }

   static {
      cacheSize = 1048576L * (long)Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - INFLIGHT_DATA_OVERHEAD_IN_MB);
      roundUp = DatabaseDescriptor.getFileCacheRoundUp();
      diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
      instance = cacheSize > 0L?new ChunkCache():null;
   }
}
