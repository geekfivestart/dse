package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.utils.memory.BufferPool;

public class BufferPoolMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("BufferPool");
   public final Meter misses;
   public final Gauge<Long> totalSize;
   public final Gauge<Long> overflowSize;
   public final Gauge<Long> usedSize;
   public final Gauge<Long> chunkReaderBufferSize;

   public BufferPoolMetrics() {
      this.misses = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Misses"));
      this.totalSize = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Size"), BufferPool::sizeInBytes);
      this.usedSize = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("UsedSize"), BufferPool::usedSizeInBytes);
      this.overflowSize = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OverflowSize"), BufferPool::sizeInBytesOverLimit);
      CassandraMetricsRegistry var10001 = CassandraMetricsRegistry.Metrics;
      CassandraMetricsRegistry.MetricName var10002 = factory.createMetricName("ChunkReaderBufferSize");
      AtomicLong var10003 = ChunkReader.bufferSize;
      ChunkReader.bufferSize.getClass();
      this.chunkReaderBufferSize = (Gauge)var10001.register(var10002, var10003::get);
   }
}
