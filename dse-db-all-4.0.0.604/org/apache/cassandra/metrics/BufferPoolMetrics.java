package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.utils.memory.BufferPool;


public class BufferPoolMetrics {
    private static final MetricNameFactory factory = new DefaultNameFactory("BufferPool");
    public final Meter misses = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Misses"));
    public final Gauge<Long> totalSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Size"), BufferPool::sizeInBytes);
    public final Gauge<Long> overflowSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OverflowSize"), BufferPool::sizeInBytesOverLimit);
    public final Gauge<Long> usedSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("UsedSize"), BufferPool::usedSizeInBytes);
    public final Gauge<Long> chunkReaderBufferSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("ChunkReaderBufferSize"), ChunkReader.bufferSize::get);
}
