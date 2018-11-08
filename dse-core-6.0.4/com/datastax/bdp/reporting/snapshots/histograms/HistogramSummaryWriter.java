package com.datastax.bdp.reporting.snapshots.histograms;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.util.SchemaTool;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.EstimatedHistogram;

public class HistogramSummaryWriter extends CqlWriter<HistogramInfo> {
   private final String tableName;

   public HistogramSummaryWriter(String tableName, InetAddress nodeAddress, int ttlSeconds) {
      super(nodeAddress, ttlSeconds);
      this.tableName = tableName;
   }

   protected String getTableName() {
      return this.tableName;
   }

   public void maybeAlterSchema() {
      SchemaTool.maybeAddNewColumn("dse_perf", this.tableName, "dropped_mutations", String.format("ALTER TABLE %s.%s ADD dropped_mutations bigint", new Object[]{"dse_perf", this.tableName}));
   }

   protected String getInsertCQL() {
      String stmt = "INSERT INTO %s.%s (node_ip, keyspace_name, table_name, histogram_id, p50, p75, p90, p95, p98, p99, min, max, dropped_mutations)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL ?";
      return String.format(stmt, new Object[]{"dse_perf", this.tableName, PerformanceObjectsPlugin.getAdditionalTableOptions()});
   }

   private String getInsertCQL_5_0() {
      String stmt = "INSERT INTO %s.%s (node_ip, keyspace_name, table_name, histogram_id, p50, p75, p90, p95, p98, p99, min, max )VALUES (?,?,?,?,?,?,?,?,?,?,?,?) USING TTL ?";
      return String.format(stmt, new Object[]{"dse_perf", this.tableName, PerformanceObjectsPlugin.getAdditionalTableOptions()});
   }

   protected CqlWriter<HistogramInfo>.WriterConfig createWriterConfig(CassandraVersion dseVersion) {
      return dseVersion.compareTo(DseVersionNotifier.VERSION_51) >= 0?new CqlWriter.WriterConfig(this.getInsertCQL(), this::getVariables):new CqlWriter.WriterConfig(this.getInsertCQL_5_0(), this::getVariables_5_0);
   }

   protected List<ByteBuffer> getVariables(HistogramInfo writeable) {
      ByteBuffer timestamp = TimestampType.instance.decompose(new Date(System.currentTimeMillis()));
      EstimatedHistogram histogram = writeable.getHistogram();
      return Arrays.asList(new ByteBuffer[]{this.nodeAddressBytes, ByteBufferUtil.bytes(writeable.keyspace), ByteBufferUtil.bytes(writeable.table), timestamp, ByteBufferUtil.bytes(histogram.percentile(0.5D)), ByteBufferUtil.bytes(histogram.percentile(0.75D)), ByteBufferUtil.bytes(histogram.percentile(0.9D)), ByteBufferUtil.bytes(histogram.percentile(0.95D)), ByteBufferUtil.bytes(histogram.percentile(0.98D)), ByteBufferUtil.bytes(histogram.percentile(0.99D)), ByteBufferUtil.bytes(histogram.min()), ByteBufferUtil.bytes(histogram.max()), ByteBufferUtil.bytes(writeable.droppedMutations), this.getTtlBytes()});
   }

   private List<ByteBuffer> getVariables_5_0(HistogramInfo writeable) {
      ByteBuffer timestamp = TimestampType.instance.decompose(new Date(System.currentTimeMillis()));
      EstimatedHistogram histogram = writeable.getHistogram();
      return Arrays.asList(new ByteBuffer[]{this.nodeAddressBytes, ByteBufferUtil.bytes(writeable.keyspace), ByteBufferUtil.bytes(writeable.table), timestamp, ByteBufferUtil.bytes(histogram.percentile(0.5D)), ByteBufferUtil.bytes(histogram.percentile(0.75D)), ByteBufferUtil.bytes(histogram.percentile(0.9D)), ByteBufferUtil.bytes(histogram.percentile(0.95D)), ByteBufferUtil.bytes(histogram.percentile(0.98D)), ByteBufferUtil.bytes(histogram.percentile(0.99D)), ByteBufferUtil.bytes(histogram.min()), ByteBufferUtil.bytes(histogram.max()), this.getTtlBytes()});
   }
}
