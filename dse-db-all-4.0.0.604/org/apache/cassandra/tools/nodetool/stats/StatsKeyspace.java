package org.apache.cassandra.tools.nodetool.stats;

import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;

public class StatsKeyspace {
   public List<StatsTable> tables = new ArrayList();
   private final NodeProbe probe;
   public String name;
   public long readCount;
   public long writeCount;
   public int pendingFlushes;
   private double totalReadTime;
   private double totalWriteTime;

   public StatsKeyspace(NodeProbe probe, String keyspaceName) {
      this.probe = probe;
      this.name = keyspaceName;
   }

   public void add(ColumnFamilyStoreMBean table) {
      String tableName = table.getTableName();
      long tableWriteCount = ((CassandraMetricsRegistry.JmxTimerMBean)this.probe.getColumnFamilyMetric(this.name, tableName, "WriteLatency")).getCount();
      long tableReadCount = ((CassandraMetricsRegistry.JmxTimerMBean)this.probe.getColumnFamilyMetric(this.name, tableName, "ReadLatency")).getCount();
      if(tableReadCount > 0L) {
         this.readCount += tableReadCount;
         this.totalReadTime += (double)((Long)this.probe.getColumnFamilyMetric(this.name, tableName, "ReadTotalLatency")).longValue();
      }

      if(tableWriteCount > 0L) {
         this.writeCount += tableWriteCount;
         this.totalWriteTime += (double)((Long)this.probe.getColumnFamilyMetric(this.name, tableName, "WriteTotalLatency")).longValue();
      }

      this.pendingFlushes = (int)((long)this.pendingFlushes + ((Long)this.probe.getColumnFamilyMetric(this.name, tableName, "PendingFlushes")).longValue());
   }

   public double readLatency() {
      return this.readCount > 0L?this.totalReadTime / (double)this.readCount / 1000.0D:0.0D / 0.0;
   }

   public double writeLatency() {
      return this.writeCount > 0L?this.totalWriteTime / (double)this.writeCount / 1000.0D:0.0D / 0.0;
   }
}
