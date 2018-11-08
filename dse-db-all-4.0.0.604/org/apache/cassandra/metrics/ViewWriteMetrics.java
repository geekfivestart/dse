package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

public class ViewWriteMetrics extends ClientRequestMetrics {
   public final Counter viewReplicasAttempted;
   public final Counter viewReplicasSuccess;
   public final Timer viewWriteLatency;

   public ViewWriteMetrics(String scope) {
      super(scope);
      this.viewReplicasAttempted = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("ViewReplicasAttempted"));
      this.viewReplicasSuccess = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("ViewReplicasSuccess"));
      this.viewWriteLatency = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("ViewWriteLatency"));
      CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("ViewPendingMutations"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(ViewWriteMetrics.this.viewReplicasAttempted.getCount() - ViewWriteMetrics.this.viewReplicasSuccess.getCount());
         }
      });
   }

   public void release() {
      super.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ViewReplicasAttempted"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ViewReplicasSuccess"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ViewWriteLatency"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ViewPendingMutations"));
   }
}
