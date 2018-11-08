package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import org.apache.cassandra.cql3.QueryProcessor;

public class CQLMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("CQL");
   public final Counter regularStatementsExecuted;
   public final Counter preparedStatementsExecuted;
   public final Counter preparedStatementsEvicted;
   public final Gauge<Integer> preparedStatementsCount;
   public final Gauge<Double> preparedStatementsRatio;

   public CQLMetrics() {
      this.regularStatementsExecuted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("RegularStatementsExecuted"));
      this.preparedStatementsExecuted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("PreparedStatementsExecuted"));
      this.preparedStatementsEvicted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("PreparedStatementsEvicted"));
      this.preparedStatementsCount = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("PreparedStatementsCount"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(QueryProcessor.preparedStatementsCount());
         }
      });
      this.preparedStatementsRatio = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("PreparedStatementsRatio"), new RatioGauge() {
         public Ratio getRatio() {
            return Ratio.of(this.getNumerator(), this.getDenominator());
         }

         public double getNumerator() {
            return (double)CQLMetrics.this.preparedStatementsExecuted.getCount();
         }

         public double getDenominator() {
            return (double)(CQLMetrics.this.regularStatementsExecuted.getCount() + CQLMetrics.this.preparedStatementsExecuted.getCount());
         }
      });
   }
}
