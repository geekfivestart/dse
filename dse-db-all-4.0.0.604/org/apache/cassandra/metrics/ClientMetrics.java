package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.Callable;

public class ClientMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("Client");
   public static final ClientMetrics instance = new ClientMetrics();

   private ClientMetrics() {
   }


   public void addCounter(final String name, final Callable<Integer> provider) {
      CassandraMetricsRegistry.Metrics.register(ClientMetrics.factory.createMetricName(name), (Gauge<Integer>)(() -> {
         try {
            return provider.call();
         }
         catch (Exception e) {
            throw new RuntimeException(e);
         }
      }));
   }

   public Meter addMeter(String name) {
      return CassandraMetricsRegistry.Metrics.meter(factory.createMetricName(name));
   }
}
