package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.Callable;

public class ClientMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("Client");
   public static final ClientMetrics instance = new ClientMetrics();

   private ClientMetrics() {
   }

   public void addCounter(String name, Callable<Integer> provider) {
      CassandraMetricsRegistry.Metrics.register(factory.createMetricName(name), () -> {
         try {
            return (Integer)provider.call();
         } catch (Exception var2) {
            throw new RuntimeException(var2);
         }
      });
   }

   public Meter addMeter(String name) {
      return CassandraMetricsRegistry.Metrics.meter(factory.createMetricName(name));
   }
}
