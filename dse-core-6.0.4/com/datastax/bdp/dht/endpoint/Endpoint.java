package com.datastax.bdp.dht.endpoint;

import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.util.MapBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.Meter;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

public class Endpoint {
   private final InetAddress address;
   private final Collection<Range<Token>> providedTokenRanges;
   private Collection<Range<Token>> usedTokenRanges;
   private Meter loadRate;

   public Endpoint(InetAddress address, Collection<Range<Token>> providedTokenRanges) {
      this.usedTokenRanges = null;
      this.loadRate = null;

      assert address != null;

      assert providedTokenRanges != null;

      this.address = address;
      this.providedTokenRanges = providedTokenRanges;
   }

   public Endpoint(Endpoint subject) {
      this(subject.address, subject.providedTokenRanges);
   }

   public void setUsedTokenRanges(Collection<Range<Token>> usedTokenRanges) {
      this.usedTokenRanges = usedTokenRanges;
   }

   public void initLoadRate(CassandraMetricsRegistry metrics) {
      if(this.loadRate == null) {
         List<String> keys = ImmutableList.of("scope", "metricType", "endpoint", "name");
         List<String> values = ImmutableList.of("core", "EndpointMetrics", this.address.toString(), "LoadRate");
         String mBeanName = JMX.buildMBeanName(JMX.Type.METRICS, MapBuilder.immutable().withKeys(keys.toArray(new String[keys.size()])).withValues(values.toArray(new String[values.size()])).build());
         MetricName name = new MetricName("com.datastax.bdp", "core", "EndpointMetrics", this.address.toString(), mBeanName);
         if(metrics.getMetrics().containsKey(name.getMetricName())) {
            metrics.remove(name);
         }

         this.loadRate = metrics.meter(name);
      }

   }

   public void markLoadRate() {
      if(this.loadRate != null) {
         this.loadRate.mark();
      }

   }

   public Collection<Range<Token>> getUsedTokenRanges() {
      return this.usedTokenRanges;
   }

   public Collection<Range<Token>> getProvidedTokenRanges() {
      return this.providedTokenRanges;
   }

   public InetAddress getAddress() {
      return this.address;
   }

   public double[] getLoadRate() {
      double[] result = new double[3];
      if(this.loadRate != null) {
         result[0] = this.loadRate.getOneMinuteRate();
         result[1] = this.loadRate.getFiveMinuteRate();
         result[2] = this.loadRate.getFifteenMinuteRate();
      }

      return result;
   }

   public String toString() {
      return "Endpoint{address=" + this.address + ", providedTokenRanges=" + this.providedTokenRanges + ", usedTokenRanges=" + this.usedTokenRanges + "}";
   }

   public static final class GetProvidedRanges implements Function<Endpoint, Collection<Range<Token>>> {
      public GetProvidedRanges() {
      }

      public Collection<Range<Token>> apply(Endpoint endpoint) {
         return endpoint.getProvidedTokenRanges();
      }
   }
}
