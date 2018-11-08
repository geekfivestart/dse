package org.apache.cassandra.metrics;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HintedHandoffMetrics {
   private static final Logger logger = LoggerFactory.getLogger(HintedHandoffMetrics.class);
   private static final MetricNameFactory factory = new DefaultNameFactory("HintedHandOffManager");
   private final LoadingCache<InetAddress, HintedHandoffMetrics.DifferencingCounter> notStored = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).build(HintedHandoffMetrics.DifferencingCounter::<init>);
   private final LoadingCache<InetAddress, Counter> createdHintCounts = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).build((address) -> {
      return CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("Hints_created-" + address.getHostAddress().replace(':', '.')));
   });

   public HintedHandoffMetrics() {
   }

   public void incrCreatedHints(InetAddress address) {
      ((Counter)this.createdHintCounts.get(address)).inc();
   }

   public void incrPastWindow(InetAddress address) {
      ((HintedHandoffMetrics.DifferencingCounter)this.notStored.get(address)).mark();
   }

   public void log() {
      Iterator var1 = this.notStored.asMap().entrySet().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, HintedHandoffMetrics.DifferencingCounter> entry = (Entry)var1.next();
         long difference = ((HintedHandoffMetrics.DifferencingCounter)entry.getValue()).difference();
         if(difference != 0L) {
            logger.warn("{} has {} dropped hints, because node is down past configured hint window.", entry.getKey(), Long.valueOf(difference));
            TPCUtils.blockingAwait(SystemKeyspace.updateHintsDropped((InetAddress)entry.getKey(), UUIDGen.getTimeUUID(), (int)difference));
         }
      }

   }

   public static class DifferencingCounter {
      private final Counter meter;
      private long reported = 0L;

      public DifferencingCounter(InetAddress address) {
         this.meter = CassandraMetricsRegistry.Metrics.counter(HintedHandoffMetrics.factory.createMetricName("Hints_not_stored-" + address.getHostAddress().replace(':', '.')));
      }

      public long difference() {
         long current = this.meter.getCount();
         long difference = current - this.reported;
         this.reported = current;
         return difference;
      }

      public long count() {
         return this.meter.getCount();
      }

      public void mark() {
         this.meter.inc();
      }
   }
}
