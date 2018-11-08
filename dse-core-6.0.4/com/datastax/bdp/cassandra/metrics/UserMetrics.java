package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.tracing.ClientConnectionMetadata;
import com.datastax.bdp.concurrent.RoutableTask;
import com.datastax.bdp.concurrent.WorkPool;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.system.TimeSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyVetoException;
import java.beans.VetoableChangeListener;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class UserMetrics {
   private static final int DEFAULT_BACKPRESSURE_MAX_PAUSE = 10000;
   private static final Logger logger = LoggerFactory.getLogger(UserMetrics.class);
   private static final AtomicReference<NonBlockingHashMap<String, LatencyTracker>> activeTrackers = new AtomicReference(new NonBlockingHashMap());
   private TimeSource timeSource;
   @VisibleForTesting
   public WorkPool pool;
   private final PropertyChangeListener backPressureListener = new PropertyChangeListener() {
      public void propertyChange(PropertyChangeEvent evt) {
         UserMetrics.this.pool.setBackPressureThreshold(((Integer)evt.getNewValue()).intValue());
      }
   };
   private final PropertyChangeListener flushTimeoutListener = new PropertyChangeListener() {
      public void propertyChange(PropertyChangeEvent evt) {
         UserMetrics.this.pool.setFlushMaxTime(((Integer)evt.getNewValue()).intValue());
      }
   };
   private final VetoableChangeListener asyncWriterListener = new VetoableChangeListener() {
      public void vetoableChange(PropertyChangeEvent evt) throws PropertyVetoException {
         try {
            UserMetrics.this.pool.setConcurrency(((Integer)evt.getNewValue()).intValue());
         } catch (Exception var3) {
            throw new PropertyVetoException(var3.getMessage(), evt);
         }
      }
   };

   @Inject
   public UserMetrics(UserLatencyTrackingBean latencyBean, TimeSource timeSource) {
      this.timeSource = timeSource;
      this.pool = createPool(timeSource, Integer.getInteger("dse.user_metrics_max_concurrency", latencyBean.getAsyncWriters()).intValue(), latencyBean.getBackpressureThreshold(), latencyBean.getFlushTimeout());
      latencyBean.hook(this.backPressureListener, this.flushTimeoutListener, this.asyncWriterListener);
   }

   public void unhook(UserLatencyTrackingBean latencyBean) {
      latencyBean.unhook(this.backPressureListener, this.flushTimeoutListener, this.asyncWriterListener);
   }

   private static WorkPool createPool(TimeSource timeSource, int maxConcurrency, int backPressureThreshold, int flushTimeout) {
      logger.debug(String.format("Initializing client metrics collector. concurrency: %s, backpressure threshold: %s, flush timeout %s", new Object[]{Integer.valueOf(maxConcurrency), Integer.valueOf(backPressureThreshold), Integer.valueOf(flushTimeout)}));
      return new WorkPool(timeSource, maxConcurrency, backPressureThreshold, flushTimeout, "User metrics");
   }

   @VisibleForTesting
   public void reset() {
      activeTrackers.getAndSet(new NonBlockingHashMap());
   }

   public Iterable<RawUserObjectLatency> getAllMetrics() {
      Map<String, LatencyTracker> toProcess = (Map)activeTrackers.getAndSet(new NonBlockingHashMap());
      return Iterables.concat(Iterables.transform(toProcess.entrySet(), new Function<Entry<String, LatencyTracker>, Iterable<RawUserObjectLatency>>() {
         public Iterable<RawUserObjectLatency> apply(final Entry<String, LatencyTracker> entry) {
            return Iterables.transform((Iterable)entry.getValue(), new Function<RawObjectLatency, RawUserObjectLatency>() {
               public RawUserObjectLatency apply(RawObjectLatency rawObjectLatency) {
                  try {
                     String[] elements = ((String)entry.getKey()).split("\\:");
                     ClientConnectionMetadata ccm = new ClientConnectionMetadata(InetAddress.getByName(elements[0]), Integer.parseInt(elements[1]), elements[2]);
                     return new RawUserObjectLatency(ccm, rawObjectLatency);
                  } catch (Exception var4) {
                     UserMetrics.logger.info("Error transforming raw user/object metrics for processing", var4);
                     return null;
                  }
               }
            });
         }
      }));
   }

   public void recordLatencyEvent(InetSocketAddress clientAddress, String userName, final String keyspace, final String columnFamily, final LatencyValues.EventType type, final long duration, final TimeUnit unit) {
      if(!PerformanceObjectsPlugin.isUntracked(keyspace) && !Boolean.getBoolean("dse.noop_user_metrics")) {
         final String connectionId = clientAddress.getAddress().getHostAddress() + ":" + clientAddress.getPort() + ":" + userName;
         this.pool.submit(new RoutableTask(this.timeSource, connectionId) {
            public int run() {
               try {
                  NonBlockingHashMap<String, LatencyTracker> trackers = (NonBlockingHashMap)UserMetrics.activeTrackers.get();
                  LatencyTracker tracker = (LatencyTracker)trackers.get(connectionId);
                  if(tracker == null) {
                     LatencyTracker old = (LatencyTracker)trackers.put(connectionId, new LatencyTracker());
                     if(old != null) {
                        tracker = old;
                     } else {
                        tracker = (LatencyTracker)trackers.get(connectionId);
                     }
                  }

                  tracker.recordLatencyEvent(keyspace, columnFamily, type, duration, unit);
                  if(UserMetrics.logger.isTraceEnabled()) {
                     UserMetrics.logger.trace(String.format("Logged %s operation by %s on %s lasting %s %s", new Object[]{type.name(), connectionId, keyspace + "." + columnFamily, Long.valueOf(duration), unit.name()}));
                  }
               } catch (Exception var4) {
                  UserMetrics.logger.info("Unable to obtain tracker for current session, latency metrics will not be recorded", var4);
               }

               return 1;
            }
         });
      }
   }
}
