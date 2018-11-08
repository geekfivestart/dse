package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.metrics.StorageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBroadcaster implements IEndpointStateChangeSubscriber {
   static final int BROADCAST_INTERVAL = Integer.getInteger("cassandra.broadcast_interval_ms", '\uea60').intValue();
   public static final LoadBroadcaster instance = new LoadBroadcaster();
   private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);
   private ConcurrentMap<InetAddress, Double> loadInfo = new ConcurrentHashMap();

   private LoadBroadcaster() {
      Gossiper.instance.register(this);
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      if(state == ApplicationState.LOAD) {
         this.loadInfo.put(endpoint, Double.valueOf(value.value));
      }
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
      VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
      if(localValue != null) {
         this.onChange(endpoint, ApplicationState.LOAD, localValue);
      }

   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
   }

   public void onRestart(InetAddress endpoint, EndpointState state) {
   }

   public void onRemove(InetAddress endpoint) {
      this.loadInfo.remove(endpoint);
   }

   public Map<InetAddress, Double> getLoadInfo() {
      return Collections.unmodifiableMap(this.loadInfo);
   }

   public void startBroadcasting() {
      Runnable runnable = new Runnable() {
         public void run() {
            if(LoadBroadcaster.logger.isTraceEnabled()) {
               LoadBroadcaster.logger.trace("Disseminating load info ...");
            }

            Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD, StorageService.instance.valueFactory.load((double)StorageMetrics.load.getCount()));
         }
      };
      ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(runnable, 2000L, (long)BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
   }
}
