package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.config.DseConfig;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyVetoException;
import java.beans.VetoableChangeListener;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

@Singleton
public class UserLatencyTrackingBean extends AsyncSnapshotInfoBean implements UserLatencyTrackingMXBean {
   private static final Logger logger = LoggerFactory.getLogger(UserLatencyTrackingBean.class);
   public static final String BACKPRESSURE_PROPERTY_NAME = "backpressureThreshold";
   public static final String FLUSH_TIMEOUT_PROPERTY_NAME = "flushTimeout";
   public static final String TOP_STATS_LIMIT_PROPERTY_NAME = "topStatsLimit";
   private final AtomicInteger backpressureThreshold = new AtomicInteger();
   private final AtomicInteger flushTimeout = new AtomicInteger();
   private final AtomicInteger topStatsLimit = new AtomicInteger();

   public UserLatencyTrackingBean() {
      try {
         this.maybeEnable(DseConfig.userLatencyTrackingEnabled());
         this.setAsyncWriters(DseConfig.getUserLatencyAsyncWriters());
         this.setRefreshRate(DseConfig.getUserLatencyRefreshRate());
         this.backpressureThreshold.set(DseConfig.getUserLatencyBackpressureThreshold());
         this.flushTimeout.set(DseConfig.getUserLatencyFlushTimeout());
         this.topStatsLimit.set(DseConfig.getUserLatencyTopStatsLimit());
      } catch (PropertyVetoException var2) {
         throw new AssertionError("Preregistered listeners?", var2);
      }
   }

   public int getBackpressureThreshold() {
      return this.backpressureThreshold.get();
   }

   public int getFlushTimeout() {
      return this.flushTimeout.get();
   }

   public int getTopStatsLimit() {
      return this.topStatsLimit.get();
   }

   public synchronized void setBackpressureThreshold(int backpressureThreshold) throws PropertyVetoException {
      int oldValue = this.backpressureThreshold.get();
      if(oldValue != backpressureThreshold) {
         if(backpressureThreshold < 0) {
            throw new PropertyVetoException("Must be >= 0", new PropertyChangeEvent(this, "backpressureThreshold", Integer.valueOf(this.backpressureThreshold.get()), Integer.valueOf(backpressureThreshold)));
         }

         this.fireVetoableChange("backpressureThreshold", Integer.valueOf(oldValue), Integer.valueOf(backpressureThreshold));
         this.backpressureThreshold.set(backpressureThreshold);
         this.firePropertyChange("backpressureThreshold", Integer.valueOf(oldValue), Integer.valueOf(backpressureThreshold));
         logger.info("Setting backpressure threshold to {}", Integer.valueOf(backpressureThreshold));
      }

   }

   public synchronized void setFlushTimeout(int flushTimeout) throws PropertyVetoException {
      int oldValue = this.flushTimeout.get();
      if(oldValue != flushTimeout) {
         if(flushTimeout < 0) {
            throw new PropertyVetoException("Must be >= 0", new PropertyChangeEvent(this, "flushTimeout", Integer.valueOf(this.flushTimeout.get()), Integer.valueOf(flushTimeout)));
         }

         this.fireVetoableChange("flushTimeout", Integer.valueOf(oldValue), Integer.valueOf(flushTimeout));
         this.flushTimeout.set(flushTimeout);
         this.firePropertyChange("flushTimeout", Integer.valueOf(oldValue), Integer.valueOf(flushTimeout));
         logger.info("Setting flush timeout to {}", Integer.valueOf(flushTimeout));
      }

   }

   public synchronized void setTopStatsLimit(int topStatsLimit) throws PropertyVetoException {
      int oldValue = this.topStatsLimit.get();
      if(oldValue != topStatsLimit) {
         if(topStatsLimit < 0) {
            throw new PropertyVetoException("Must be >= 0", new PropertyChangeEvent(this, "topStatsLimit", Integer.valueOf(this.topStatsLimit.get()), Integer.valueOf(topStatsLimit)));
         }

         this.fireVetoableChange("topStatsLimit", Integer.valueOf(oldValue), Integer.valueOf(topStatsLimit));
         this.topStatsLimit.set(topStatsLimit);
         this.firePropertyChange("topStatsLimit", Integer.valueOf(oldValue), Integer.valueOf(topStatsLimit));
         logger.info("Setting top stats limit to {}", Integer.valueOf(topStatsLimit));
      }

   }

   public void unhook(PropertyChangeListener backPressureListener, PropertyChangeListener flushTimeoutListener, VetoableChangeListener asyncWriterListener) {
      this.removePropertyChangeListener("backpressureThreshold", backPressureListener);
      this.removePropertyChangeListener("flushTimeout", flushTimeoutListener);
      this.removeVetoableChangeListener("asyncWriters", asyncWriterListener);
   }

   public void hook(PropertyChangeListener backPressureListener, PropertyChangeListener flushTimeoutListener, VetoableChangeListener asyncWriterListener) {
      this.addPropertyChangeListener("backpressureThreshold", backPressureListener);
      this.addPropertyChangeListener("flushTimeout", flushTimeoutListener);
      this.addVetoableChangeListener("asyncWriters", asyncWriterListener);
   }

   public String getConfigName() {
      return "user_level_latency_tracking_options";
   }

   public String getConfigSetting() {
      return (new Yaml()).dump(ImmutableMap.builder().put(this.getConfigName(), ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("refresh_rate_ms", "" + this.getRefreshRate()).put("top_stats_limit", "" + this.getTopStatsLimit()).put("async_writers", "" + this.getAsyncWriters()).put("backpressure_threshold", "" + this.getBackpressureThreshold()).put("flush_timeout_ms", "" + this.getFlushTimeout()).put("quantiles", "" + DseConfig.resourceLatencyTrackingQuantiles()).build()).build());
   }
}
