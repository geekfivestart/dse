package com.datastax.bdp.plugin;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

@Singleton
public class ThreadPoolPluginBean extends SnapshotInfoBean implements ThreadPoolPluginMXBean {
   public static final String MAX_THREADS_PROP_NAME = "maxThreads";
   public static final String CORE_THREADS_PROP_NAME = "coreThreads";
   private static final String MAX_QUEUE_PROP_NAME = "queueSize";
   private static final int MIN_QUEUE = 0;
   private final AtomicInteger coreThreads;
   private final AtomicInteger maxThreads;
   private final AtomicInteger queueSize;
   private ThreadPoolPlugin plugin;
   private static final Logger logger = LoggerFactory.getLogger(ThreadPoolPluginBean.class);

   public ThreadPoolPluginBean() {
      this(DseConfig.getPerformanceCoreThreads(), DseConfig.getPerformanceMaxThreads(), DseConfig.getPerformanceQueueCapacity());
   }

   public ThreadPoolPluginBean(int coreThreads, int maxThreads, int queueSize) {
      this.coreThreads = new AtomicInteger(4);
      this.maxThreads = new AtomicInteger(32);
      this.queueSize = new AtomicInteger(32000);

      try {
         this.setCoreThreads(coreThreads);
      } catch (PropertyVetoException var7) {
         throw new AssertionError("Invalid value in dse.yaml for performance_core_threads");
      }

      try {
         this.setMaxThreads(maxThreads);
      } catch (PropertyVetoException var6) {
         throw new AssertionError("Invalid value in dse.yaml for performance_max_threads");
      }

      try {
         this.setQueueSize(queueSize);
      } catch (PropertyVetoException var5) {
         throw new AssertionError("Invalid value in dse.yaml for performance_queue_capacity");
      }
   }

   public int getMaxThreads() {
      return this.maxThreads.get();
   }

   public final void setMaxThreads(int maxThreads) throws PropertyVetoException {
      int oldValue = this.maxThreads.get();
      if(maxThreads < 1) {
         throw new PropertyVetoException("Capacity must be greater or equal to 1", new PropertyChangeEvent(this, "maxThreads", Integer.valueOf(oldValue), Integer.valueOf(maxThreads)));
      } else {
         this.fireVetoableChange("maxThreads", Integer.valueOf(oldValue), Integer.valueOf(maxThreads));
         this.maxThreads.set(maxThreads);
         this.firePropertyChange("maxThreads", Integer.valueOf(oldValue), Integer.valueOf(maxThreads));
      }
   }

   public int getQueueCapacity() {
      return this.queueSize.get();
   }

   public final void setQueueSize(int queueSize) throws PropertyVetoException {
      int oldValue = this.queueSize.get();
      if(queueSize < 0) {
         throw new PropertyVetoException("Queue size must be greater or equal to 0", new PropertyChangeEvent(this, "queueSize", Integer.valueOf(oldValue), Integer.valueOf(queueSize)));
      } else {
         this.fireVetoableChange("queueSize", Integer.valueOf(oldValue), Integer.valueOf(queueSize));
         this.queueSize.set(queueSize);
         this.firePropertyChange("queueSize", Integer.valueOf(oldValue), Integer.valueOf(queueSize));
      }
   }

   public int getCoreThreads() {
      return this.coreThreads.get();
   }

   public final void setCoreThreads(int coreThreads) throws PropertyVetoException {
      int oldValue = this.coreThreads.get();
      if(coreThreads < 0) {
         throw new PropertyVetoException("Core threads must be greater or equal to 0", new PropertyChangeEvent(this, "coreThreads", Integer.valueOf(oldValue), Integer.valueOf(coreThreads)));
      } else {
         this.fireVetoableChange("coreThreads", Integer.valueOf(oldValue), Integer.valueOf(this.maxThreads.get()));
         this.coreThreads.set(coreThreads);
         this.firePropertyChange("coreThreads", Integer.valueOf(oldValue), Integer.valueOf(this.maxThreads.get()));
      }
   }

   public int getActiveCount() {
      return this.plugin.getActiveCount();
   }

   public void setPlugin(ThreadPoolPlugin plugin) {
      this.plugin = plugin;
   }

   public String getConfigName() {
      return null;
   }

   public String getConfigSetting() {
      return (new Yaml()).dump(ImmutableMap.builder().put("performance_core_threads", "" + this.getCoreThreads()).put("performance_max_threads", "" + this.getMaxThreads()).build());
   }
}
