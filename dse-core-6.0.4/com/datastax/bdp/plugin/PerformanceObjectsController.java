package com.datastax.bdp.plugin;

import com.datastax.bdp.config.CqlSlowLogOptions;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.plugin.bean.PluginBean;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.plugin.bean.TTLBean;
import com.datastax.bdp.server.LifecycleAware;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.util.rpc.Rpc;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.auth.permission.CorePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

@Singleton
public class PerformanceObjectsController implements LifecycleAware {
   private static final Logger logger = LoggerFactory.getLogger(PerformanceObjectsController.class);
   public static final String PROPERTY_VETO_ERROR_MESSAGE = "Please make sure no listeners are already registered and that dse.yaml option values are valid.";
   private final Set<PluginBean> plugins;

   @Inject
   public PerformanceObjectsController(Set<PluginBean> pluginBeans) {
      this.plugins = pluginBeans;
   }

   @Override
   public void preSetup() {
      for (PluginBean plugin : this.plugins) {
         plugin.registerMBean();
      }
   }

   public <T extends PluginBean> T getPluginBeanFor(Class<T> pluginClass) {
      for (PluginBean plugin : this.plugins) {
         if (!plugin.getClass().equals(pluginClass)) continue;
         return (T)plugin;
      }
      return null;
   }

   public static String getPerfBeanName(Class<? extends PluginBean> pluginClass) {
      String name = pluginClass.getSimpleName();
      return name.endsWith("Bean")?name.substring(0, name.length() - 4):name;
   }

   public List<PluginBean> getPlugins() {
      return new ArrayList(this.plugins);
   }

   public boolean isAnyEnabled() {
      Iterator var1 = this.plugins.iterator();

      PluginBean plugin;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         plugin = (PluginBean)var1.next();
      } while(!plugin.isEnabled());

      return true;
   }

   @Singleton
   public static class SparkApplicationInfoBean extends SnapshotInfoBean implements ConfigExportableMXBean {
      public final boolean driverSink;
      public final boolean driverJvmSource;
      public final boolean driverConnectorSource;
      public final boolean driverStateSource;
      public final boolean executorSink;
      public final boolean executorJvmSource;
      public final boolean executorConnectorSource;
      private static final String jvmSrc = "jvm";
      private static final String jvmClass = "org.apache.spark.metrics.source.JvmSource";
      private static final String ccSrc = "cassandra-connector";
      private static final String ccClass = "org.apache.spark.metrics.CassandraConnectorSource";
      private static final String stateSrc = "application-state";
      private static final String stateClass = "org.apache.spark.metrics.ApplicationStateSource";
      private static final String executor = "executor";
      private static final String driver = "driver";

      public SparkApplicationInfoBean() {
         try {
            this.setRefreshRate(DseConfig.getSparkApplicationInfoRefreshRate());
            this.maybeEnable(DseConfig.sparkApplicationInfoEnabled());
            this.driverSink = DseConfig.sparkApplicationInfoDriverSink();
            this.driverJvmSource = DseConfig.sparkApplicationInfoDriverJvmSource();
            this.driverConnectorSource = DseConfig.sparkApplicationInfoDriverConnectorSource();
            this.driverStateSource = DseConfig.sparkApplicationInfoDriverStateSource();
            this.executorSink = DseConfig.sparkApplicationInfoExecutorSink();
            this.executorJvmSource = DseConfig.sparkApplicationInfoExecutorJvmSource();
            this.executorConnectorSource = DseConfig.sparkApplicationInfoExecutorConnectorSource();
         } catch (PropertyVetoException var2) {
            throw new IllegalArgumentException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      private void addSink(Map<String, String> props, String instance) {
         props.put(String.format("%s.sink.cassandra.class", new Object[]{instance}), "org.apache.spark.metrics.CassandraSink");
         props.put(String.format("%s.sink.cassandra.period", new Object[]{instance}), String.valueOf(this.getRefreshRate() / 1000));
         props.put(String.format("%s.sink.cassandra.ttl", new Object[]{instance}), String.valueOf(this.getRefreshRate() * 3 / 1000));
      }

      private void addSource(Map<String, String> props, String instance, String name, String className) {
         props.put(String.format("%s.source.%s.class", new Object[]{instance, name}), className);
      }

      public Map<String, String> asSparkProperties() {
         Map<String, String> props = new HashMap();
         if(this.driverSink) {
            this.addSink(props, "driver");
         }

         if(this.executorSink) {
            this.addSink(props, "executor");
         }

         if(this.driverConnectorSource) {
            this.addSource(props, "driver", "cassandra-connector", "org.apache.spark.metrics.CassandraConnectorSource");
         }

         if(this.driverJvmSource) {
            this.addSource(props, "driver", "jvm", "org.apache.spark.metrics.source.JvmSource");
         }

         if(this.driverStateSource) {
            this.addSource(props, "driver", "application-state", "org.apache.spark.metrics.ApplicationStateSource");
         }

         if(this.executorConnectorSource) {
            this.addSource(props, "executor", "cassandra-connector", "org.apache.spark.metrics.CassandraConnectorSource");
         }

         if(this.executorJvmSource) {
            this.addSource(props, "executor", "jvm", "org.apache.spark.metrics.source.JvmSource");
         }

         return props;
      }

      public String getConfigName() {
         return "spark_application_info_options";
      }

      public String getConfigSetting() {
         return (new Yaml()).dump(ImmutableMap.builder().put(this.getConfigName(), ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("refresh_rate_ms", "" + this.getRefreshRate()).put("driver", ImmutableMap.builder().put("sink", "" + this.driverSink).put("connectorSource", "" + this.driverConnectorSource).put("jvmSource", "" + this.driverJvmSource).put("stateSource", "" + this.driverStateSource).build()).put("executor", ImmutableMap.builder().put("sink", "" + this.executorSink).put("connectorSource", "" + this.executorConnectorSource).put("jvmSource", "" + this.executorJvmSource).build()).build()).build());
      }

      public boolean isCompatibleWithWorkload() {
         return SystemInfo.isSparkNode();
      }
   }

   @Singleton
   public static class SparkClusterInfoBean extends SnapshotInfoBean {
      public SparkClusterInfoBean() {
         try {
            this.maybeEnable(DseConfig.sparkClusterInfoEnabled());
            this.setRefreshRate(DseConfig.getSparkClusterInfoRefreshRate());
         } catch (PropertyVetoException var2) {
            throw new IllegalStateException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      public String getConfigName() {
         return "spark_cluster_info_options";
      }

      public boolean isCompatibleWithWorkload() {
         return SystemInfo.isSparkNode();
      }
   }

   @Singleton
   public static class ResourceLatencyTrackingBean extends SnapshotInfoBean {
      public ResourceLatencyTrackingBean() {
         try {
            this.maybeEnable(DseConfig.resourceLatencyTrackingEnabled());
            this.setRefreshRate(DseConfig.getResourceLatencyRefreshRate());
         } catch (PropertyVetoException var2) {
            throw new IllegalStateException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      public String getConfigName() {
         return "resource_level_latency_tracking_options";
      }
   }

   @Singleton
   public static class DbSummaryStatsBean extends SnapshotInfoBean {
      public DbSummaryStatsBean() {
         try {
            this.maybeEnable(DseConfig.dbSummaryStatsEnabled());
            this.setRefreshRate(DseConfig.getDbSummaryStatsRefreshRate());
         } catch (PropertyVetoException var2) {
            throw new IllegalStateException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      public String getConfigName() {
         return "db_summary_stats_options";
      }
   }

   @Singleton
   public static class ClusterSummaryStatsBean extends SnapshotInfoBean {
      public ClusterSummaryStatsBean() {
         try {
            this.setRefreshRate(DseConfig.getClusterSummaryStatsRefreshRate());
            this.maybeEnable(DseConfig.clusterSummaryStatsEnabled());
         } catch (PropertyVetoException var2) {
            throw new IllegalStateException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      public String getConfigName() {
         return "cluster_summary_stats_options";
      }
   }

   @Singleton
   public static class CqlSystemInfoBean extends SnapshotInfoBean {
      public CqlSystemInfoBean() {
         try {
            this.setEnabled(DseConfig.cqlSystemInfoEnabled());
            this.setRefreshRate(DseConfig.getCqlSystemInfoRefreshRate());
         } catch (PropertyVetoException var2) {
            throw new IllegalStateException("Please make sure no listeners are already registered and that dse.yaml option values are valid.", var2);
         }
      }

      public String getConfigName() {
         return "cql_system_info_options";
      }
   }

   @Singleton
   public static class CqlSlowLogBean extends TTLBean implements CqlSlowLogMXBean {
      private final AtomicDouble threshold = new AtomicDouble();
      private final AtomicLong effectiveThreshold = new AtomicLong(9223372036854775807L);
      private final AtomicLong minimumSamples = new AtomicLong();
      private final AtomicBoolean skipWritingToDB = new AtomicBoolean();
      private final AtomicInteger numSlowestQueries = new AtomicInteger();
      public final AtomicReference<Queue<CqlSlowLogMXBean.SlowCqlQuery>> slowestQueries = new AtomicReference();

      public CqlSlowLogBean() {
         super(DseConfig.getCqlSlowLogEnabled(), DseConfig.getCqlSlowLogTTL());
         this.threshold.set(DseConfig.getCqlSlowLogThreshold());
         this.minimumSamples.set((long)DseConfig.getCqlSlowLogMinimumSamples());
         this.skipWritingToDB.set(DseConfig.getCqlSlowLogSkipWritingToDB());
         this.numSlowestQueries.set(DseConfig.getCqlSlowLogNumSlowestQueries());
         this.slowestQueries.set(Queues.synchronizedQueue(MinMaxPriorityQueue.orderedBy(SLOW_CQL_QUERY_COMPARATOR).maximumSize(this.numSlowestQueries.get()).create()));
      }

      public long getEffectiveThreshold() {
         return this.effectiveThreshold.get();
      }

      public void setEffectiveThreshold(long effectiveThreshold) {
         long oldValue = this.effectiveThreshold.get();
         if(oldValue != effectiveThreshold) {
            this.effectiveThreshold.set(effectiveThreshold);
            this.firePropertyChange("effectiveThreshold", Long.valueOf(oldValue), Long.valueOf(effectiveThreshold));
            PerformanceObjectsController.logger.debug("{} effective threshold set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Long.valueOf(effectiveThreshold), Long.valueOf(oldValue)});
         }

      }

      public double getThreshold() {
         return this.threshold.get();
      }

      public void setThreshold(double threshold) throws PropertyVetoException {
         double oldValue = this.threshold.get();
         if(oldValue != threshold) {
            if(threshold < 0.0D) {
               throw new PropertyVetoException("Threshold must be >= 0.0", new PropertyChangeEvent(this, "threshold", Double.valueOf(oldValue), Double.valueOf(threshold)));
            }

            this.fireVetoableChange("threshold", Double.valueOf(oldValue), Double.valueOf(threshold));
            this.threshold.set(threshold);
            this.firePropertyChange("threshold", Double.valueOf(oldValue), Double.valueOf(threshold));
            PerformanceObjectsController.logger.info("{} threshold set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Double.valueOf(threshold), Double.valueOf(oldValue)});
         }

      }

      public long getMinimumSamples() {
         return this.minimumSamples.get();
      }

      public void setMinimumSamples(long minimumSamples) throws PropertyVetoException {
         long oldValue = this.minimumSamples.get();
         if(oldValue != minimumSamples) {
            if(minimumSamples < 1L) {
               throw new PropertyVetoException("Minimum samples must be >= 1", new PropertyChangeEvent(this, "minimumSamples", Long.valueOf(oldValue), Long.valueOf(minimumSamples)));
            }

            this.fireVetoableChange("minimumSamples", Long.valueOf(oldValue), Long.valueOf(minimumSamples));
            this.minimumSamples.set(minimumSamples);
            this.firePropertyChange("minimumSamples", Long.valueOf(oldValue), Long.valueOf(minimumSamples));
            PerformanceObjectsController.logger.info("{} minimum samples set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Long.valueOf(minimumSamples), Long.valueOf(oldValue)});
         }

      }

      public boolean isSkipWritingToDB() {
         return this.skipWritingToDB.get();
      }

      public void setSkipWritingToDB(boolean skipWritingToDB) {
         boolean oldValue = this.skipWritingToDB.get();
         if(oldValue != skipWritingToDB) {
            this.skipWritingToDB.set(skipWritingToDB);
            PerformanceObjectsController.logger.info("{} skip_writing_to_db set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Boolean.valueOf(skipWritingToDB), Boolean.valueOf(oldValue)});
         }

      }

      public int getNumSlowestQueries() {
         return this.numSlowestQueries.get();
      }

      public void setNumSlowestQueries(int numSlowestQueries) throws PropertyVetoException {
         int oldValue = this.numSlowestQueries.get();
         if(oldValue != numSlowestQueries) {
            if(numSlowestQueries < CqlSlowLogOptions.NUM_SLOWEST_QUERIES_LOWER_BOUND.intValue()) {
               throw new PropertyVetoException(String.format("num_slowest_queries must be >= %s", new Object[]{CqlSlowLogOptions.NUM_SLOWEST_QUERIES_LOWER_BOUND}), new PropertyChangeEvent(this, "num_slowest_queries", Integer.valueOf(oldValue), Integer.valueOf(numSlowestQueries)));
            }

            this.numSlowestQueries.set(numSlowestQueries);
            this.resizeMinMaxHeap();
            PerformanceObjectsController.logger.info("{} num_slowest_queries set to {} (was {}).", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Integer.valueOf(numSlowestQueries), Integer.valueOf(oldValue)});
         }

      }

      private void resizeMinMaxHeap() {
         synchronized((Queue)this.slowestQueries.get()) {
            Queue<CqlSlowLogMXBean.SlowCqlQuery> q = (Queue)this.slowestQueries.get();
            this.slowestQueries.set(Queues.synchronizedQueue(MinMaxPriorityQueue.orderedBy(SLOW_CQL_QUERY_COMPARATOR).maximumSize(this.numSlowestQueries.get()).create()));
            ((Queue)this.slowestQueries.get()).addAll(q);
         }
      }

      @Rpc(
         name = "retrieveRecentSlowestCqlQueries",
         permission = CorePermission.SELECT,
         multiRow = true
      )
      public List<CqlSlowLogMXBean.SlowCqlQuery> retrieveRecentSlowestCqlQueries() {
         List<CqlSlowLogMXBean.SlowCqlQuery> queries = new ArrayList(this.numSlowestQueries.get());
         queries.addAll((Collection)this.slowestQueries.getAndSet(Queues.synchronizedQueue(MinMaxPriorityQueue.orderedBy(SLOW_CQL_QUERY_COMPARATOR).maximumSize(this.numSlowestQueries.get()).create())));
         Collections.sort(queries, SLOW_CQL_QUERY_COMPARATOR);
         return queries;
      }

      public String getConfigSetting() {
         return (new Yaml()).dump(ImmutableMap.builder().put("cql_slow_log_options", ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("threshold", "" + this.threshold.toString()).put("minimum_samples", "" + this.minimumSamples.toString()).put("ttl_seconds", "" + this.ttl.toString()).put("skip_writing_to_db", "" + this.skipWritingToDB.get()).put("num_slowest_queries", "" + this.numSlowestQueries.get()).build()).build());
      }
   }
}
