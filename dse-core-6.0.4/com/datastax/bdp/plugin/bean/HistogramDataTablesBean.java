package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.config.DseConfig;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

@Singleton
public class HistogramDataTablesBean extends SnapshotInfoBean implements HistogramDataMXBean {
   private static final Logger logger = LoggerFactory.getLogger(HistogramDataTablesBean.class);
   public static final String RETENTION_COUNT_PROPERTY_NAME = "retentionCount";
   private final AtomicInteger retentionCount = new AtomicInteger();

   public HistogramDataTablesBean() {
      try {
         this.setRefreshRate(DseConfig.getHistogramDataTablesRefreshRate());
         this.maybeEnable(DseConfig.histogramDataTablesStatsEnabled());
         this.retentionCount.set(DseConfig.getHistogramDataTablesRetentionCount());
      } catch (PropertyVetoException var2) {
         throw new AssertionError("Preregistered listeners?", var2);
      }
   }

   public int getRetentionCount() {
      return this.retentionCount.get();
   }

   public synchronized void setRetentionCount(int retentionCount) throws PropertyVetoException {
      int oldValue = this.retentionCount.get();
      if(oldValue != retentionCount) {
         this.fireVetoableChange("retentionCount", Integer.valueOf(oldValue), Integer.valueOf(retentionCount));
         this.retentionCount.set(retentionCount);
         this.firePropertyChange("retentionCount", Integer.valueOf(oldValue), Integer.valueOf(retentionCount));
         logger.info("Setting retention count to {}", Integer.valueOf(retentionCount));
      }

   }

   public String getConfigName() {
      return "histogram_data_options";
   }

   public String getConfigSetting() {
      return (new Yaml()).dump(ImmutableMap.builder().put(this.getConfigName(), ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("refresh_rate_ms", "" + this.getRefreshRate()).put("retention_count", "" + this.getRetentionCount()).build()).build());
   }
}
