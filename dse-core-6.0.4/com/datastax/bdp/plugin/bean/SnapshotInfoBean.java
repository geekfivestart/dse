package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.google.common.collect.ImmutableMap;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public abstract class SnapshotInfoBean extends PluginBean implements SnapshotPluginMXBean {
   private static final Logger logger = LoggerFactory.getLogger(SnapshotInfoBean.class);
   public static final String REFRESH_RATE_PROPERTY_NAME = "refreshRate";
   private final AtomicInteger refreshRate = new AtomicInteger();

   public SnapshotInfoBean() {
   }

   public int getRefreshRate() {
      return this.refreshRate.get();
   }

   public synchronized void setRefreshRate(int refreshRate) throws PropertyVetoException {
      int oldValue = this.refreshRate.get();
      if(oldValue != refreshRate) {
         if(refreshRate < DseConfig.MIN_REFRESH_RATE_MS) {
            PropertyChangeEvent event = new PropertyChangeEvent(this, "refreshRate", Integer.valueOf(oldValue), Integer.valueOf(refreshRate));
            throw new PropertyVetoException("Refresh rate must be >= " + DseConfig.MIN_REFRESH_RATE_MS, event);
         }

         this.fireVetoableChange("refreshRate", Integer.valueOf(oldValue), Integer.valueOf(refreshRate));
         this.refreshRate.set(refreshRate);
         this.firePropertyChange("refreshRate", Integer.valueOf(oldValue), Integer.valueOf(refreshRate));
         if(this.isEnabled()) {
            logger.info("{} refresh rate set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Integer.valueOf(refreshRate), Integer.valueOf(oldValue)});
         }
      }

   }

   public abstract String getConfigName();

   public String getConfigSetting() {
      return (new Yaml()).dump(ImmutableMap.builder().put(this.getConfigName(), ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("refresh_rate_ms", "" + this.getRefreshRate()).build()).build());
   }
}
