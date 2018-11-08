package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.plugin.PerformanceObjectsController;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThresholdPerformanceObjectBean extends TTLBean implements ThresholdPerformanceObjectMXBean {
   private static final Logger logger = LoggerFactory.getLogger(ThresholdPerformanceObjectBean.class);
   public static final String THRESHOLD_PROPERTY_NAME = "threshold";
   protected final AtomicInteger threshold = new AtomicInteger();

   public ThresholdPerformanceObjectBean(boolean enabled, int thresholdMillis, int ttlSeconds) {
      super(enabled, ttlSeconds);
      this.threshold.set(thresholdMillis);
   }

   public int getThreshold() {
      return this.threshold.get();
   }

   public synchronized void setThreshold(int threshold) throws PropertyVetoException {
      int oldValue = this.threshold.get();
      if(oldValue != threshold) {
         this.fireVetoableChange("threshold", Integer.valueOf(oldValue), Integer.valueOf(threshold));
         this.threshold.set(threshold);
         this.firePropertyChange("threshold", Integer.valueOf(oldValue), Integer.valueOf(threshold));
         if(this.isEnabled()) {
            logger.info("{} threshold set to {} (was {})", new Object[]{PerformanceObjectsController.getPerfBeanName(this.getClass()), Integer.valueOf(threshold), Integer.valueOf(oldValue)});
         }
      }

   }
}
