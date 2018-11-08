package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.config.DseConfig;
import com.google.inject.Singleton;
import java.beans.PropertyVetoException;
import java.beans.VetoableChangeListener;

@Singleton
public class LeaseMetricsBean extends AsyncSnapshotInfoBean implements LeaseMetricsMXBean {
   public LeaseMetricsBean() {
      try {
         this.setEnabled(DseConfig.leaseMetricsEnabled());
         this.setAsyncWriters(1);
         this.setRefreshRate(DseConfig.getLeaseMetricsRefreshRate());
      } catch (PropertyVetoException var2) {
         throw new AssertionError("Did the superclass register listeners that reject the initial values?", var2);
      }
   }

   public void unhook(VetoableChangeListener asyncWriterListener) {
      this.removeVetoableChangeListener("asyncWriters", asyncWriterListener);
   }

   public void hook(VetoableChangeListener asyncWriterListener) {
      this.addVetoableChangeListener("asyncWriters", asyncWriterListener);
   }

   public String getConfigName() {
      return "lease_metrics_options";
   }
}
