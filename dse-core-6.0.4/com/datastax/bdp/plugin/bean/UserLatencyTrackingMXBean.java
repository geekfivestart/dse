package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface UserLatencyTrackingMXBean extends AsyncSnapshotPluginMXBean {
   int getBackpressureThreshold();

   int getFlushTimeout();

   int getTopStatsLimit();

   void setBackpressureThreshold(int var1) throws PropertyVetoException;

   void setFlushTimeout(int var1) throws PropertyVetoException;

   void setTopStatsLimit(int var1) throws PropertyVetoException;
}
