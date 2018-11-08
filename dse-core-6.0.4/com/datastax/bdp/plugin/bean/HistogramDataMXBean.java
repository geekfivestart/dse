package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface HistogramDataMXBean extends SnapshotPluginMXBean {
   int getRetentionCount();

   void setRetentionCount(int var1) throws PropertyVetoException;
}
