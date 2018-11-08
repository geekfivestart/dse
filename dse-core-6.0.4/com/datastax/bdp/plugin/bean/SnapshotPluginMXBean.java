package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.plugin.ConfigExportableMXBean;
import java.beans.PropertyVetoException;

public interface SnapshotPluginMXBean extends PluginMXBean, ConfigExportableMXBean {
   int getRefreshRate();

   void setRefreshRate(int var1) throws PropertyVetoException;
}
