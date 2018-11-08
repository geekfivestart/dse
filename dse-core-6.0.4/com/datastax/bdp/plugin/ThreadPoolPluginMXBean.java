package com.datastax.bdp.plugin;

import com.datastax.bdp.plugin.bean.SnapshotPluginMXBean;
import java.beans.PropertyVetoException;

public interface ThreadPoolPluginMXBean extends SnapshotPluginMXBean {
   int getActiveCount();

   int getCoreThreads();

   void setCoreThreads(int var1) throws PropertyVetoException;

   int getMaxThreads();

   void setMaxThreads(int var1) throws PropertyVetoException;

   int getQueueCapacity();
}
