package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface AsyncSnapshotPluginMXBean extends SnapshotPluginMXBean {
   int getAsyncWriters();

   void setAsyncWriters(int var1) throws PropertyVetoException;
}
