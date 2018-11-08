package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface ExplicitTTLSnapshotInfoMXBean extends SnapshotPluginMXBean {
   int getTTL();

   void setTTL(int var1) throws PropertyVetoException;
}
