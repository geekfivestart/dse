package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface PluginMXBean {
   boolean isEnabled();

   void setEnabled(boolean var1) throws PropertyVetoException;
}
