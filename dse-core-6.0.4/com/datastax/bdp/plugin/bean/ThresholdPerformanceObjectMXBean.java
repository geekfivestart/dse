package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface ThresholdPerformanceObjectMXBean extends SettableTTLMXBean {
   int getThreshold();

   void setThreshold(int var1) throws PropertyVetoException;
}
