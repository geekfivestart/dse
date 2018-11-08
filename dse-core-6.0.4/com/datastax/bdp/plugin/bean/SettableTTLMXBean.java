package com.datastax.bdp.plugin.bean;

import java.beans.PropertyVetoException;

public interface SettableTTLMXBean extends TTLMXBean {
   void setTTL(int var1) throws PropertyVetoException;
}
