package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.plugin.ConfigExportableMXBean;

public interface TTLMXBean extends PluginMXBean, ConfigExportableMXBean {
   int getTTL();
}
