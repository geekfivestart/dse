package com.datastax.bdp.plugin;

import java.io.IOException;

public interface GraphOLAPPluginMXBean {
   String getAnalyticsGraphServerIP() throws IOException;

   int getPort();

   boolean isActive();
}
