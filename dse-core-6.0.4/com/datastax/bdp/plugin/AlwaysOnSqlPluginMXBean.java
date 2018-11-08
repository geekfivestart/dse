package com.datastax.bdp.plugin;

public interface AlwaysOnSqlPluginMXBean {
   void reconfigureAlwaysOnSql();

   void reconfigureAlwaysOnSql(String var1);

   boolean isAlwaysOnSqlRunningAt(String var1);

   boolean isActive();

   boolean isRunning();

   String getStatus();

   String getServiceAddress();
}
