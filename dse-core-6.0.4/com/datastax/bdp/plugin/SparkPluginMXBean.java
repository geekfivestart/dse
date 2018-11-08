package com.datastax.bdp.plugin;

import java.io.IOException;
import java.util.Map;

public interface SparkPluginMXBean {
   void createSparkMasterRecoverySchema(boolean var1) throws IOException;

   void clearRecoveryData(String var1) throws IOException;

   boolean isMasterRunning();

   boolean isWorkerRunning();

   String getReleaseVersion();

   void restartWorker();

   String getMasterAddress() throws IOException;

   boolean isActive();

   Map<String, String> getSecurityConfig();

   int getShuffleServicePort(boolean var1);
}
