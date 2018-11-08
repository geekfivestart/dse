package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StorageProxyMBean {
   long getTotalHints();

   boolean getHintedHandoffEnabled();

   void setHintedHandoffEnabled(boolean var1);

   void enableHintsForDC(String var1);

   void disableHintsForDC(String var1);

   Set<String> getHintedHandoffDisabledDCs();

   int getMaxHintWindow();

   void setMaxHintWindow(int var1);

   int getMaxHintsInProgress();

   void setMaxHintsInProgress(int var1);

   int getHintsInProgress();

   Long getRpcTimeout();

   void setRpcTimeout(Long var1);

   Long getReadRpcTimeout();

   void setReadRpcTimeout(Long var1);

   Long getWriteRpcTimeout();

   void setWriteRpcTimeout(Long var1);

   Long getCounterWriteRpcTimeout();

   void setCounterWriteRpcTimeout(Long var1);

   Long getCasContentionTimeout();

   void setCasContentionTimeout(Long var1);

   Long getRangeRpcTimeout();

   void setRangeRpcTimeout(Long var1);

   Long getTruncateRpcTimeout();

   void setTruncateRpcTimeout(Long var1);

   long getCrossDCRttLatency();

   void setCrossDCRttLatency(long var1);

   void setNativeTransportMaxConcurrentConnections(Long var1);

   Long getNativeTransportMaxConcurrentConnections();

   void reloadTriggerClasses();

   long getReadRepairAttempted();

   long getReadRepairRepairedBlocking();

   long getReadRepairRepairedBackground();

   /** @deprecated */
   @Deprecated
   int getOtcBacklogExpirationInterval();

   /** @deprecated */
   @Deprecated
   void setOtcBacklogExpirationInterval(int var1);

   Map<String, List<String>> getSchemaVersions();

   int getNumberOfTables();

   String getIdealConsistencyLevel();

   String setIdealConsistencyLevel(String var1);
}
