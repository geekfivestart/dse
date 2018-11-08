package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.management.NotificationEmitter;
import javax.management.openmbean.TabularData;

public interface StorageServiceMBean extends NotificationEmitter {
   List<String> getLiveNodes();

   List<String> getUnreachableNodes();

   List<String> getJoiningNodes();

   List<String> getLeavingNodes();

   List<String> getMovingNodes();

   List<String> getTokens();

   List<String> getTokens(String var1) throws UnknownHostException;

   String getReleaseVersion();

   String getSchemaVersion();

   String[] getAllDataFileLocations();

   String getCommitLogLocation();

   String getSavedCachesLocation();

   Map<List<String>, List<String>> getRangeToEndpointMap(String var1);

   /** @deprecated */
   @Deprecated
   Map<List<String>, List<String>> getRangeToRpcaddressMap(String var1);

   Map<List<String>, List<String>> getRangeToNativeTransportAddressMap(String var1);

   List<String> describeRingJMX(String var1) throws IOException;

   Map<List<String>, List<String>> getPendingRangeToEndpointMap(String var1);

   Map<String, String> getTokenToEndpointMap();

   String getLocalHostId();

   /** @deprecated */
   @Deprecated
   Map<String, String> getHostIdMap();

   Map<String, String> getEndpointToHostId();

   Map<String, String> getHostIdToEndpoint();

   String getLoadString();

   Map<String, String> getLoadMap();

   int getCurrentGenerationNumber();

   List<InetAddress> getNaturalEndpoints(String var1, String var2, String var3);

   List<InetAddress> getNaturalEndpoints(String var1, ByteBuffer var2);

   /** @deprecated */
   @Deprecated
   void takeSnapshot(String var1, String... var2) throws IOException;

   /** @deprecated */
   @Deprecated
   void takeTableSnapshot(String var1, String var2, String var3) throws IOException;

   /** @deprecated */
   @Deprecated
   void takeMultipleTableSnapshot(String var1, String... var2) throws IOException;

   void takeSnapshot(String var1, Map<String, String> var2, String... var3) throws IOException;

   void clearSnapshot(String var1, String... var2) throws IOException;

   Map<String, TabularData> getSnapshotDetails();

   long trueSnapshotsSize();

   void refreshSizeEstimates() throws ExecutionException;

   void cleanupSizeEstimates();

   void forceKeyspaceCompaction(boolean var1, String var2, String... var3) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int relocateSSTables(String var1, String... var2) throws IOException, ExecutionException, InterruptedException;

   int relocateSSTables(int var1, String var2, String... var3) throws IOException, ExecutionException, InterruptedException;

   void forceKeyspaceCompactionForTokenRange(String var1, String var2, String var3, String... var4) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int forceKeyspaceCleanup(String var1, String... var2) throws IOException, ExecutionException, InterruptedException;

   int forceKeyspaceCleanup(int var1, String var2, String... var3) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int scrub(boolean var1, boolean var2, String var3, String... var4) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int scrub(boolean var1, boolean var2, boolean var3, String var4, String... var5) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int scrub(boolean var1, boolean var2, boolean var3, int var4, String var5, String... var6) throws IOException, ExecutionException, InterruptedException;

   int scrub(boolean var1, boolean var2, boolean var3, boolean var4, int var5, String var6, String... var7) throws IOException, ExecutionException, InterruptedException;

   int verify(boolean var1, String var2, String... var3) throws IOException, ExecutionException, InterruptedException;

   /** @deprecated */
   @Deprecated
   int upgradeSSTables(String var1, boolean var2, String... var3) throws IOException, ExecutionException, InterruptedException;

   int upgradeSSTables(String var1, boolean var2, int var3, String... var4) throws IOException, ExecutionException, InterruptedException;

   int garbageCollect(String var1, int var2, String var3, String... var4) throws IOException, ExecutionException, InterruptedException;

   void forceKeyspaceFlush(String var1, String... var2) throws IOException, ExecutionException, InterruptedException;

   int repairAsync(String var1, Map<String, String> var2);

   void forceTerminateAllRepairSessions();

   @Nullable
   List<String> getParentRepairStatus(int var1);

   void decommission(boolean var1) throws InterruptedException;

   void move(String var1) throws IOException;

   void removeNode(String var1);

   String getRemovalStatus();

   void forceRemoveCompletion();

   void setLoggingLevel(String var1, String var2) throws Exception;

   Map<String, String> getLoggingLevels();

   String getOperationMode();

   boolean isStarting();

   String getDrainProgress();

   void drain() throws IOException, InterruptedException, ExecutionException;

   void truncate(String var1, String var2) throws TimeoutException, IOException;

   Map<InetAddress, Float> getOwnership();

   Map<InetAddress, Float> effectiveOwnership(String var1) throws IllegalStateException;

   List<String> getKeyspaces();

   List<String> getNonSystemKeyspaces();

   List<String> getNonLocalStrategyKeyspaces();

   Map<String, List<String>> getKeyspacesAndViews();

   Map<String, String> getViewBuildStatuses(String var1, String var2);

   Map<String, Map<String, String>> getTableInfos(String var1, String... var2);

   void updateSnitch(String var1, Boolean var2, Integer var3, Integer var4, Double var5) throws ClassNotFoundException;

   void setDynamicUpdateInterval(int var1);

   int getDynamicUpdateInterval();

   String getBatchlogEndpointStrategy();

   void setBatchlogEndpointStrategy(String var1);

   void stopGossiping();

   void startGossiping();

   boolean isGossipRunning();

   void stopDaemon();

   boolean isInitialized();

   void stopNativeTransport();

   void startNativeTransport();

   boolean isNativeTransportRunning();

   void joinRing() throws IOException;

   boolean isJoined();

   boolean isDrained();

   boolean isDraining();

   void setRpcTimeout(long var1);

   long getRpcTimeout();

   void setReadRpcTimeout(long var1);

   long getReadRpcTimeout();

   void setRangeRpcTimeout(long var1);

   long getRangeRpcTimeout();

   void setWriteRpcTimeout(long var1);

   long getWriteRpcTimeout();

   void setCounterWriteRpcTimeout(long var1);

   long getCounterWriteRpcTimeout();

   void setCasContentionTimeout(long var1);

   long getCasContentionTimeout();

   void setTruncateRpcTimeout(long var1);

   long getTruncateRpcTimeout();

   void setStreamThroughputMbPerSec(int var1);

   int getStreamThroughputMbPerSec();

   void setStreamingConnectionsPerHost(int var1);

   int getStreamingConnectionsPerHost();

   void setInterDCStreamThroughputMbPerSec(int var1);

   int getInterDCStreamThroughputMbPerSec();

   int getCompactionThroughputMbPerSec();

   void setCompactionThroughputMbPerSec(int var1);

   int getBatchlogReplayThrottleInKB();

   void setBatchlogReplayThrottleInKB(int var1);

   int getConcurrentCompactors();

   void setConcurrentCompactors(int var1);

   int getConcurrentValidators();

   void setConcurrentValidators(int var1);

   int getConcurrentViewBuilders();

   void setConcurrentViewBuilders(int var1);

   boolean isIncrementalBackupsEnabled();

   void setIncrementalBackupsEnabled(boolean var1);

   void rebuild(String var1);

   void abortRebuild(String var1);

   void rebuild(String var1, String var2, String var3, String var4);

   String rebuild(List<String> var1, String var2, String var3, List<String> var4, List<String> var5, List<String> var6, List<String> var7);

   String rebuild(List<String> var1, String var2, String var3, int var4, List<String> var5, List<String> var6, List<String> var7, List<String> var8);

   void bulkLoad(String var1);

   String bulkLoadAsync(String var1);

   void rescheduleFailedDeletions();

   void loadNewSSTables(String var1, String var2, boolean var3);

   List<String> sampleKeyRange();

   void rebuildSecondaryIndex(String var1, String var2, String... var3);

   void resetLocalSchema() throws IOException;

   void reloadLocalSchema();

   void setTraceProbability(double var1);

   double getTraceProbability();

   void disableAutoCompaction(String var1, String... var2) throws IOException;

   void enableAutoCompaction(String var1, String... var2) throws IOException;

   Map<String, Boolean> getAutoCompactionStatus(String var1, String... var2) throws IOException;

   void deliverHints(String var1) throws UnknownHostException;

   String getClusterName();

   String getPartitionerName();

   int getTombstoneWarnThreshold();

   void setTombstoneWarnThreshold(int var1);

   int getTombstoneFailureThreshold();

   void setTombstoneFailureThreshold(int var1);

   int getBatchSizeFailureThreshold();

   void setBatchSizeFailureThreshold(int var1);

   int getBatchSizeWarnThreshold();

   void setBatchSizeWarnThreshold(int var1);

   void setHintedHandoffThrottleInKB(int var1);

   long getPid();

   boolean resumeBootstrap();

   int forceMarkAllSSTablesAsUnrepaired(String var1, String... var2) throws IOException;
}
