package org.apache.cassandra.tools;

import com.datastax.bdp.db.nodesync.NodeSyncServiceMBean;
import com.datastax.bdp.db.nodesync.NodeSyncServiceProxyMBean;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.ConnectException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationFilter;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.service.TableInfo;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;

public class NodeProbe implements AutoCloseable {
   private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
   private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
   private static final int defaultPort = 7199;
   static long JMX_NOTIFICATION_POLL_INTERVAL_SECONDS;
   public final String host;
   final int port;
   private String username;
   private String password;
   private JMXConnector jmxc;
   private MBeanServerConnection mbeanServerConn;
   private CompactionManagerMBean compactionProxy;
   private StorageServiceMBean ssProxy;
   private GossiperMBean gossProxy;
   private MemoryMXBean memProxy;
   private GCInspectorMXBean gcProxy;
   private RuntimeMXBean runtimeProxy;
   private StreamManagerMBean streamProxy;
   public MessagingServiceMBean msProxy;
   private FailureDetectorMBean fdProxy;
   private CacheServiceMBean cacheService;
   private StorageProxyMBean spProxy;
   private HintedHandOffManagerMBean hhProxy;
   private BatchlogManagerMBean bmProxy;
   private ActiveRepairServiceMBean arsProxy;
   private MemoryOnlyStatusMXBean mosProxy;
   private NodeSyncServiceMBean nodeSyncProxy;
   private NodeSyncServiceProxyMBean nodeSyncRemoteProxy;
   private boolean failed;

   public NodeProbe(String host, int port, String username, String password) throws IOException {
      assert username != null && !username.isEmpty() && password != null && !password.isEmpty() : "neither username nor password can be blank";

      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.connect();
   }

   public NodeProbe(String host, int port) throws IOException {
      this.host = host;
      this.port = port;
      this.connect();
   }

   public NodeProbe(String host) throws IOException {
      this.host = host;
      this.port = 7199;
      this.connect();
   }

   private void connect() throws IOException {
      JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi", new Object[]{this.host, Integer.valueOf(this.port)}));
      Map<String, Object> env = new HashMap();
      if(this.username != null) {
         String[] creds = new String[]{this.username, this.password};
         env.put("jmx.remote.credentials", creds);
      }

      env.put("com.sun.jndi.rmi.factory.socket", this.getRMIClientSocketFactory());
      this.jmxc = JMXConnectorFactory.connect(jmxUrl, env);
      this.mbeanServerConn = this.jmxc.getMBeanServerConnection();

      try {
         ObjectName name = new ObjectName("org.apache.cassandra.db:type=StorageService");
         this.ssProxy = (StorageServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, StorageServiceMBean.class);
         name = new ObjectName("org.apache.cassandra.net:type=MessagingService");
         this.msProxy = (MessagingServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, MessagingServiceMBean.class);
         name = new ObjectName("org.apache.cassandra.net:type=StreamManager");
         this.streamProxy = (StreamManagerMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, StreamManagerMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=CompactionManager");
         this.compactionProxy = (CompactionManagerMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, CompactionManagerMBean.class);
         name = new ObjectName("org.apache.cassandra.net:type=FailureDetector");
         this.fdProxy = (FailureDetectorMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, FailureDetectorMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=Caches");
         this.cacheService = (CacheServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, CacheServiceMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=StorageProxy");
         this.spProxy = (StorageProxyMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, StorageProxyMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=HintedHandoffManager");
         this.hhProxy = (HintedHandOffManagerMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, HintedHandOffManagerMBean.class);
         name = new ObjectName("org.apache.cassandra.service:type=GCInspector");
         this.gcProxy = (GCInspectorMXBean)JMX.newMBeanProxy(this.mbeanServerConn, name, GCInspectorMXBean.class);
         name = new ObjectName("org.apache.cassandra.net:type=Gossiper");
         this.gossProxy = (GossiperMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, GossiperMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=BatchlogManager");
         this.bmProxy = (BatchlogManagerMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, BatchlogManagerMBean.class);
         name = new ObjectName("org.apache.cassandra.db:type=RepairService");
         this.arsProxy = (ActiveRepairServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, ActiveRepairServiceMBean.class);
         name = new ObjectName(NodeSyncServiceMBean.MBEAN_NAME);
         this.nodeSyncProxy = (NodeSyncServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, NodeSyncServiceMBean.class);
         name = new ObjectName(NodeSyncServiceProxyMBean.MBEAN_NAME);
         this.nodeSyncRemoteProxy = (NodeSyncServiceProxyMBean)JMX.newMBeanProxy(this.mbeanServerConn, name, NodeSyncServiceProxyMBean.class);
         this.mosProxy = (MemoryOnlyStatusMXBean)JMX.newMXBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.db:type=MemoryOnlyStatus"), MemoryOnlyStatusMXBean.class);
      } catch (MalformedObjectNameException var4) {
         throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", var4);
      }

      this.memProxy = (MemoryMXBean)ManagementFactory.newPlatformMXBeanProxy(this.mbeanServerConn, "java.lang:type=Memory", MemoryMXBean.class);
      this.runtimeProxy = (RuntimeMXBean)ManagementFactory.newPlatformMXBeanProxy(this.mbeanServerConn, "java.lang:type=Runtime", RuntimeMXBean.class);
   }

   private RMIClientSocketFactory getRMIClientSocketFactory() {
      return (RMIClientSocketFactory)(Boolean.parseBoolean(System.getProperty("ssl.enable"))?new SslRMIClientSocketFactory():RMISocketFactory.getDefaultSocketFactory());
   }

   public void close() throws IOException {
      try {
         this.jmxc.close();
      } catch (ConnectException var2) {
         System.out.println("Cassandra has shutdown.");
      }

   }

   public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.ssProxy.forceKeyspaceCleanup(jobs, keyspaceName, tables);
   }

   public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.ssProxy.scrub(disableSnapshot, skipCorrupted, checkData, reinsertOverflowedTTL, jobs, keyspaceName, tables);
   }

   public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      return this.ssProxy.verify(extendedVerify, keyspaceName, tableNames);
   }

   public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      return this.ssProxy.upgradeSSTables(keyspaceName, excludeCurrentVersion, jobs, tableNames);
   }

   public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      return this.ssProxy.garbageCollect(tombstoneOption, jobs, keyspaceName, tableNames);
   }

   private void checkJobs(PrintStream out, int jobs) {
      int concurrentCompactors = this.ssProxy.getConcurrentCompactors();
      if(jobs > concurrentCompactors) {
         out.println(String.format("jobs (%d) is bigger than configured concurrent_compactors (%d), using at most %d threads", new Object[]{Integer.valueOf(jobs), Integer.valueOf(concurrentCompactors), Integer.valueOf(concurrentCompactors)}));
      }

   }

   public void forceKeyspaceCleanup(PrintStream out, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      this.checkJobs(out, jobs);
      switch(this.forceKeyspaceCleanup(jobs, keyspaceName, tableNames)) {
      case 1:
         this.failed = true;
         out.println("Aborted cleaning up at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
         break;
      case 2:
         this.failed = true;
         out.println("Failed marking some sstables compacting in keyspace " + keyspaceName + ", check server logs for more information");
      }

   }

   public void scrub(PrintStream out, boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      this.checkJobs(out, jobs);
      switch(this.ssProxy.scrub(disableSnapshot, skipCorrupted, checkData, reinsertOverflowedTTL, jobs, keyspaceName, tables)) {
      case 1:
         this.failed = true;
         out.println("Aborted scrubbing at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
         break;
      case 2:
         this.failed = true;
         out.println("Failed marking some sstables compacting in keyspace " + keyspaceName + ", check server logs for more information");
      }

   }

   public void verify(PrintStream out, boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      switch(this.verify(extendedVerify, keyspaceName, tableNames)) {
      case 1:
         this.failed = true;
         out.println("Aborted verifying at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
         break;
      case 2:
         this.failed = true;
         out.println("Failed marking some sstables compacting in keyspace " + keyspaceName + ", check server logs for more information");
      }

   }

   public void upgradeSSTables(PrintStream out, String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      this.checkJobs(out, jobs);
      switch(this.upgradeSSTables(keyspaceName, excludeCurrentVersion, jobs, tableNames)) {
      case 1:
         this.failed = true;
         out.println("Aborted upgrading sstables for at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
         break;
      case 2:
         this.failed = true;
         out.println("Failed marking some sstables compacting in keyspace " + keyspaceName + ", check server logs for more information");
      }

   }

   public void garbageCollect(PrintStream out, String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      if(this.garbageCollect(tombstoneOption, jobs, keyspaceName, tableNames) != 0) {
         this.failed = true;
         out.println("Aborted garbage collection for at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
      }

   }

   public void forceUserDefinedCompaction(String datafiles) throws IOException, ExecutionException, InterruptedException {
      this.compactionProxy.forceUserDefinedCompaction(datafiles);
   }

   public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      this.ssProxy.forceKeyspaceCompaction(splitOutput, keyspaceName, tableNames);
   }

   public void relocateSSTables(int jobs, String keyspace, String[] cfnames) throws IOException, ExecutionException, InterruptedException {
      this.ssProxy.relocateSSTables(jobs, keyspace, cfnames);
   }

   public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      this.ssProxy.forceKeyspaceCompactionForTokenRange(keyspaceName, startToken, endToken, tableNames);
   }

   public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      this.ssProxy.forceKeyspaceFlush(keyspaceName, tableNames);
   }

   public void repairAsync(PrintStream out, String keyspace, Map<String, String> options) throws IOException {
      RepairRunner runner = new RepairRunner(out, this.ssProxy, keyspace, options);

      try {
         this.jmxc.addConnectionNotificationListener(runner, (NotificationFilter)null, (Object)null);
         this.ssProxy.addNotificationListener(runner, (NotificationFilter)null, (Object)null);
         runner.run();
      } catch (Exception var13) {
         throw new IOException(var13);
      } finally {
         try {
            this.ssProxy.removeNotificationListener(runner);
            this.jmxc.removeConnectionNotificationListener(runner);
         } catch (Throwable var12) {
            out.println("Exception occurred during clean-up. " + var12);
         }

      }

   }

   public Map<TableMetrics.Sampler, CompositeData> getPartitionSample(String ks, String cf, ColumnFamilyStoreMBean cfsProxy, int capacity, int duration, int count, List<TableMetrics.Sampler> samplers) throws OpenDataException {
      Iterator var8 = samplers.iterator();

      while(var8.hasNext()) {
         TableMetrics.Sampler sampler = (TableMetrics.Sampler)var8.next();
         cfsProxy.beginLocalSampling(sampler.name(), capacity);
      }

      Uninterruptibles.sleepUninterruptibly((long)duration, TimeUnit.MILLISECONDS);
      Map<TableMetrics.Sampler, CompositeData> result = Maps.newHashMap();
      Iterator var12 = samplers.iterator();

      while(var12.hasNext()) {
         TableMetrics.Sampler sampler = (TableMetrics.Sampler)var12.next();
         result.put(sampler, cfsProxy.finishLocalSampling(sampler.name(), count));
      }

      return result;
   }

   public void invalidateCounterCache() {
      this.cacheService.invalidateCounterCache();
   }

   public void invalidateKeyCache() {
      this.cacheService.invalidateKeyCache();
   }

   public void invalidateRowCache() {
      this.cacheService.invalidateRowCache();
   }

   public void drain() throws IOException, InterruptedException, ExecutionException {
      this.ssProxy.drain();
   }

   public Map<String, String> getTokenToEndpointMap() {
      return this.ssProxy.getTokenToEndpointMap();
   }

   public List<String> getLiveNodes() {
      return this.ssProxy.getLiveNodes();
   }

   public List<String> getJoiningNodes() {
      return this.ssProxy.getJoiningNodes();
   }

   public List<String> getLeavingNodes() {
      return this.ssProxy.getLeavingNodes();
   }

   public List<String> getMovingNodes() {
      return this.ssProxy.getMovingNodes();
   }

   public List<String> getUnreachableNodes() {
      return this.ssProxy.getUnreachableNodes();
   }

   public Map<String, String> getLoadMap() {
      return this.ssProxy.getLoadMap();
   }

   public Map<InetAddress, Float> getOwnership() {
      return this.ssProxy.getOwnership();
   }

   public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException {
      return this.ssProxy.effectiveOwnership(keyspace);
   }

   public MBeanServerConnection getMbeanServerConn() {
      return this.mbeanServerConn;
   }

   public CacheServiceMBean getCacheServiceMBean() {
      String cachePath = "org.apache.cassandra.db:type=Caches";

      try {
         return (CacheServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName(cachePath), CacheServiceMBean.class);
      } catch (MalformedObjectNameException var3) {
         throw new RuntimeException(var3);
      }
   }

   public double[] getAndResetGCStats() {
      return this.gcProxy.getAndResetStats();
   }

   public Iterator<Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies() {
      try {
         return new ColumnFamilyStoreMBeanIterator(this.mbeanServerConn);
      } catch (MalformedObjectNameException var2) {
         throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", var2);
      } catch (IOException var3) {
         throw new RuntimeException("Could not retrieve list of stat mbeans.", var3);
      }
   }

   public CompactionManagerMBean getCompactionManagerProxy() {
      return this.compactionProxy;
   }

   public List<String> getTokens() {
      return this.ssProxy.getTokens();
   }

   public List<String> getTokens(String endpoint) {
      try {
         return this.ssProxy.getTokens(endpoint);
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getLocalHostId() {
      return this.ssProxy.getLocalHostId();
   }

   public Map<String, String> getHostIdMap() {
      return this.ssProxy.getEndpointToHostId();
   }

   public String getLoadString() {
      return this.ssProxy.getLoadString();
   }

   public String getReleaseVersion() {
      return this.ssProxy.getReleaseVersion();
   }

   public int getCurrentGenerationNumber() {
      return this.ssProxy.getCurrentGenerationNumber();
   }

   public long getUptime() {
      return this.runtimeProxy.getUptime();
   }

   public MemoryUsage getHeapMemoryUsage() {
      return this.memProxy.getHeapMemoryUsage();
   }

   public void takeSnapshot(String snapshotName, String table, Map<String, String> options, String... keyspaces) throws IOException {
      if(table != null) {
         if(keyspaces.length != 1) {
            throw new IOException("When specifying the table for a snapshot, you must specify one and only one keyspace");
         }

         this.ssProxy.takeSnapshot(snapshotName, options, new String[]{keyspaces[0] + "." + table});
      } else {
         this.ssProxy.takeSnapshot(snapshotName, options, keyspaces);
      }

   }

   public void takeMultipleTableSnapshot(String snapshotName, Map<String, String> options, String... tableList) throws IOException {
      if(null != tableList && tableList.length != 0) {
         this.ssProxy.takeSnapshot(snapshotName, options, tableList);
      } else {
         throw new IOException("The column family List  for a snapshot should not be empty or null");
      }
   }

   public void clearSnapshot(String tag, String... keyspaces) throws IOException {
      this.ssProxy.clearSnapshot(tag, keyspaces);
   }

   public Map<String, TabularData> getSnapshotDetails() {
      return this.ssProxy.getSnapshotDetails();
   }

   public long trueSnapshotsSize() {
      return this.ssProxy.trueSnapshotsSize();
   }

   public boolean isJoined() {
      return this.ssProxy.isJoined();
   }

   public boolean isDrained() {
      return this.ssProxy.isDrained();
   }

   public boolean isDraining() {
      return this.ssProxy.isDraining();
   }

   public void joinRing() throws IOException {
      this.ssProxy.joinRing();
   }

   public void decommission(boolean force) throws InterruptedException {
      this.ssProxy.decommission(force);
   }

   public void move(String newToken) throws IOException {
      this.ssProxy.move(newToken);
   }

   public void removeNode(String token) {
      this.ssProxy.removeNode(token);
   }

   public String getRemovalStatus() {
      return this.ssProxy.getRemovalStatus();
   }

   public void forceRemoveCompletion() {
      this.ssProxy.forceRemoveCompletion();
   }

   public void assassinateEndpoint(String address) throws UnknownHostException {
      this.gossProxy.assassinateEndpoint(address);
   }

   public List<String> reloadSeeds() {
      return this.gossProxy.reloadSeeds();
   }

   public List<String> getSeeds() {
      return this.gossProxy.getSeeds();
   }

   public void setCompactionThreshold(String ks, String cf, int minimumCompactionThreshold, int maximumCompactionThreshold) {
      ColumnFamilyStoreMBean cfsProxy = this.getCfsProxy(ks, cf);
      cfsProxy.setCompactionThresholds(minimumCompactionThreshold, maximumCompactionThreshold);
   }

   public void disableAutoCompaction(String ks, String... tables) throws IOException {
      this.ssProxy.disableAutoCompaction(ks, tables);
   }

   public void enableAutoCompaction(String ks, String... tableNames) throws IOException {
      this.ssProxy.enableAutoCompaction(ks, tableNames);
   }

   public Map<String, Boolean> getAutoCompactionDisabled(String ks, String... tableNames) throws IOException {
      return this.ssProxy.getAutoCompactionStatus(ks, tableNames);
   }

   public void setIncrementalBackupsEnabled(boolean enabled) {
      this.ssProxy.setIncrementalBackupsEnabled(enabled);
   }

   public boolean isIncrementalBackupsEnabled() {
      return this.ssProxy.isIncrementalBackupsEnabled();
   }

   public void setCacheCapacities(int keyCacheCapacity, int rowCacheCapacity, int counterCacheCapacity) {
      try {
         String keyCachePath = "org.apache.cassandra.db:type=Caches";
         CacheServiceMBean cacheMBean = (CacheServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName(keyCachePath), CacheServiceMBean.class);
         cacheMBean.setKeyCacheCapacityInMB((long)keyCacheCapacity);
         cacheMBean.setRowCacheCapacityInMB((long)rowCacheCapacity);
         cacheMBean.setCounterCacheCapacityInMB((long)counterCacheCapacity);
      } catch (MalformedObjectNameException var6) {
         throw new RuntimeException(var6);
      }
   }

   public void setCacheKeysToSave(int keyCacheKeysToSave, int rowCacheKeysToSave, int counterCacheKeysToSave) {
      try {
         String keyCachePath = "org.apache.cassandra.db:type=Caches";
         CacheServiceMBean cacheMBean = (CacheServiceMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName(keyCachePath), CacheServiceMBean.class);
         cacheMBean.setKeyCacheKeysToSave(keyCacheKeysToSave);
         cacheMBean.setRowCacheKeysToSave(rowCacheKeysToSave);
         cacheMBean.setCounterCacheKeysToSave(counterCacheKeysToSave);
      } catch (MalformedObjectNameException var6) {
         throw new RuntimeException(var6);
      }
   }

   public void setHintedHandoffThrottleInKB(int throttleInKB) {
      this.ssProxy.setHintedHandoffThrottleInKB(throttleInKB);
   }

   public List<InetAddress> getEndpoints(String keyspace, String cf, String key) {
      return this.ssProxy.getNaturalEndpoints(keyspace, cf, key);
   }

   public List<String> getSSTables(String keyspace, String cf, String key, boolean hexFormat) {
      ColumnFamilyStoreMBean cfsProxy = this.getCfsProxy(keyspace, cf);
      return cfsProxy.getSSTablesForKey(key, hexFormat);
   }

   public Set<StreamState> getStreamStatus() {
      return Sets.newHashSet(Iterables.transform(this.streamProxy.getCurrentStreams(), new Function<CompositeData, StreamState>() {
         public StreamState apply(CompositeData input) {
            return StreamStateCompositeData.fromCompositeData(input);
         }
      }));
   }

   public String getOperationMode() {
      return this.ssProxy.getOperationMode();
   }

   public boolean isStarting() {
      return this.ssProxy.isStarting();
   }

   public void truncate(String keyspaceName, String tableName) {
      try {
         this.ssProxy.truncate(keyspaceName, tableName);
      } catch (TimeoutException var4) {
         throw new RuntimeException("Error while executing truncate", var4);
      } catch (IOException var5) {
         throw new RuntimeException("Error while executing truncate", var5);
      }
   }

   public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy() {
      try {
         return (EndpointSnitchInfoMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo"), EndpointSnitchInfoMBean.class);
      } catch (MalformedObjectNameException var2) {
         throw new RuntimeException(var2);
      }
   }

   public DynamicEndpointSnitchMBean getDynamicEndpointSnitchInfoProxy() {
      try {
         return (DynamicEndpointSnitchMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.db:type=DynamicEndpointSnitch"), DynamicEndpointSnitchMBean.class);
      } catch (MalformedObjectNameException var2) {
         throw new RuntimeException(var2);
      }
   }

   public ColumnFamilyStoreMBean getCfsProxy(String ks, String cf) {
      ColumnFamilyStoreMBean cfsProxy = null;

      try {
         String type = cf.contains(".")?"IndexColumnFamilies":"ColumnFamilies";
         Set<ObjectName> beans = this.mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.db:type=*" + type + ",keyspace=" + ks + ",columnfamily=" + cf), (QueryExp)null);
         if(beans.isEmpty()) {
            throw new MalformedObjectNameException("couldn't find that bean");
         }

         assert beans.size() == 1;

         ObjectName bean;
         for(Iterator var6 = beans.iterator(); var6.hasNext(); cfsProxy = (ColumnFamilyStoreMBean)JMX.newMBeanProxy(this.mbeanServerConn, bean, ColumnFamilyStoreMBean.class)) {
            bean = (ObjectName)var6.next();
         }
      } catch (MalformedObjectNameException var8) {
         System.err.println("ColumnFamilyStore for " + ks + "/" + cf + " not found.");
         System.exit(1);
      } catch (IOException var9) {
         System.err.println("ColumnFamilyStore for " + ks + "/" + cf + " not found: " + var9);
         System.exit(1);
      }

      return cfsProxy;
   }

   public StorageProxyMBean getSpProxy() {
      return this.spProxy;
   }

   public String getEndpoint() {
      Map<String, String> hostIdToEndpoint = this.ssProxy.getHostIdToEndpoint();
      return (String)hostIdToEndpoint.get(this.ssProxy.getLocalHostId());
   }

   public String getDataCenter() {
      return this.getEndpointSnitchInfoProxy().getDatacenter();
   }

   public String getRack() {
      return this.getEndpointSnitchInfoProxy().getRack();
   }

   public List<String> getKeyspaces() {
      return this.ssProxy.getKeyspaces();
   }

   public Map<String, TableInfo> getTableInfos(String keyspace, String... tables) {
      Map<String, Map<String, String>> tableInfosAsMap = this.ssProxy.getTableInfos(keyspace, tables);
      Map<String, TableInfo> tableInfo = new HashMap();
      tableInfosAsMap.entrySet().stream().forEach((e) -> {
         TableInfo var10000 = (TableInfo)tableInfo.put(e.getKey(), TableInfo.fromMap((Map)e.getValue()));
      });
      return tableInfo;
   }

   public Map<String, List<String>> getKeyspacesAndViews() {
      return this.ssProxy.getKeyspacesAndViews();
   }

   public List<String> getNonSystemKeyspaces() {
      return this.ssProxy.getNonSystemKeyspaces();
   }

   public List<String> getNonLocalStrategyKeyspaces() {
      return this.ssProxy.getNonLocalStrategyKeyspaces();
   }

   public String getClusterName() {
      return this.ssProxy.getClusterName();
   }

   public String getPartitioner() {
      return this.ssProxy.getPartitionerName();
   }

   public void disableHintedHandoff() {
      this.spProxy.setHintedHandoffEnabled(false);
   }

   public void enableHintedHandoff() {
      this.spProxy.setHintedHandoffEnabled(true);
   }

   public boolean isHandoffEnabled() {
      return this.spProxy.getHintedHandoffEnabled();
   }

   public void enableHintsForDC(String dc) {
      this.spProxy.enableHintsForDC(dc);
   }

   public void disableHintsForDC(String dc) {
      this.spProxy.disableHintsForDC(dc);
   }

   public Set<String> getHintedHandoffDisabledDCs() {
      return this.spProxy.getHintedHandoffDisabledDCs();
   }

   public Map<String, String> getViewBuildStatuses(String keyspace, String view) {
      return this.ssProxy.getViewBuildStatuses(keyspace, view);
   }

   public void pauseHintsDelivery() {
      this.hhProxy.pauseHintsDelivery(true);
   }

   public void resumeHintsDelivery() {
      this.hhProxy.pauseHintsDelivery(false);
   }

   public void truncateHints(String host) {
      this.hhProxy.deleteHintsForEndpoint(host);
   }

   public void truncateHints() {
      try {
         this.hhProxy.truncateAllHints();
      } catch (InterruptedException | ExecutionException var2) {
         throw new RuntimeException("Error while executing truncate hints", var2);
      }
   }

   public void refreshSizeEstimates() {
      try {
         this.ssProxy.refreshSizeEstimates();
      } catch (ExecutionException var2) {
         throw new RuntimeException("Error while refreshing system.size_estimates", var2);
      }
   }

   public void stopNativeTransport() {
      this.ssProxy.stopNativeTransport();
   }

   public void startNativeTransport() {
      this.ssProxy.startNativeTransport();
   }

   public boolean isNativeTransportRunning() {
      return this.ssProxy.isNativeTransportRunning();
   }

   public void stopGossiping() {
      this.ssProxy.stopGossiping();
   }

   public void startGossiping() {
      this.ssProxy.startGossiping();
   }

   public boolean isGossipRunning() {
      return this.ssProxy.isGossipRunning();
   }

   public void stopCassandraDaemon() {
      this.ssProxy.stopDaemon();
   }

   public boolean isInitialized() {
      return this.ssProxy.isInitialized();
   }

   public void setCompactionThroughput(int value) {
      this.ssProxy.setCompactionThroughputMbPerSec(value);
   }

   public int getCompactionThroughput() {
      return this.ssProxy.getCompactionThroughputMbPerSec();
   }

   public void setBatchlogReplayThrottle(int value) {
      this.ssProxy.setBatchlogReplayThrottleInKB(value);
   }

   public int getBatchlogReplayThrottle() {
      return this.ssProxy.getBatchlogReplayThrottleInKB();
   }

   public void setConcurrentCompactors(int value) {
      this.ssProxy.setConcurrentCompactors(value);
   }

   public int getConcurrentCompactors() {
      return this.ssProxy.getConcurrentCompactors();
   }

   public void setConcurrentViewBuilders(int value) {
      this.ssProxy.setConcurrentViewBuilders(value);
   }

   public int getConcurrentViewBuilders() {
      return this.ssProxy.getConcurrentViewBuilders();
   }

   public void setMaxHintWindow(int value) {
      this.spProxy.setMaxHintWindow(value);
   }

   public int getMaxHintWindow() {
      return this.spProxy.getMaxHintWindow();
   }

   public long getTimeout(String type) {
      byte var3 = -1;
      switch(type.hashCode()) {
      case -1644339357:
         if(type.equals("counterwrite")) {
            var3 = 4;
         }
         break;
      case 3351788:
         if(type.equals("misc")) {
            var3 = 0;
         }
         break;
      case 3496342:
         if(type.equals("read")) {
            var3 = 1;
         }
         break;
      case 108280125:
         if(type.equals("range")) {
            var3 = 2;
         }
         break;
      case 113399775:
         if(type.equals("write")) {
            var3 = 3;
         }
         break;
      case 427581060:
         if(type.equals("cascontention")) {
            var3 = 5;
         }
         break;
      case 1852984678:
         if(type.equals("truncate")) {
            var3 = 6;
         }
      }

      switch(var3) {
      case 0:
         return this.ssProxy.getRpcTimeout();
      case 1:
         return this.ssProxy.getReadRpcTimeout();
      case 2:
         return this.ssProxy.getRangeRpcTimeout();
      case 3:
         return this.ssProxy.getWriteRpcTimeout();
      case 4:
         return this.ssProxy.getCounterWriteRpcTimeout();
      case 5:
         return this.ssProxy.getCasContentionTimeout();
      case 6:
         return this.ssProxy.getTruncateRpcTimeout();
      default:
         throw new RuntimeException("Timeout type requires one of (read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms))");
      }
   }

   public int getStreamThroughput() {
      return this.ssProxy.getStreamThroughputMbPerSec();
   }

   public int getStreamingConnectionsPerHost() {
      return this.ssProxy.getStreamingConnectionsPerHost();
   }

   public int getInterDCStreamThroughput() {
      return this.ssProxy.getInterDCStreamThroughputMbPerSec();
   }

   public double getTraceProbability() {
      return this.ssProxy.getTraceProbability();
   }

   public int getExceptionCount() {
      return (int)StorageMetrics.uncaughtExceptions.getCount();
   }

   public Map<String, Integer> getDroppedMessages() {
      return this.msProxy.getDroppedMessages();
   }

   public void loadNewSSTables(String ksName, String cfName, boolean resetLevels) {
      this.ssProxy.loadNewSSTables(ksName, cfName, resetLevels);
   }

   public void rebuildIndex(String ksName, String cfName, String... idxNames) {
      this.ssProxy.rebuildSecondaryIndex(ksName, cfName, idxNames);
   }

   public String getGossipInfo() {
      return this.fdProxy.getAllEndpointStates();
   }

   public void stop(String string) {
      this.compactionProxy.stopCompaction(string);
   }

   public void setTimeout(String type, long value) {
      if(value < 0L) {
         throw new RuntimeException("timeout must be non-negative");
      } else {
         byte var5 = -1;
         switch(type.hashCode()) {
         case -1644339357:
            if(type.equals("counterwrite")) {
               var5 = 4;
            }
            break;
         case 3351788:
            if(type.equals("misc")) {
               var5 = 0;
            }
            break;
         case 3496342:
            if(type.equals("read")) {
               var5 = 1;
            }
            break;
         case 108280125:
            if(type.equals("range")) {
               var5 = 2;
            }
            break;
         case 113399775:
            if(type.equals("write")) {
               var5 = 3;
            }
            break;
         case 427581060:
            if(type.equals("cascontention")) {
               var5 = 5;
            }
            break;
         case 1852984678:
            if(type.equals("truncate")) {
               var5 = 6;
            }
         }

         switch(var5) {
         case 0:
            this.ssProxy.setRpcTimeout(value);
            break;
         case 1:
            this.ssProxy.setReadRpcTimeout(value);
            break;
         case 2:
            this.ssProxy.setRangeRpcTimeout(value);
            break;
         case 3:
            this.ssProxy.setWriteRpcTimeout(value);
            break;
         case 4:
            this.ssProxy.setCounterWriteRpcTimeout(value);
            break;
         case 5:
            this.ssProxy.setCasContentionTimeout(value);
            break;
         case 6:
            this.ssProxy.setTruncateRpcTimeout(value);
            break;
         default:
            throw new RuntimeException("Timeout type requires one of (read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms))");
         }

      }
   }

   public void stopById(String compactionId) {
      this.compactionProxy.stopCompactionById(compactionId);
   }

   public void setStreamThroughput(int value) {
      this.ssProxy.setStreamThroughputMbPerSec(value);
   }

   public void setInterDCStreamThroughput(int value) {
      this.ssProxy.setInterDCStreamThroughputMbPerSec(value);
   }

   public void setTraceProbability(double value) {
      this.ssProxy.setTraceProbability(value);
   }

   public String getSchemaVersion() {
      return this.ssProxy.getSchemaVersion();
   }

   public List<String> describeRing(String keyspaceName) throws IOException {
      return this.ssProxy.describeRingJMX(keyspaceName);
   }

   public String rebuild(List<String> keyspaces, String tokens, String mode, int streamingConnectionsPerHost, List<String> whitelistDcs, List<String> blacklistDcs, List<String> whitelistSources, List<String> blacklistSources) {
      return this.ssProxy.rebuild(keyspaces, tokens, mode, streamingConnectionsPerHost, whitelistDcs, blacklistDcs, whitelistSources, blacklistSources);
   }

   public void abortRebuild(String reason) {
      this.ssProxy.abortRebuild(reason);
   }

   public List<String> sampleKeyRange() {
      return this.ssProxy.sampleKeyRange();
   }

   public void resetLocalSchema() throws IOException {
      this.ssProxy.resetLocalSchema();
   }

   public void reloadLocalSchema() {
      this.ssProxy.reloadLocalSchema();
   }

   public boolean isFailed() {
      return this.failed;
   }

   public void failed() {
      this.failed = true;
   }

   public void clearFailed() {
      this.failed = false;
   }

   public long getReadRepairAttempted() {
      return this.spProxy.getReadRepairAttempted();
   }

   public long getReadRepairRepairedBlocking() {
      return this.spProxy.getReadRepairRepairedBlocking();
   }

   public long getReadRepairRepairedBackground() {
      return this.spProxy.getReadRepairRepairedBackground();
   }

   public Object getCacheMetric(String cacheType, String metricName) {
      try {
         byte var4 = -1;
         switch(metricName.hashCode()) {
         case -1990013238:
            if(metricName.equals("Misses")) {
               var4 = 6;
            }
            break;
         case -1703482637:
            if(metricName.equals("HitRate")) {
               var4 = 2;
            }
            break;
         case -1646235418:
            if(metricName.equals("MissLatencyUnit")) {
               var4 = 8;
            }
            break;
         case -1606413246:
            if(metricName.equals("MissLatency")) {
               var4 = 7;
            }
            break;
         case -328612892:
            if(metricName.equals("Requests")) {
               var4 = 4;
            }
            break;
         case -3180326:
            if(metricName.equals("Capacity")) {
               var4 = 0;
            }
            break;
         case 2249568:
            if(metricName.equals("Hits")) {
               var4 = 5;
            }
            break;
         case 2577441:
            if(metricName.equals("Size")) {
               var4 = 3;
            }
            break;
         case 73079920:
            if(metricName.equals("Entries")) {
               var4 = 1;
            }
         }

         switch(var4) {
         case 0:
         case 1:
         case 2:
         case 3:
            return ((CassandraMetricsRegistry.JmxGaugeMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName), CassandraMetricsRegistry.JmxGaugeMBean.class)).getValue();
         case 4:
         case 5:
         case 6:
            return Long.valueOf(((CassandraMetricsRegistry.JmxMeterMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName), CassandraMetricsRegistry.JmxMeterMBean.class)).getCount());
         case 7:
            return Double.valueOf(((CassandraMetricsRegistry.JmxTimerMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName), CassandraMetricsRegistry.JmxTimerMBean.class)).getMean());
         case 8:
            return ((CassandraMetricsRegistry.JmxTimerMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=MissLatency"), CassandraMetricsRegistry.JmxTimerMBean.class)).getDurationUnit();
         default:
            throw new RuntimeException("Unknown cache metric name.");
         }
      } catch (MalformedObjectNameException var5) {
         throw new RuntimeException(var5);
      }
   }

   public Object getThreadPoolMetric(String pathName, String poolName, String metricName) {
      return ThreadPoolMetrics.getJmxMetric(this.mbeanServerConn, pathName, poolName, metricName);
   }

   public Multimap<String, String> getThreadPools() {
      return ThreadPoolMetrics.getJmxThreadPools(this.mbeanServerConn);
   }

   public int getNumberOfTables() {
      return this.spProxy.getNumberOfTables();
   }

   public Object getColumnFamilyMetric(String ks, String cf, String metricName) {
      try {
         ObjectName oName = null;
         if(!Strings.isNullOrEmpty(ks) && !Strings.isNullOrEmpty(cf)) {
            String type = cf.contains(".")?"IndexTable":"Table";
            oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=%s,keyspace=%s,scope=%s,name=%s", new Object[]{type, ks, cf, metricName}));
         } else if(!Strings.isNullOrEmpty(ks)) {
            oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=%s", new Object[]{ks, metricName}));
         } else {
            oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=Table,name=%s", new Object[]{metricName}));
         }

         byte var6 = -1;
         switch(metricName.hashCode()) {
         case -1951202055:
            if(metricName.equals("MinPartitionSize")) {
               var6 = 17;
            }
            break;
         case -1887333593:
            if(metricName.equals("MaxPartitionSize")) {
               var6 = 12;
            }
            break;
         case -1867190954:
            if(metricName.equals("SSTablesPerReadHistogram")) {
               var6 = 38;
            }
            break;
         case -1866587829:
            if(metricName.equals("BloomFilterDiskSpaceUsed")) {
               var6 = 0;
            }
            break;
         case -1763863388:
            if(metricName.equals("CompressionMetadataOffHeapMemoryUsed")) {
               var6 = 5;
            }
            break;
         case -1667508752:
            if(metricName.equals("ReadTotalLatency")) {
               var6 = 30;
            }
            break;
         case -1557926608:
            if(metricName.equals("BytesUnrepaired")) {
               var6 = 20;
            }
            break;
         case -1477475695:
            if(metricName.equals("CoordinatorScanLatency")) {
               var6 = 34;
            }
            break;
         case -1465837505:
            if(metricName.equals("WriteLatency")) {
               var6 = 36;
            }
            break;
         case -1370758488:
            if(metricName.equals("RecentBloomFilterFalseRatio")) {
               var6 = 23;
            }
            break;
         case -1114239206:
            if(metricName.equals("LiveDiskSpaceUsed")) {
               var6 = 25;
            }
            break;
         case -1006440446:
            if(metricName.equals("TotalDiskSpaceUsed")) {
               var6 = 28;
            }
            break;
         case -667833728:
            if(metricName.equals("LiveScannedHistogram")) {
               var6 = 37;
            }
            break;
         case -463587056:
            if(metricName.equals("KeyCacheHitRate")) {
               var6 = 10;
            }
            break;
         case -393169043:
            if(metricName.equals("LiveSSTableCount")) {
               var6 = 11;
            }
            break;
         case -321785512:
            if(metricName.equals("CoordinatorReadLatency")) {
               var6 = 33;
            }
            break;
         case -238574638:
            if(metricName.equals("BloomFilterFalsePositives")) {
               var6 = 1;
            }
            break;
         case 74042690:
            if(metricName.equals("MemtableSwitchCount")) {
               var6 = 26;
            }
            break;
         case 143897221:
            if(metricName.equals("IndexSummaryOffHeapMemoryUsed")) {
               var6 = 4;
            }
            break;
         case 263309627:
            if(metricName.equals("PendingFlushes")) {
               var6 = 31;
            }
            break;
         case 443978857:
            if(metricName.equals("TombstoneScannedHistogram")) {
               var6 = 39;
            }
            break;
         case 630998566:
            if(metricName.equals("MeanPartitionSize")) {
               var6 = 13;
            }
            break;
         case 701129331:
            if(metricName.equals("SpeculativeRetries")) {
               var6 = 27;
            }
            break;
         case 797304881:
            if(metricName.equals("PercentRepaired")) {
               var6 = 18;
            }
            break;
         case 845374896:
            if(metricName.equals("MemtableLiveDataSize")) {
               var6 = 15;
            }
            break;
         case 877934969:
            if(metricName.equals("BytesPendingRepair")) {
               var6 = 21;
            }
            break;
         case 881876584:
            if(metricName.equals("ReadLatency")) {
               var6 = 35;
            }
            break;
         case 912468547:
            if(metricName.equals("BloomFilterFalseRatio")) {
               var6 = 2;
            }
            break;
         case 1086144931:
            if(metricName.equals("MemtableOffHeapSize")) {
               var6 = 16;
            }
            break;
         case 1109151815:
            if(metricName.equals("EstimatedColumnCountHistogram")) {
               var6 = 7;
            }
            break;
         case 1173199038:
            if(metricName.equals("BloomFilterOffHeapMemoryUsed")) {
               var6 = 3;
            }
            break;
         case 1375251065:
            if(metricName.equals("WriteTotalLatency")) {
               var6 = 29;
            }
            break;
         case 1387870821:
            if(metricName.equals("CompressionRatio")) {
               var6 = 6;
            }
            break;
         case 1539939201:
            if(metricName.equals("EstimatedPartitionCount")) {
               var6 = 9;
            }
            break;
         case 1545839031:
            if(metricName.equals("RecentBloomFilterFalsePositives")) {
               var6 = 22;
            }
            break;
         case 1671916245:
            if(metricName.equals("EstimatedPartitionSizeHistogram")) {
               var6 = 8;
            }
            break;
         case 1935395083:
            if(metricName.equals("MemtableColumnsCount")) {
               var6 = 14;
            }
            break;
         case 1988130602:
            if(metricName.equals("DroppedMutations")) {
               var6 = 32;
            }
            break;
         case 2014525360:
            if(metricName.equals("SnapshotsSize")) {
               var6 = 24;
            }
            break;
         case 2108962391:
            if(metricName.equals("BytesRepaired")) {
               var6 = 19;
            }
         }

         switch(var6) {
         case 0:
         case 1:
         case 2:
         case 3:
         case 4:
         case 5:
         case 6:
         case 7:
         case 8:
         case 9:
         case 10:
         case 11:
         case 12:
         case 13:
         case 14:
         case 15:
         case 16:
         case 17:
         case 18:
         case 19:
         case 20:
         case 21:
         case 22:
         case 23:
         case 24:
            return ((CassandraMetricsRegistry.JmxGaugeMBean)JMX.newMBeanProxy(this.mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class)).getValue();
         case 25:
         case 26:
         case 27:
         case 28:
         case 29:
         case 30:
         case 31:
         case 32:
            return Long.valueOf(((CassandraMetricsRegistry.JmxCounterMBean)JMX.newMBeanProxy(this.mbeanServerConn, oName, CassandraMetricsRegistry.JmxCounterMBean.class)).getCount());
         case 33:
         case 34:
         case 35:
         case 36:
            return JMX.newMBeanProxy(this.mbeanServerConn, oName, CassandraMetricsRegistry.JmxTimerMBean.class);
         case 37:
         case 38:
         case 39:
            return JMX.newMBeanProxy(this.mbeanServerConn, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);
         default:
            throw new RuntimeException("Unknown table metric " + metricName);
         }
      } catch (MalformedObjectNameException var7) {
         throw new RuntimeException(var7);
      }
   }

   public CassandraMetricsRegistry.JmxTimerMBean getProxyMetric(String scope) {
      try {
         return (CassandraMetricsRegistry.JmxTimerMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=ClientRequest,scope=" + scope + ",name=Latency"), CassandraMetricsRegistry.JmxTimerMBean.class);
      } catch (MalformedObjectNameException var3) {
         throw new RuntimeException(var3);
      }
   }

   public CassandraMetricsRegistry.JmxTimerMBean getMessagingQueueWaitMetrics(String verb) {
      try {
         return (CassandraMetricsRegistry.JmxTimerMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:name=" + verb + "-WaitLatency,type=Messaging"), CassandraMetricsRegistry.JmxTimerMBean.class);
      } catch (MalformedObjectNameException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Object getCompactionMetric(String metricName) {
      try {
         byte var3 = -1;
         switch(metricName.hashCode()) {
         case -363499487:
            if(metricName.equals("TotalCompactionsCompleted")) {
               var3 = 4;
            }
            break;
         case -203081717:
            if(metricName.equals("PendingTasksByTableName")) {
               var3 = 3;
            }
            break;
         case 302677751:
            if(metricName.equals("BytesCompacted")) {
               var3 = 0;
            }
            break;
         case 316783703:
            if(metricName.equals("PendingTasks")) {
               var3 = 2;
            }
            break;
         case 551065635:
            if(metricName.equals("CompletedTasks")) {
               var3 = 1;
            }
         }

         switch(var3) {
         case 0:
            return JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName), CassandraMetricsRegistry.JmxCounterMBean.class);
         case 1:
         case 2:
         case 3:
            return ((CassandraMetricsRegistry.JmxGaugeMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName), CassandraMetricsRegistry.JmxGaugeMBean.class)).getValue();
         case 4:
            return JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName), CassandraMetricsRegistry.JmxMeterMBean.class);
         default:
            throw new RuntimeException("Unknown compaction metric.");
         }
      } catch (MalformedObjectNameException var4) {
         throw new RuntimeException(var4);
      }
   }

   public long getStorageMetric(String metricName) {
      try {
         return ((CassandraMetricsRegistry.JmxCounterMBean)JMX.newMBeanProxy(this.mbeanServerConn, new ObjectName("org.apache.cassandra.metrics:type=Storage,name=" + metricName), CassandraMetricsRegistry.JmxCounterMBean.class)).getCount();
      } catch (MalformedObjectNameException var3) {
         throw new RuntimeException(var3);
      }
   }

   public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxHistogramMBean metric) {
      return new double[]{metric.get50thPercentile(), metric.get75thPercentile(), metric.get95thPercentile(), metric.get98thPercentile(), metric.get99thPercentile(), (double)metric.getMin(), (double)metric.getMax()};
   }

   public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxTimerMBean metric) {
      return new double[]{metric.get50thPercentile(), metric.get75thPercentile(), metric.get95thPercentile(), metric.get98thPercentile(), metric.get99thPercentile(), metric.getMin(), metric.getMax()};
   }

   public TabularData getCompactionHistory() {
      return this.compactionProxy.getCompactionHistory();
   }

   public void reloadTriggers() {
      this.spProxy.reloadTriggerClasses();
   }

   public void setLoggingLevel(String classQualifier, String level) {
      try {
         this.ssProxy.setLoggingLevel(classQualifier, level);
      } catch (Exception var4) {
         throw new RuntimeException("Error setting log for " + classQualifier + " on level " + level + ". Please check logback configuration and ensure to have <jmxConfigurator /> set", var4);
      }
   }

   public Map<String, String> getLoggingLevels() {
      return this.ssProxy.getLoggingLevels();
   }

   public void resumeBootstrap(PrintStream out) throws IOException {
      BootstrapMonitor monitor = new BootstrapMonitor(out);

      try {
         this.jmxc.addConnectionNotificationListener(monitor, (NotificationFilter)null, (Object)null);
         this.ssProxy.addNotificationListener(monitor, (NotificationFilter)null, (Object)null);
         if(this.ssProxy.resumeBootstrap()) {
            out.println("Resuming bootstrap");
            monitor.awaitCompletion();
         } else {
            out.println("Node is already bootstrapped.");
         }
      } catch (Exception var11) {
         throw new IOException(var11);
      } finally {
         try {
            this.ssProxy.removeNotificationListener(monitor);
            this.jmxc.removeConnectionNotificationListener(monitor);
         } catch (Throwable var10) {
            out.println("Exception occurred during clean-up. " + var10);
         }

      }

   }

   public void replayBatchlog() throws IOException {
      try {
         this.bmProxy.forceBatchlogReplay();
      } catch (Exception var2) {
         throw new IOException(var2);
      }
   }

   public TabularData getFailureDetectorPhilValues() {
      try {
         return this.fdProxy.getPhiValues();
      } catch (OpenDataException var2) {
         throw new RuntimeException(var2);
      }
   }

   public long getPid() {
      return this.ssProxy.getPid();
   }

   public ActiveRepairServiceMBean getRepairServiceProxy() {
      return this.arsProxy;
   }

   public MemoryOnlyStatusMXBean getMemoryOnlyStatusProxy() {
      return this.mosProxy;
   }

   public boolean enableNodeSync(long timeout, TimeUnit timeoutUnit) throws TimeoutException {
      return this.nodeSyncProxy.enable(timeout, timeoutUnit);
   }

   public boolean disableNodeSync(boolean force, long timeout, TimeUnit timeoutUnit) throws TimeoutException {
      return this.nodeSyncProxy.disable(force, timeout, timeoutUnit);
   }

   public boolean nodeSyncStatus() {
      return this.nodeSyncProxy.isRunning();
   }

   public void setNodeSyncRate(Integer repairRate) {
      this.nodeSyncProxy.setRate(repairRate.intValue());
   }

   public int getNodeSyncRate() {
      return this.nodeSyncProxy.getRate();
   }

   public void startUserValidation(Map<String, String> optionMap) {
      this.nodeSyncProxy.startUserValidation(optionMap);
   }

   public void startUserValidation(String id, String keyspace, String table, String ranges, Integer rateInKB) {
      this.nodeSyncProxy.startUserValidation(id, keyspace, table, ranges, rateInKB);
   }

   public void startUserValidation(InetAddress address, Map<String, String> options) {
      this.nodeSyncRemoteProxy.startUserValidation(address, options);
   }

   public void cancelUserValidation(String idStr) {
      this.nodeSyncProxy.cancelUserValidation(idStr);
   }

   public void cancelUserValidation(InetAddress address, String idStr) {
      this.nodeSyncRemoteProxy.cancelUserValidation(address, idStr);
   }

   public List<Map<String, String>> getNodeSyncRateSimulatorInfo(boolean includeAllTables) {
      return this.nodeSyncProxy.getRateSimulatorInfo(includeAllTables);
   }

   public void markAllSSTablesAsUnrepaired(PrintStream out, String keyspace, String[] tables) throws IOException {
      int marked = this.ssProxy.forceMarkAllSSTablesAsUnrepaired(keyspace, tables);
      if(marked == 0) {
         out.println(String.format("No repaired SSTables to mark as unrepaired.", new Object[]{Integer.valueOf(marked)}));
      } else {
         out.println(String.format("Marked %d SSTable(s) as unrepaired.", new Object[]{Integer.valueOf(marked)}));
      }

   }

   public void enableNodeSyncTracing(Map<String, String> options) {
      this.nodeSyncProxy.enableTracing(options);
   }

   public void enableNodeSyncTracing(InetAddress address, Map<String, String> options) {
      this.nodeSyncRemoteProxy.enableTracing(address, options);
   }

   public void disableNodeSyncTracing() {
      this.nodeSyncProxy.disableTracing();
   }

   public void disableNodeSyncTracing(InetAddress address) {
      this.nodeSyncRemoteProxy.disableTracing(address);
   }

   public UUID currentNodeSyncTracingSession() {
      return this.nodeSyncProxy.currentTracingSession();
   }

   public UUID currentNodeSyncTracingSession(InetAddress address) {
      return this.nodeSyncRemoteProxy.currentTracingSession(address);
   }

   static {
      JMX_NOTIFICATION_POLL_INTERVAL_SECONDS = Long.getLong("cassandra.nodetool.jmx_notification_poll_interval_seconds", TimeUnit.SECONDS.convert(5L, TimeUnit.MINUTES)).longValue();
   }
}
