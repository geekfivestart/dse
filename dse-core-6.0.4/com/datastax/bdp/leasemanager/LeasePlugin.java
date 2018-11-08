package com.datastax.bdp.leasemanager;

import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.IPlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.server.LifecycleAware;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.MapBuilder;
import com.datastax.bdp.util.rpc.Rpc;
import com.datastax.bdp.util.rpc.RpcParam;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.commons.lang3.concurrent.BasicThreadFactory.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {ThreadPoolPlugin.class}
)
public class LeasePlugin extends AbstractPlugin implements LeaseMXBean, LifecycleAware {
   private static final Logger logger = LoggerFactory.getLogger(LeasePlugin.class);
   public static final long WARNING_PERIOD_MS = 3600000L;
   private static final NoSpamLogger noSpamLogger;
   public static final CompositeType keyType;
   public static final Duration INTERNODE_TIMEOUT_MS;
   public static final int MIN_LEASE_DURATION_MS = 5000;
   public static final int MAX_LEASE_DURATION_MS = 3600000;
   volatile LeasePlugin.Resources resources = null;
   @Inject
   private volatile ThreadPoolPlugin threadPool;
   @Inject
   private volatile InternodeClient client;
   @Inject
   private volatile InternodeProtocolRegistry protocolRegistry;
   @Inject
   private volatile LeaseProtocol leaseProtocol;

   public LeasePlugin() {
   }

   public void preSetup() {
      this.protocolRegistry.register(this.leaseProtocol);
   }

   public void onRegister() {
      super.onRegister();
      JMX.registerMBean(this, JMX.Type.CORE, MapBuilder.immutable().withKeys(new String[]{"name"}).withValues(new String[]{"Leases"}).build());
   }

   public void onActivate() {
      try {
         synchronized(this) {
            if(this.resources == null) {
               this.resources = new LeasePlugin.Resources();
               RpcRegistry.register("Leases", this);
            } else {
               throw new AssertionError("Tried to register resources but one already exists; was activate called twice?");
            }
         }
      } catch (Exception var4) {
         throw new AssertionError("Couldn't start lease plugin!", var4);
      }
   }

   public void onPreDeactivate() {
      LeasePlugin.Resources localResouces = this.resources;
      this.resources = null;
      if(localResouces != null) {
         localResouces.shutdown();
      }

      RpcRegistry.unregister("Leases");
   }

   public boolean isEnabled() {
      return true;
   }

   public Map<LeaseMonitorCore.LeaseId, Map<String, Boolean>> getAllLeasesStatus() throws Exception {
      LeaseManager manager = this.getManager();
      return (Map)manager.core.readLeases().stream().collect(Collectors.toMap((row) -> {
         return new LeaseMonitorCore.LeaseId(row.name, row.dc);
      }, (row) -> {
         return this.getLeaseStatus(manager, row.name, row.dc);
      }));
   }

   @Rpc(
      name = "getLeaseStatus",
      permission = CorePermission.SELECT
   )
   public Map<String, Boolean> getLeaseStatus(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc) throws IPlugin.PluginNotActiveException {
      this.checkLeaseNameAndDc(name, dc);
      return this.getLeaseStatus(this.getManager(), name, dc);
   }

   private Map<String, Boolean> getLeaseStatus(LeaseManager manager, String name, String dc) {
      return (Map)StorageService.instance.getNaturalEndpoints(manager.core.keyspace, keyType.decompose(new Object[]{name, dc})).stream().filter((ep) -> {
         return manager.core.inDc(dc, EndpointStateTracker.instance.getDatacenter(ep));
      }).collect(Collectors.toMap((ep) -> {
         return ep.getHostAddress();
      }, (ep) -> {
         return Boolean.valueOf(FailureDetector.instance.isAlive(ep));
      }));
   }

   public Set<LeaseMonitorCore.LeaseRow> getAllLeases() throws Exception {
      return this.getAllLeases((row) -> {
         return true;
      }, true);
   }

   public Set<LeaseMonitorCore.LeaseRow> getAllLeases(Predicate<LeaseMonitorCore.LeaseRow> filter, boolean ping) throws Exception {
      return (Set)this.getCore().readLeases().stream().map((row) -> {
         return new LeaseMonitorCore.LeaseRow(row.name, row.dc, row.epoch, ping?this.internalClientPingNoThrow(row.name, row.dc, false).holder:null, row.duration_ms);
      }).filter(filter).collect(Collectors.toSet());
   }

   public Set<LeaseMonitorCore.LeaseId> getLeasesOfNonexistentDatacenters() throws Exception {
      AbstractReplicationStrategy replicationStrategy = Keyspace.open(this.getCore().keyspace).getReplicationStrategy();
      return LeaseMonitorCore.rowsToIds(this.getAllLeases((row) -> {
         return !this.isLiveLease(replicationStrategy, row.getDc());
      }, false));
   }

   public Set<LeaseMonitorCore.LeaseId> getSparkMastersOfNonAnalyticsDcs() throws Exception {
      Set<String> analyticsDcs = (Set)Gossiper.instance.getEndpointStates().stream().filter((entry) -> {
         return !Gossiper.instance.isDeadState((EndpointState)entry.getValue());
      }).filter((entry) -> {
         return Workload.Analytics.isCompatibleWith(EndpointStateTracker.instance.getWorkloads((InetAddress)entry.getKey()));
      }).map((entry) -> {
         return EndpointStateTracker.instance.getDatacenter((InetAddress)entry.getKey());
      }).collect(Collectors.toSet());
      return LeaseMonitorCore.rowsToIds(this.getAllLeases((row) -> {
         return row.getName().equals("Leader/master/6.0") && !analyticsDcs.contains(row.getDc());
      }, false));
   }

   public LeaseMonitor.ClientPingResult internalClientPing(String name, String dc, boolean takeIfOpen) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      long systemTime = System.currentTimeMillis();
      LeaseMonitor.ClientPingResult r = this.clientPing(name, dc, Addresses.Internode.getBroadcastAddress(), takeIfOpen);
      return new LeaseMonitor.ClientPingResult(r.holder, r.holder == null?r.leaseTimeRemaining:r.leaseTimeRemaining + systemTime);
   }

   public LeaseMonitor.ClientPingResult internalClientPingNoThrow(String name, String dc, boolean takeIfOpen) {
      try {
         return this.internalClientPing(name, dc, takeIfOpen);
      } catch (Exception var5) {
         noSpamLogger.warn("internal client ping failed!", new Object[]{var5});
         return new LeaseMonitor.ClientPingResult(0L);
      }
   }

   public LeaseMonitor.ClientPingResult clientPing(String name, String dc) throws Exception {
      return this.clientPing(name, dc, (InetAddress)null, false);
   }

   @Rpc(
      name = "clientPing",
      permission = CorePermission.SELECT
   )
   public LeaseMonitor.ClientPingResult clientPing(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc, @RpcParam(name = "client") InetAddress client, @RpcParam(name = "takeIfOpen") boolean takeIfOpen) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      if(client == null && takeIfOpen) {
         throw new IOException("Cannot take the lease with a null client!");
      } else {
         return (LeaseMonitor.ClientPingResult)this.route("clientPing", name, dc, (manager) -> {
            return manager.clientPing(name, dc, client, takeIfOpen);
         }, () -> {
            return new LeaseProtocol.ClientPingMessage(name, dc, client, takeIfOpen);
         });
      }
   }

   @Rpc(
      name = "createLease",
      permission = CorePermission.MODIFY
   )
   public boolean createLease(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc, @RpcParam(name = "duration_ms") int duration_ms) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      if(duration_ms >= 5000 && duration_ms <= 3600000) {
         return ((Boolean)this.route("createLease", name, dc, (manager) -> {
            return Boolean.valueOf(manager.createLease(name, dc, duration_ms));
         }, () -> {
            return new LeaseProtocol.CreateLeaseMessage(name, dc, duration_ms);
         })).booleanValue();
      } else {
         throw new IOException(String.format("Lease duration must be in the range {%d, %d} ms", new Object[]{Integer.valueOf(5000), Integer.valueOf(3600000)}));
      }
   }

   @Rpc(
      name = "disableLease",
      permission = CorePermission.MODIFY
   )
   public boolean disableLease(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      return ((Boolean)this.route("disableLease", name, dc, (manager) -> {
         return Boolean.valueOf(manager.disableLease(name, dc));
      }, () -> {
         return new LeaseProtocol.DisableLeaseMessage(name, dc);
      })).booleanValue();
   }

   @Rpc(
      name = "deleteLease",
      permission = CorePermission.MODIFY
   )
   public boolean deleteLease(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      return ((Boolean)this.route("deleteLease", name, dc, (manager) -> {
         return Boolean.valueOf(manager.deleteLease(name, dc));
      }, () -> {
         return new LeaseProtocol.DeleteLeaseMessage(name, dc);
      })).booleanValue();
   }

   @Rpc(
      name = "getLeaseDuration",
      permission = CorePermission.SELECT
   )
   public int getLeaseDuration(@RpcParam(name = "name") String name, @RpcParam(name = "dc") String dc) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      return ((Integer)this.route("getLeaseDuration", name, dc, (manager) -> {
         return manager.getLeaseDuration(name, dc);
      }, () -> {
         return new LeaseProtocol.LeaseDurationMessage(name, dc);
      })).intValue();
   }

   public boolean cleanupDeadLease(String name, String dc) throws Exception {
      this.checkLeaseNameAndDc(name, dc);
      LeaseMonitorCore core = this.getCore();
      if(this.isLiveLease(Keyspace.open(core.keyspace).getReplicationStrategy(), dc)) {
         throw new IllegalArgumentException(String.format("Keyspace %s has replicas in DC %s; the lease is not dead!", new Object[]{core.keyspace, dc}));
      } else {
         return core.cleanupLease(name, dc);
      }
   }

   public List<LeaseMetrics> getMetrics() throws IPlugin.PluginNotActiveException {
      return this.getManager().getMetrics();
   }

   private static List<InetAddress> filterByDc(Collection<InetAddress> endpoints, LeaseManager manager, String dc, boolean filterInactive) {
      Stream<InetAddress> endpointStream = endpoints.stream().filter((addr) -> {
         return manager.core.inDc(dc, EndpointStateTracker.instance.getDatacenter(addr));
      });
      if(filterInactive) {
         endpointStream = endpointStream.filter((addr) -> {
            return EndpointStateTracker.instance.isActive(addr);
         });
      }

      return (List)endpointStream.collect(Collectors.toList());
   }

   protected InetAddress getRemoteMonitor(LeaseManager manager, String name, String dc) throws IOException {
      ArrayList<InetAddress> endPoints = new ArrayList();
      DecoratedKey decoratedKey = StorageService.instance.getTokenMetadata().decorateKey(keyType.decompose(new Object[]{name, dc}));
      StorageService.addLiveNaturalEndpointsToList(Keyspace.open(manager.core.keyspace), decoratedKey, endPoints);
      List<InetAddress> endpoints = filterByDc(endPoints, manager, dc, true);
      if(endpoints.contains(Addresses.Internode.getBroadcastAddress())) {
         return null;
      } else if(endpoints.isEmpty()) {
         String message = String.format("No live replicas for lease %s.%s in table %s.%s ", new Object[]{name, dc, manager.core.keyspace, manager.core.table});
         List<InetAddress> deadEndpoints = filterByDc(StorageService.instance.getNaturalEndpoints(manager.core.keyspace, keyType.decompose(new Object[]{name, dc})), manager, dc, false);
         if(!EndpointStateTracker.instance.getAllKnownDatacenters().containsKey(dc)) {
            message = message + String.format("(%s is not a known DC)", new Object[]{dc});
         } else if(deadEndpoints.isEmpty()) {
            message = message + String.format("(keyspace %s has no replicas in datacenter %s!  You need to adjust the replication factor.)", new Object[]{manager.core.keyspace, dc});
         } else {
            message = message + String.format("Nodes %s are all down/still starting.", new Object[]{deadEndpoints});
         }

         throw new IOException(message);
      } else {
         DatabaseDescriptor.getEndpointSnitch().sortByProximity(Addresses.Internode.getBroadcastAddress(), endpoints);
         return (InetAddress)endpoints.get(0);
      }
   }

   protected <Result, Request extends LeaseProtocol.LeaseRequest> Result route(String operation, String name, String dc, Function<LeaseManager, Result> local, Supplier<Request> request) throws IllegalArgumentException, IPlugin.PluginNotActiveException, IOException {
      logger.debug("Executing {} on {}.{}", new Object[]{operation, name, dc});
      LeasePlugin.Resources localResources = this.getResources();
      this.checkLeaseNameAndDc(name, dc);
      LeaseManager manager = this.resources.getManager();
      InetAddress remoteMonitor = this.getRemoteMonitor(manager, name, dc);
      if(remoteMonitor != null) {
         logger.debug("Forwarding request to node {}", remoteMonitor);
         Request message = (LeaseProtocol.LeaseRequest)request.get();
         return this.client.sendSync(remoteMonitor, message.type, message, INTERNODE_TIMEOUT_MS);
      } else {
         logger.debug("Handling request locally!");
         return local.apply(manager);
      }
   }

   private void checkLeaseNameAndDc(String name, String dc) {
      if(name != null && !name.isEmpty()) {
         if(dc == null || dc.isEmpty()) {
            throw new IllegalArgumentException("Lease dc cannot be null or empty.");
         }
      } else {
         throw new IllegalArgumentException("Lease name cannot be null or empty.");
      }
   }

   public InternalLeaseLeader getLeader(String name, String dc, int duration_ms, boolean takeIfOpen) {
      return new InternalLeaseLeader(this, this.threadPool, name, dc, duration_ms, takeIfOpen);
   }

   public int getReplicationFactor() throws IPlugin.PluginNotActiveException {
      return this.getCore().getLocalDcRf();
   }

   protected LeasePlugin.Resources getResources() throws IPlugin.PluginNotActiveException {
      LeasePlugin.Resources localResources = this.resources;
      if(localResources == null) {
         throw new IPlugin.PluginNotActiveException("The plugin is not enabled/not started");
      } else {
         return localResources;
      }
   }

   private LeaseManager getManager() throws IPlugin.PluginNotActiveException {
      return this.getResources().getManager();
   }

   private LeaseMonitorCore getCore() throws IPlugin.PluginNotActiveException {
      return this.getManager().core;
   }

   private boolean isLiveLease(AbstractReplicationStrategy replicationStrategy, String dc) {
      if(!LeaseMonitorCore.isGlobal(dc) && !(replicationStrategy instanceof SimpleStrategy)) {
         if(replicationStrategy instanceof NetworkTopologyStrategy) {
            return ((NetworkTopologyStrategy)replicationStrategy).getReplicationFactor(dc) != 0;
         } else {
            noSpamLogger.warn("Did not expect replication strategy {}", new Object[]{replicationStrategy.getClass()});
            return true;
         }
      } else {
         return true;
      }
   }

   public static long clock() {
      return System.nanoTime() / 1000000L;
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 3600000L, TimeUnit.MILLISECONDS);
      keyType = CompositeType.getInstance(new AbstractType[]{UTF8Type.instance, UTF8Type.instance});
      INTERNODE_TIMEOUT_MS = Duration.ofMillis(DatabaseDescriptor.getRpcTimeout());
   }

   protected class Resources {
      protected final LeaseManager manager;
      protected final ScheduledExecutorService pool;

      protected Resources() throws Exception {
         ThreadFactory threadFactory = (new Builder()).namingPattern("LeasePlugin-%d").build();
         this.pool = Executors.newScheduledThreadPool(4, threadFactory);
         this.manager = new LeaseManager(this.pool);
         this.manager.start();
         LeasePlugin.logger.info("Lease service up and running.");
      }

      LeaseManager getManager() throws IPlugin.PluginNotActiveException {
         LeaseManager m = this.manager;
         if(!m.isRunning()) {
            throw new IPlugin.PluginNotActiveException("5.0 lease operations are disabled until the upgrade finishes.");
         } else {
            return this.manager;
         }
      }

      protected void shutdown() {
         LeaseManager m = this.manager;
         if(m != null) {
            m.stop();
         }

         this.pool.shutdown();
      }
   }
}
