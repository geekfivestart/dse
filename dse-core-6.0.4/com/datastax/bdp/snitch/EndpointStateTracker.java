package com.datastax.bdp.snitch;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.leasemanager.LeasePlugin;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.StringUtil;
import com.datastax.bdp.util.rpc.Rpc;
import com.datastax.bdp.util.rpc.RpcParam;
import com.google.common.collect.Maps;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointStateTracker implements IEndpointStateChangeSubscriber, EndpointStateTrackerMXBean {
   private static final Logger logger = LoggerFactory.getLogger(EndpointStateTracker.class);
   public static final EndpointStateTracker instance = new EndpointStateTracker();
   private final EndpointStatePersister persister = new EndpointStatePersister();
   private final Gossiper gossiper;
   private final ConcurrentMap<InetAddress, Map<String, DseState.CoreIndexingStatus>> coreIndexingStatus;
   private final ConcurrentMap<InetAddress, Double> nodeHealth;
   private final ConcurrentMap<InetAddress, Boolean> activeStatuses;
   private final ConcurrentMap<InetAddress, Boolean> blacklistedStatuses;
   private final DseVersionNotifier versionNotifier;

   public EndpointStateTracker() {
      this.gossiper = Gossiper.instance;
      this.coreIndexingStatus = Maps.newConcurrentMap();
      this.nodeHealth = Maps.newConcurrentMap();
      this.activeStatuses = Maps.newConcurrentMap();
      this.blacklistedStatuses = Maps.newConcurrentMap();
      this.gossiper.register(this);
      this.versionNotifier = new DseVersionNotifier(this);
      logger.debug("EndpointStateTracker started");
   }

   public void setup() {
      Iterator var1 = this.gossiper.getEndpointStates().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, EndpointState> e = (Entry)var1.next();
         this.updateEndpointState((InetAddress)e.getKey(), (EndpointState)e.getValue());
      }

      logger.info("EndpointStateTracker setup");
   }

   public static void runAtVersion(CassandraVersion version, String standByMessage, Runnable runnable, String readyMessage) {
      instance.getVersionNotifier().runAtVersion(version, standByMessage, readyMessage, runnable);
   }

   public String getDatacenter(InetAddress endpoint) {
      return this.persister.getDatacenter(endpoint);
   }

   public Set<Workload> getWorkloads(InetAddress endpoint) {
      return this.persister.getWorkloads(endpoint);
   }

   public Set<Workload> getWorkloadsIfPresent(InetAddress endpoint) {
      return this.persister.getWorkloadsIfPresent(endpoint);
   }

   public Map<String, Set<Workload>> getDatacenterWorkloads() {
      return this.persister.getDatacenterWorkloads();
   }

   public String getLegacyWorkload(InetAddress endpoint) {
      return Workload.legacyWorkloadName(this.getWorkloads(endpoint));
   }

   public CassandraVersion getDseVersion(InetAddress endpoint) {
      return this.persister.getDseVersion(endpoint);
   }

   public boolean getIsGraphServer(String endpoint) {
      try {
         return this.getIsGraphServer(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public boolean getIsGraphServer(InetAddress endpoint) {
      return this.persister.isGraphNode(endpoint);
   }

   public String getServerId(InetAddress endpoint) {
      return this.persister.getServerId(endpoint);
   }

   public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(InetAddress endpoint) {
      return this.coreIndexingStatus.containsKey(endpoint)?(Map)this.coreIndexingStatus.get(endpoint):Collections.emptyMap();
   }

   public String getDatacenter(String endpoint) {
      try {
         return this.getDatacenter(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Map<String, Long> getAllKnownDatacenters() {
      return this.persister.getAllKnownDatacenters();
   }

   @Rpc(
      name = "getWorkload",
      permission = CorePermission.SELECT
   )
   public String getWorkloads(@RpcParam(name = "endpoint") String endpoint) {
      try {
         return Workload.workloadNames(this.getWorkloads(InetAddress.getByName(endpoint)));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getServerId(String endpoint) {
      try {
         return this.getServerId(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getServerId() {
      return this.getServerId(Addresses.Internode.getBroadcastAddress());
   }

   public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(String endpoint) {
      try {
         String address = InetAddress.getByName(endpoint).isLoopbackAddress()?Addresses.Internode.getBroadcastAddress().getHostAddress():endpoint;
         return this.getCoreIndexingStatus(InetAddress.getByName(address));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Boolean getActiveStatus(String endpoint) {
      try {
         return Boolean.valueOf(this.isActive(InetAddress.getByName(endpoint)));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Boolean getBlacklistedStatus(String endpoint) {
      try {
         return this.getBlacklistedStatus(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Double getNodeHealth(String endpoint) {
      try {
         String address = InetAddress.getByName(endpoint).isLoopbackAddress()?Addresses.Internode.getBroadcastAddress().getHostAddress():endpoint;
         return this.getNodeHealth(InetAddress.getByName(address));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Double getNodeHealth(InetAddress endpoint) {
      return this.nodeHealth.containsKey(endpoint)?(Double)this.nodeHealth.get(endpoint):Double.valueOf(0.0D);
   }

   public boolean getActiveStatus(InetAddress endpoint) {
      return ((Boolean)this.activeStatuses.getOrDefault(endpoint, Boolean.valueOf(false))).booleanValue();
   }

   public boolean isActive(InetAddress endpoint) {
      return this.getActiveStatus(endpoint) && Gossiper.instance.isAlive(endpoint);
   }

   public Boolean getBlacklistedStatus(InetAddress endpoint) {
      Boolean blacklisted = (Boolean)this.blacklistedStatuses.get(endpoint);
      return Boolean.valueOf(blacklisted != null?blacklisted.booleanValue():false);
   }

   public boolean isBlacklisted() {
      Boolean blacklisted = (Boolean)this.blacklistedStatuses.get(Addresses.Internode.getBroadcastAddress());
      return blacklisted != null?blacklisted.booleanValue():false;
   }

   public void setBlacklisted(boolean blacklisted) {
      DseState.instance.setBlacklistedStatusAsync(blacklisted);
      logger.info("This node is now {}, and it will participate in distributed search queries{}.", blacklisted?"blacklisted":"non-blacklisted", blacklisted?" only when no active replicas exist":"");
   }

   public void beforeChange(InetAddress address, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
      boolean inactive = newValue.value.startsWith("shutdown") || newValue.value.startsWith("LEAVING");
      if(address.equals(Addresses.Internode.getBroadcastAddress()) && newStateKey.equals(ApplicationState.STATUS) && inactive) {
         this.persister.shutdown();
      }

   }

   public void onJoin(InetAddress endpoint, EndpointState state) {
      if(logger.isDebugEnabled()) {
         String log = StringUtil.stripNonPrintableCharacters("Endpoint " + endpoint + " joined with state " + state);
         logger.debug(log);
      }

      this.updateEndpointState(endpoint, state);
      this.stateChanged();
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      if(logger.isDebugEnabled() && (state == ApplicationState.DC || DseState.instance.isDseState(state))) {
         String log = StringUtil.stripNonPrintableCharacters("Endpoint " + endpoint + " state changed " + state + " = " + value.value);
         logger.debug(log);
      }

      this.updateAppState(state, endpoint, value);
      this.stateChanged();
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
      if(logger.isDebugEnabled()) {
         String log = StringUtil.stripNonPrintableCharacters("Endpoint " + endpoint + " alive with state " + state);
         logger.debug(log);
      }

      this.updateEndpointState(endpoint, state);
      this.stateChanged();
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
      if(logger.isDebugEnabled()) {
         String log = StringUtil.stripNonPrintableCharacters("Endpoint " + endpoint + " dead with state " + state);
         logger.debug(log);
      }

      this.updateEndpointState(endpoint, state);
      this.stateChanged();
   }

   public void onRemove(InetAddress endpoint) {
      this.coreIndexingStatus.remove(endpoint);
      this.nodeHealth.remove(endpoint);
      this.activeStatuses.remove(endpoint);
      this.blacklistedStatuses.remove(endpoint);
      this.persister.bustCache();
      this.stateChanged();
   }

   public void onRestart(InetAddress endpoint, EndpointState state) {
      if(logger.isDebugEnabled()) {
         String log = StringUtil.stripNonPrintableCharacters("Endpoint " + endpoint + " restarted with state " + state);
         logger.debug(log);
      }

      this.updateEndpointState(endpoint, state);
      this.stateChanged();
   }

   private void updateAppState(ApplicationState state, InetAddress endpoint, VersionedValue value) {
      if(state.equals(ApplicationState.DC)) {
         this.persister.setDatacenter(endpoint, value.value);
      } else if(DseState.instance.isDseState(state)) {
         Map<String, Object> dseState = DseState.instance.getValues(state, value);
         this.updateState(dseState, endpoint);
      }

   }

   private void checkRackAndServerId(InetAddress endpoint, String endpointServerId) {
      InetAddress self = Addresses.Internode.getBroadcastAddress();
      if(!self.equals(endpoint)) {
         String localId = DseConfig.getServerId();
         if(localId != null && endpointServerId != null) {
            if(localId.equals(endpointServerId)) {
               IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
               InetAddress broadcastAddress = Addresses.Internode.getBroadcastAddress();
               String localDC = snitch.getDatacenter(broadcastAddress);
               String endpointDC = snitch.getDatacenter(endpoint);
               String localRack = snitch.getRack(broadcastAddress);
               String endpointRack = snitch.getRack(endpoint);
               if(localDC.equals(endpointDC) && !localRack.equals(endpointRack)) {
                  String msg = "%s has the same server ID %s, and same DC %s, but is placed in a different rack %s - (%s, %s, %s) vs. (%s, %s, %s)";
                  logger.warn(String.format(msg, new Object[]{endpoint, endpointServerId, endpointDC, endpointRack, broadcastAddress, localDC, localRack, endpoint, endpointDC, endpointRack}));
               }

            }
         }
      }
   }

   private void updateEndpointState(InetAddress endpoint, EndpointState epState) {
      String datacenter = this.datacenterOf(epState);
      if(datacenter != null) {
         this.persister.setDatacenter(endpoint, datacenter);
      }

      Map<String, Object> dseState = this.dseStateOf(epState);
      if(dseState != null) {
         this.updateState(dseState, endpoint);
      }

   }

   private void updateState(Map<String, Object> dseState, InetAddress endpoint) {
      Boolean activeStatus = DseState.instance.getActiveStatus(dseState);
      this.activeStatuses.put(endpoint, activeStatus);
      Boolean blacklistingStatus = DseState.instance.getBlacklistingStatus(dseState);
      this.blacklistedStatuses.put(endpoint, blacklistingStatus);
      Set<Workload> workloads = DseState.instance.getWorkloads(dseState, endpoint);
      this.persister.setWorkloads(endpoint, workloads, activeStatus.booleanValue());
      Map<String, DseState.CoreIndexingStatus> cores = DseState.instance.getCoreIndexingStatus(dseState);
      if(cores != null) {
         this.coreIndexingStatus.put(endpoint, cores);
      }

      Double nh = Double.valueOf(DseState.instance.getNodeHealth(dseState));
      this.nodeHealth.put(endpoint, nh);
      CassandraVersion version = DseState.instance.getDseVersion(dseState);
      if(version != null) {
         this.persister.setDseVersion(endpoint, version);
      }

      this.versionNotifier.maybeUpdateVersion(version);
      String serverId = DseState.instance.getServerID(dseState);
      if(serverId != null) {
         this.persister.setServerId(endpoint, serverId);
         this.checkRackAndServerId(endpoint, serverId);
      }

      Boolean isGraph = DseState.instance.getIsGraphNode(dseState);
      if(isGraph != null) {
         this.persister.setIsGraphNode(endpoint, isGraph.booleanValue());
      }

   }

   private String datacenterOf(EndpointState es) {
      VersionedValue value = es != null?es.getApplicationState(ApplicationState.DC):null;
      return value != null?value.value:null;
   }

   private Map<String, Object> dseStateOf(EndpointState es) {
      return DseState.instance.getValues(es);
   }

   public synchronized boolean waitForAllNodesAlive(int timeout) throws InterruptedException {
      long startTime = System.currentTimeMillis();

      while(System.currentTimeMillis() - startTime < (long)timeout && !this.areAllNodesAlive()) {
         this.wait((long)timeout);
      }

      return this.areAllNodesAlive();
   }

   private boolean areAllNodesAlive() {
      return this.gossiper.getUnreachableMembers().isEmpty() && this.getVersionNotifier().isReady();
   }

   private synchronized void stateChanged() {
      this.notifyAll();
   }

   public boolean vnodesEnabled() {
      return StorageService.instance.getTokenMetadata().sortedTokens().size() != Gossiper.instance.getEndpointStates().size();
   }

   public SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> getRing(String keyspace) throws UnknownHostException {
      Map<String, String> tokenToEndpoint = StorageService.instance.getTokenToEndpointMap();
      List<String> sortedTokens = new ArrayList(tokenToEndpoint.keySet());
      Collection<String> liveNodes = StorageService.instance.getLiveNodes();
      Collection<String> deadNodes = StorageService.instance.getUnreachableNodes();
      Collection<String> joiningNodes = StorageService.instance.getJoiningNodes();
      Collection<String> leavingNodes = StorageService.instance.getLeavingNodes();
      Collection<String> movingNodes = StorageService.instance.getMovingNodes();
      Map<String, String> loadMap = StorageService.instance.getLoadMap();
      LinkedHashMap<String, List<String>> endpointToTokens = this.endpointToTokens(sortedTokens, tokenToEndpoint);
      boolean vNodesEnabled = endpointToTokens.size() != tokenToEndpoint.size();
      boolean effective = true;

      Object ownerships;
      try {
         ownerships = StorageService.instance.effectiveOwnership(keyspace);
      } catch (IllegalStateException var33) {
         effective = false;
         ownerships = StorageService.instance.getOwnership();
      }

      Map<String, String> leaderLabels = this.getLeaderLabels();
      SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> ring = new TreeMap();
      Iterator var16 = endpointToTokens.keySet().iterator();

      while(var16.hasNext()) {
         String primaryEndpoint = (String)var16.next();
         InetAddress ep = InetAddress.getByName(primaryEndpoint);
         String dataCenter = this.unknownIfNull(instance.getDatacenter(ep));
         String rack = this.unknownIfNull(DatabaseDescriptor.getEndpointSnitch().getRack(ep));
         boolean live = liveNodes.contains(primaryEndpoint);
         boolean dead = deadNodes.contains(primaryEndpoint);
         String status = live?"Up":(dead?"Down":"?");
         String state = "Normal";
         if(joiningNodes.contains(primaryEndpoint)) {
            state = "Joining";
         } else if(leavingNodes.contains(primaryEndpoint)) {
            state = "Leaving";
         } else if(movingNodes.contains(primaryEndpoint)) {
            state = "Moving";
         }

         String load = loadMap.containsKey(primaryEndpoint)?(String)loadMap.get(primaryEndpoint):"?";
         String owns = (new DecimalFormat("##0.00%")).format(((Map)ownerships).get(ep) == null?0.0D:(double)((Float)((Map)ownerships).get(ep)).floatValue());
         String legacyWorkloadName = this.unknownIfNull(instance.getLegacyWorkload(ep));
         boolean isGraph = instance.getIsGraphServer(ep);
         String analyticsLabel = "";
         if(leaderLabels.containsKey(primaryEndpoint)) {
            analyticsLabel = (String)leaderLabels.get(primaryEndpoint);
         } else if(Workload.Analytics.name().equals(legacyWorkloadName)) {
            analyticsLabel = "(SW)";
         }

         List<String> tokens = (List)endpointToTokens.get(primaryEndpoint);
         String token = vNodesEnabled?Integer.toString(tokens.size()):(String)tokens.get(0);
         String serverId = this.unknownIfNull(instance.getServerId(ep));
         ring.put(primaryEndpoint, new EndpointStateTrackerMXBean.NodeStatus(primaryEndpoint, dataCenter, rack, legacyWorkloadName, status, state, load, owns, token, effective, serverId, isGraph, this.getNodeHealth(primaryEndpoint).doubleValue(), analyticsLabel));
      }

      return ring;
   }

   private String unknownIfNull(String value) {
      return value == null?Workload.Unknown.name():value;
   }

   private Map<String, String> getLeaderLabels() {
      try {
         return (Map)((LeasePlugin)DseInjector.get().getProvider(LeasePlugin.class).get()).getAllLeases().stream().filter((row) -> {
            return row.name.equals("Leader/master/6.0") && row.holder != null;
         }).collect(Collectors.toMap((row) -> {
            return row.holder.getHostAddress();
         }, (row) -> {
            return "(SM)";
         }));
      } catch (Exception var2) {
         logger.debug("Lease exception: ", var2);
         return Collections.emptyMap();
      }
   }

   private LinkedHashMap<String, List<String>> endpointToTokens(List<String> sortedTokens, Map<String, String> tokenToEndpoint) {
      LinkedHashMap<String, List<String>> result = new LinkedHashMap();

      String token;
      Object tokenList;
      for(Iterator var4 = sortedTokens.iterator(); var4.hasNext(); ((List)tokenList).add(token)) {
         token = (String)var4.next();
         String endpoint = (String)tokenToEndpoint.get(token);
         tokenList = (List)result.get(endpoint);
         if(tokenList == null) {
            tokenList = new ArrayList();
            result.put(endpoint, tokenList);
         }
      }

      return result;
   }

   public DseVersionNotifier getVersionNotifier() {
      return this.versionNotifier;
   }

   public InetAddress getInternalAddress() throws UnknownHostException {
      InetAddress internalAddress = Addresses.Internode.getBroadcastAddress();
      EndpointState currentState = Gossiper.instance.getEndpointStateForEndpoint(Addresses.Internode.getBroadcastAddress());
      if(currentState != null) {
         VersionedValue appInternalAddress = currentState.getApplicationState(ApplicationState.INTERNAL_IP);
         if(appInternalAddress != null) {
            internalAddress = InetAddress.getByName(appInternalAddress.value);
         }
      }

      return internalAddress;
   }
}
