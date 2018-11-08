package com.datastax.bdp.snitch;

import com.datastax.bdp.util.Addresses;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.net.InetAddress;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointStatePersister {
   private static final Logger logger = LoggerFactory.getLogger(EndpointStatePersister.class);
   private static final String DATACENTER_COLUMN = "data_center";
   /** @deprecated */
   @Deprecated
   public static final String WORKLOAD_COLUMN = "workload";
   public static final String WORKLOADS_COLUMN = "workloads";
   public static final String DSE_VERSION_COLUMN = "dse_version";
   public static final String GRAPH_SERVER_COLUMN = "graph";
   public static final String SERVER_ID_COLUMN = "server_id";
   private final ConcurrentMap<InetAddress, String> datacenterCache = Maps.newConcurrentMap();
   private final ConcurrentMap<InetAddress, Set<Workload>> workloadsCache = Maps.newConcurrentMap();
   private final ConcurrentMap<InetAddress, CassandraVersion> versionCache = Maps.newConcurrentMap();
   private final ConcurrentMap<InetAddress, Boolean> graphCache = Maps.newConcurrentMap();
   private final ConcurrentMap<InetAddress, String> serverIdCache = Maps.newConcurrentMap();
   private volatile boolean shutdown;

   public EndpointStatePersister() {
      logger.info("EndpointStatePersister started");
   }

   public void setDatacenter(InetAddress endpoint, String datacenter) {
      Preconditions.checkNotNull(endpoint);
      Preconditions.checkNotNull(datacenter);
      if(logger.isDebugEnabled()) {
         logger.debug("Setting datacenter of " + endpoint.getHostAddress() + " to " + datacenter);
      }

      this.datacenterCache.put(endpoint, datacenter);
      this.setPeerInfo(endpoint, "data_center", datacenter);
   }

   public void setWorkloads(InetAddress endpoint, Set<Workload> workloads, boolean active) {
      Preconditions.checkNotNull(endpoint);
      Preconditions.checkNotNull(workloads);
      if(logger.isDebugEnabled()) {
         logger.debug("Setting workloads of " + endpoint.getHostAddress() + " to " + workloads.toString() + ". Endpoint is " + (active?"active":"not active") + " and endpoint is " + (StorageService.instance.getTokenMetadata().isMember(endpoint)?"commissioned":"decommissioned"));
      }

      this.workloadsCache.put(endpoint, workloads);
      this.setPeerInfo(endpoint, "workload", Workload.legacyWorkloadName(workloads));
      this.setPeerInfo(endpoint, "workloads", Workload.toStringSet(workloads));
   }

   public void setDseVersion(InetAddress endpoint, CassandraVersion version) {
      Preconditions.checkArgument(endpoint != null, "Endpoint must not be null");
      Preconditions.checkArgument(version != null, "version must not be null");
      if(logger.isDebugEnabled()) {
         logger.debug("Setting version of " + endpoint.getHostAddress() + " to " + version);
      }

      this.versionCache.put(endpoint, version);
      this.setPeerInfo(endpoint, "dse_version", version.toString());
   }

   public void setIsGraphNode(InetAddress endpoint, boolean isServer) {
      Preconditions.checkArgument(endpoint != null, "Endpoint must not be null");
      if(logger.isDebugEnabled()) {
         logger.debug("Setting graph server value of " + endpoint.getHostAddress() + " to " + isServer);
      }

      this.graphCache.put(endpoint, Boolean.valueOf(isServer));
      this.setPeerInfo(endpoint, "graph", Boolean.valueOf(isServer));
   }

   public void setServerId(InetAddress endpoint, String id) {
      Preconditions.checkArgument(endpoint != null, "Endpoint must not be null");
      Preconditions.checkArgument(id != null, "Server ID must not be null");
      if(logger.isDebugEnabled()) {
         logger.debug("Setting server ID of " + endpoint.getHostAddress() + " to " + id);
      }

      this.serverIdCache.put(endpoint, id);
      this.setPeerInfo(endpoint, "server_id", id);
   }

   public String getDatacenter(InetAddress endpoint) {
      String datacenter = (String)this.datacenterCache.get(endpoint);
      if(datacenter == null) {
         datacenter = this.loadPeerInfo(endpoint, "data_center");
         if(datacenter != null) {
            this.datacenterCache.put(endpoint, datacenter);
         }
      }

      return datacenter;
   }

   public Set<Workload> getWorkloads(InetAddress endpoint) {
      Set<Workload> workloads = (Set)this.workloadsCache.get(endpoint);
      if(workloads == null || workloads.isEmpty()) {
         workloads = this.loadWorkloads(endpoint);
         if(!Workload.isDefined(workloads)) {
            return Collections.unmodifiableSet(EnumSet.of(Workload.Unknown));
         }

         if(!workloads.contains(Workload.Unknown)) {
            this.workloadsCache.put(endpoint, workloads);
         }
      }

      return workloads;
   }

   public Set<Workload> getWorkloadsIfPresent(InetAddress endpoint) {
      Set<Workload> workloads = (Set)this.workloadsCache.get(endpoint);
      return workloads != null && !workloads.isEmpty()?workloads:Collections.unmodifiableSet(EnumSet.of(Workload.Unknown));
   }

   public CassandraVersion getDseVersion(InetAddress endpoint) {
      CassandraVersion version = (CassandraVersion)this.versionCache.get(endpoint);
      if(version == null) {
         String v = this.loadPeerInfo(endpoint, "dse_version");
         if(v != null) {
            version = new CassandraVersion(v);
            this.versionCache.put(endpoint, version);
         }
      }

      return version;
   }

   public String getServerId(InetAddress endpoint) {
      String id = (String)this.serverIdCache.get(endpoint);
      if(id == null) {
         id = this.loadPeerInfo(endpoint, "server_id");
         if(id != null) {
            this.serverIdCache.put(endpoint, id);
         }
      }

      return id;
   }

   public boolean isGraphNode(InetAddress endpoint) {
      if(this.graphCache.containsKey(endpoint)) {
         return ((Boolean)this.graphCache.get(endpoint)).booleanValue();
      } else {
         String v = this.loadPeerInfo(endpoint, "graph");
         boolean isGraph = false;
         if(v != null) {
            isGraph = Boolean.parseBoolean(v);
            this.graphCache.put(endpoint, Boolean.valueOf(isGraph));
         }

         return isGraph;
      }
   }

   private String loadPeerInfo(InetAddress endpoint, String columnName) {
      return Addresses.Internode.isLocalEndpoint(endpoint)?this.loadLocalInfo(columnName):this.loadRemotePeerInfo(endpoint, columnName);
   }

   private String loadLocalInfo(String columnName) {
      Iterator var2 = ((UntypedResultSet)TPCUtils.blockingGet(SystemKeyspace.loadLocalInfo(columnName))).iterator();

      Row row;
      do {
         if(!var2.hasNext()) {
            return null;
         }

         row = (Row)var2.next();
      } while(!row.has(columnName));

      return row.getString(columnName);
   }

   private String loadRemotePeerInfo(InetAddress endpoint, String columnName) {
      Iterator var3 = ((UntypedResultSet)TPCUtils.blockingGet(SystemKeyspace.loadPeerInfo(endpoint, columnName))).iterator();

      Row row;
      do {
         if(!var3.hasNext()) {
            return null;
         }

         row = (Row)var3.next();
      } while(!row.has(columnName));

      return row.getString(columnName);
   }

   private Set<Workload> loadWorkloads(InetAddress endpoint) {
      InetAddress ep = endpoint;
      UntypedResultSet resultSet;
      if(Addresses.Internode.isLocalEndpoint(endpoint)) {
         resultSet = (UntypedResultSet)TPCUtils.blockingGet(SystemKeyspace.loadLocalInfo("workloads"));
         ep = Addresses.Internode.getBroadcastAddress();
      } else {
         resultSet = (UntypedResultSet)TPCUtils.blockingGet(SystemKeyspace.loadPeerInfo(endpoint, "workloads"));
      }

      Iterator var4 = resultSet.iterator();

      Row row;
      do {
         if(!var4.hasNext()) {
            return null;
         }

         row = (Row)var4.next();
      } while(!row.has("workloads"));

      return Workload.fromStringSet(row.getSet("workloads", UTF8Type.instance), ep);
   }

   private void setPeerInfo(InetAddress endpoint, String columnName, Object value) {
      if(!Addresses.Internode.isLocalEndpoint(endpoint)) {
         this.setRemotePeerInfo(endpoint, columnName, value);
      } else if(!this.shutdown) {
         this.setLocalInfo(columnName, value);
      }

   }

   private void setRemotePeerInfo(InetAddress endpoint, String columnName, Object value) {
      if(Gossiper.instance.getLiveMembers().contains(endpoint) && !StorageService.instance.getTokenMetadata().isLeaving(endpoint)) {
         TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(endpoint, columnName, value));
      }

   }

   private void setLocalInfo(String columnName, Object value) {
      TPCUtils.blockingAwait(SystemKeyspace.updateLocalInfo(columnName, value));
   }

   public void bustCache() {
      this.datacenterCache.clear();
      this.workloadsCache.clear();
      this.versionCache.clear();
      this.graphCache.clear();
      this.serverIdCache.clear();
   }

   public Map<String, Long> getAllKnownDatacenters() {
      return (Map)this.datacenterCache.values().stream().collect(Collectors.groupingBy((x) -> {
         return x;
      }, Collectors.counting()));
   }

   public Map<String, Set<Workload>> getDatacenterWorkloads() {
      Map<String, Set<Workload>> datacenterWorkloads = Maps.newHashMap();
      this.datacenterCache.entrySet().stream().forEach((entry) -> {
         String dc = (String)entry.getValue();
         InetAddress endpoint = (InetAddress)entry.getKey();
         Set<Workload> workloads = (Set)this.workloadsCache.get(endpoint);
         datacenterWorkloads.putIfAbsent(dc, workloads);
      });
      return datacenterWorkloads;
   }

   public void shutdown() {
      this.shutdown = true;
   }
}
