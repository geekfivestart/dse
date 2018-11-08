package com.datastax.bdp.gms;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.Addresses;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseState {
   public static final DseState instance = new DseState();
   private static final ApplicationState DSE_GOSSIP_STATE;
   private static final String ENABLE_OLD_DSE_STATE = "enable-old-dse-state";
   private static final String INDEXING_CORES = "indexingCores";
   private static final String CORE_INDEXING_STATUS = "coreIndexingStatus";
   private static final String WORKLOAD = "workload";
   private static final String WORKLOADS = "workloads";
   private static final String ACTIVE = "active";
   private static final String BLACKLISTED = "blacklisted";
   private static final String NODE_HEALTH = "health";
   private static final String DSE_VERSION = "dse_version";
   private static final String GRAPH = "graph";
   private static final String SERVER_ID = "server_id";
   private static final Logger logger;
   private static final ObjectMapper jsonMapper;
   private final boolean newDseState = !Boolean.parseBoolean(System.getProperty("enable-old-dse-state", "false"));
   private volatile boolean verified = false;
   private volatile boolean supported = false;
   private volatile boolean initialized = false;
   private final ExecutorService gossipStateUpdater = Executors.newSingleThreadExecutor((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("DseGossipStateUpdater").build());
   public static final CassandraVersion localVersion;

   private DseState() {
   }

   public boolean isDseState(ApplicationState state) {
      return state.equals(DSE_GOSSIP_STATE);
   }

   public synchronized boolean isSupported() {
      if(!this.verified) {
         EndpointState es = Gossiper.instance.getEndpointStateForEndpoint(Addresses.Internode.getBroadcastAddress());
         this.supported = es == null || es.getApplicationState(DSE_GOSSIP_STATE) == null;
         this.verified = true;
      }

      if(!this.supported) {
         logger.error("ApplicationState.X_11_PADDING is already being used. This will create problems for workload assignment.");
      }

      return this.supported;
   }

   public synchronized void initialize() {
      if(!this.initialized && this.isSupported()) {
         try {
            Set<Workload> workloads = SystemInfo.getWorkloads();
            String workloadNames = Workload.workloadNames(workloads);
            if(this.newDseState) {
               Map<String, Object> state = this.doGetCurrentState();
               state.put("workload", Workload.legacyWorkloadName(workloads));
               state.put("workloads", workloadNames);
               state.put("graph", Boolean.valueOf(SystemInfo.isGraphNode()));
               state.put("dse_version", localVersion.toString());
               state.put("server_id", DseConfig.getServerId());
               state.put("active", Boolean.valueOf(false));
               String newValue = jsonMapper.writeValueAsString(state);
               Gossiper.instance.addLocalApplicationState(DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
            } else {
               logger.warn("Cannot set active status, graph, dse version or server id, looks like the {} system property is set to true!", "enable-old-dse-state");
               Gossiper.instance.addLocalApplicationState(DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(workloadNames));
            }
         } catch (IOException var5) {
            throw new RuntimeException(var5);
         }

         this.initialized = true;
      }

   }

   @VisibleForTesting
   public boolean isInitialized() {
      return this.initialized;
   }

   public Future<?> setIndexingStatusAsync(String core, DseState.CoreIndexingStatus status) {
      return this.gossipStateUpdater.submit(() -> {
         this.setIndexingStatusSync(core, status);
      });
   }

   public synchronized void setIndexingStatusSync(String core, DseState.CoreIndexingStatus status) {
      if(this.newDseState) {
         try {
            Map<String, Object> state = this.doGetCurrentState();
            Map<String, String> indexingStatus = this.doGetCoreIndexingStatus(state);
            switch(null.$SwitchMap$com$datastax$bdp$gms$DseState$CoreIndexingStatus[status.ordinal()]) {
            case 1:
            case 2:
               indexingStatus.put(core, status.toString());
               break;
            case 3:
               indexingStatus.remove(core);
            }

            state.put("coreIndexingStatus", indexingStatus);
            Set<String> indexingCores = new HashSet();
            Iterator var6 = indexingStatus.entrySet().iterator();

            while(var6.hasNext()) {
               Entry<String, String> entry = (Entry)var6.next();
               if(((String)entry.getValue()).equals(DseState.CoreIndexingStatus.INDEXING.toString())) {
                  indexingCores.add(entry.getKey());
               }
            }

            state.put("indexingCores", indexingCores);
            String newValue = jsonMapper.writeValueAsString(state);
            Gossiper.instance.addLocalApplicationState(DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
         } catch (IOException var8) {
            throw new RuntimeException(var8);
         }
      } else {
         logger.warn("Cannot set indexing status {} for core {}, looks like the {} system property is set to true!", new Object[]{status, core, "enable-old-dse-state"});
      }

   }

   public Future<?> setActiveStatusAsync(boolean active) {
      return this.gossipStateUpdater.submit(() -> {
         this.setActiveStatusSync(active);
      });
   }

   public synchronized void setActiveStatusSync(boolean active) {
      if(this.newDseState) {
         this.setBooleanApplicationState("active", active);
      } else {
         logger.warn("Cannot set active status, looks like the {} system property is set to true!", "enable-old-dse-state");
      }

   }

   public Future<?> setBlacklistedStatusAsync(final boolean blacklisted) {
      return this.gossipStateUpdater.submit(new Runnable() {
         public void run() {
            DseState.this.setBlacklistedStatusSync(blacklisted);
         }
      });
   }

   public synchronized void setBlacklistedStatusSync(boolean blacklisted) {
      if(this.newDseState) {
         this.setBooleanApplicationState("blacklisted", blacklisted);
      } else {
         logger.warn("Cannot set blacklisting status, looks like the {} system property is set to true!", "enable-old-dse-state");
      }

   }

   public void setNodeHealthAsync(double nodeHealth) {
      Preconditions.checkArgument(nodeHealth >= 0.0D && nodeHealth <= 1.0D, "Node Health cannot be less 0.0 or more than 1.0");
      this.gossipStateUpdater.submit(() -> {
         this.setNodeHealthSync(nodeHealth);
      });
   }

   private synchronized void setNodeHealthSync(double nodeHealth) {
      if(this.newDseState) {
         try {
            Map<String, Object> state = this.doGetCurrentState();
            state.put("health", Double.valueOf(nodeHealth));
            String newValue = jsonMapper.writeValueAsString(state);
            Gossiper.instance.addLocalApplicationState(DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
         } catch (Throwable var5) {
            logger.error("Failed to set the node health", var5);
         }
      } else {
         logger.warn("Cannot update node health, looks like the {} system property is set to true!", "enable-old-dse-state");
      }

   }

   public String getServerID(Map<String, Object> values) {
      Object id = values.get("server_id");
      return id == null?null:id.toString();
   }

   public Map<String, Object> getValues(EndpointState endpointState) {
      VersionedValue value = endpointState.getApplicationState(DSE_GOSSIP_STATE);
      return value != null?this.doGetVersionedValue(value):null;
   }

   public Map<String, Object> getValues(ApplicationState applicationState, VersionedValue value) {
      return applicationState.equals(DSE_GOSSIP_STATE)?this.doGetVersionedValue(value):null;
   }

   public Set<Workload> getWorkloads(Map<String, Object> values, InetAddress endpoint) {
      if(null != values.get("workloads")) {
         return Workload.fromString((String)values.get("workloads"), endpoint);
      } else {
         Boolean graphEnabled = this.getIsGraphNode(values);
         if(null == graphEnabled) {
            graphEnabled = Boolean.valueOf(false);
            logger.error(String.format("Node %s did not send a value for the %s field. Are you upgrading from 4.8 or earlier?", new Object[]{endpoint, "graph"}));
         }

         return Workload.fromLegacyWorkloadName((String)values.get("workload"), graphEnabled.booleanValue());
      }
   }

   public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(Map<String, Object> values) {
      Map<String, DseState.CoreIndexingStatus> coreIndexingStatus = new HashMap();
      Map<String, String> wireFormat = this.doGetCoreIndexingStatus(values);
      Iterator var4 = wireFormat.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<String, String> entry = (Entry)var4.next();
         coreIndexingStatus.put(entry.getKey(), DseState.CoreIndexingStatus.valueOf((String)entry.getValue()));
      }

      return coreIndexingStatus;
   }

   public Boolean getActiveStatus(Map<String, Object> values) {
      return this.getBoolean("active", values);
   }

   public Boolean getBlacklistingStatus(Map<String, Object> values) {
      return this.getBoolean("blacklisted", values);
   }

   public CassandraVersion getDseVersion(Map<String, Object> values) {
      Object version = values.get("dse_version");
      return version == null?null:new CassandraVersion(version.toString());
   }

   public Boolean getIsGraphNode(Map<String, Object> values) {
      Object value = values.get("graph");
      return value == null?null:Boolean.valueOf(Boolean.parseBoolean(value.toString()));
   }

   public double getNodeHealth(Map<String, Object> values) {
      Object nodeHealth = values.get("health");
      return nodeHealth != null?((Double)nodeHealth).doubleValue():0.0D;
   }

   private void setBooleanApplicationState(String key, boolean value) {
      try {
         Map<String, Object> state = this.doGetCurrentState();
         state.put(key, Boolean.toString(value));
         String newValue = jsonMapper.writeValueAsString(state);
         Gossiper.instance.addLocalApplicationState(DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
      } catch (IOException var5) {
         throw new RuntimeException(var5);
      }
   }

   private Boolean getBoolean(String key, Map<String, Object> values) {
      Object value = values.get(key);
      value = value != null?value:Boolean.FALSE;
      return Boolean.valueOf(Boolean.parseBoolean(value.toString()));
   }

   private Map<String, Object> doGetCurrentState() throws IOException {
      EndpointState currentState = Gossiper.instance.getEndpointStateForEndpoint(Addresses.Internode.getBroadcastAddress());
      if(currentState != null) {
         VersionedValue currentValue = currentState.getApplicationState(DSE_GOSSIP_STATE);
         if(currentValue != null) {
            return (Map)jsonMapper.readValue(currentValue.value, Map.class);
         }
      }

      return new HashMap();
   }

   private Map<String, Object> doGetVersionedValue(VersionedValue value) throws RuntimeException {
      try {
         return (Map)jsonMapper.readValue(value.value, Map.class);
      } catch (IOException var4) {
         logger.debug("Cannot parse new DSE state, assuming old state: {}", value.value);
         Map<String, Object> values = new HashMap();
         values.put("workload", value.value);
         values.put("workloads", value.value);
         return values;
      }
   }

   private Map<String, String> doGetCoreIndexingStatus(Map<String, Object> values) {
      Map<String, String> coreIndexingStatus = (Map)values.get("coreIndexingStatus");
      if(coreIndexingStatus == null) {
         coreIndexingStatus = new HashMap();
      }

      Set<String> indexingCore = this.doGetIndexingCores(values);
      Iterator var4 = indexingCore.iterator();

      while(var4.hasNext()) {
         String core = (String)var4.next();
         ((Map)coreIndexingStatus).put(core, DseState.CoreIndexingStatus.INDEXING.toString());
      }

      return (Map)coreIndexingStatus;
   }

   /** @deprecated */
   @Deprecated
   private Set<String> doGetIndexingCores(Map<String, Object> values) {
      Collection<String> cores = (Collection)values.get("indexingCores");
      return cores != null?Sets.newHashSet(cores):new HashSet();
   }

   static {
      DSE_GOSSIP_STATE = ApplicationState.X_11_PADDING;
      logger = LoggerFactory.getLogger(DseState.class);
      jsonMapper = new ObjectMapper();
      localVersion = new CassandraVersion(SystemInfo.getReleaseVersion("dse_version"));
   }

   public static enum CoreIndexingStatus {
      INDEXING,
      FINISHED,
      FAILED;

      private CoreIndexingStatus() {
      }
   }
}
