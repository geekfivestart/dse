package org.apache.cassandra.gms;

import com.datastax.bdp.db.upgrade.ClusterVersionBarrier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gossiper implements IFailureDetectionEventListener, GossiperMBean {
   public static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";
   private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("GossipTasks");
   static final ApplicationState[] STATES = ApplicationState.values();
   static final List<String> DEAD_STATES = Arrays.asList(new String[]{"removing", "removed", "LEFT", "hibernate"});
   static ArrayList<String> SILENT_SHUTDOWN_STATES = new ArrayList();
   protected AtomicInteger pendingEcho = new AtomicInteger();
   protected AtomicInteger failedEchos = new AtomicInteger();
   protected AtomicInteger successEchos = new AtomicInteger();
   private volatile ScheduledFuture<?> scheduledGossipTask;
   private static final ReentrantLock taskLock;
   public static final int intervalInMillis = 1000;
   public static final int QUARANTINE_DELAY;
   private static final Logger logger;
   public static final Gossiper instance;
   volatile long firstSynSendAt = 0L;
   public static final long aVeryLongTime = 259200000L;
   static final int MAX_GENERATION_DIFFERENCE = 31536000;
   public final ClusterVersionBarrier clusterVersionBarrier;
   private long fatClientTimeout;
   private final Random random = new Random();
   private final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>() {
      public int compare(InetAddress addr1, InetAddress addr2) {
         return addr1.getHostAddress().compareTo(addr2.getHostAddress());
      }
   };
   private final List<IEndpointStateChangeSubscriber> subscribers = new CopyOnWriteArrayList();
   private final Set<InetAddress> liveEndpoints;
   private final Map<InetAddress, Long> unreachableEndpoints;
   @VisibleForTesting
   final Set<InetAddress> seeds;
   final ConcurrentMap<InetAddress, EndpointState> endpointStateMap;
   private final Map<InetAddress, Long> justRemovedEndpoints;
   private final Map<InetAddress, Long> expireTimeEndpointMap;
   private volatile boolean inShadowRound;
   private final Set<InetAddress> seedsInShadowRound;
   private final Map<InetAddress, EndpointState> endpointShadowStateMap;
   private volatile long lastProcessedMessageAt;

   Gossiper(boolean registerJmx) {
      this.liveEndpoints = new ConcurrentSkipListSet(this.inetcomparator);
      this.unreachableEndpoints = new ConcurrentHashMap();
      this.seeds = new ConcurrentSkipListSet(this.inetcomparator);
      this.endpointStateMap = new ConcurrentHashMap();
      this.justRemovedEndpoints = new ConcurrentHashMap();
      this.expireTimeEndpointMap = new ConcurrentHashMap();
      this.inShadowRound = false;
      this.seedsInShadowRound = new ConcurrentSkipListSet(this.inetcomparator);
      this.endpointShadowStateMap = new ConcurrentHashMap();
      this.lastProcessedMessageAt = System.currentTimeMillis();
      this.fatClientTimeout = (long)(QUARANTINE_DELAY / 2);
      FailureDetector.instance.registerFailureDetectionEventListener(this);
      this.clusterVersionBarrier = new ClusterVersionBarrier(this::getAllEndpoints, this::getEndpointInfo);
      if(registerJmx) {
         try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.net:type=Gossiper"));
         } catch (Exception var3) {
            throw new RuntimeException(var3);
         }
      }

   }

   public void registerUpgradeBarrierListener() {
      this.register(new IEndpointStateChangeSubscriber() {
         public void onJoin(InetAddress endpoint, EndpointState state) {
            Gossiper.logger.trace("ClusterVersionBarrier/IEndpointStateChangeSubscriber - onJoin {} / {}", endpoint, state);
            Gossiper.this.clusterVersionBarrier.scheduleUpdateVersions();
         }

         public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
         }

         public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
            if(state == ApplicationState.RELEASE_VERSION || state == ApplicationState.SCHEMA) {
               Gossiper.logger.trace("ClusterVersionBarrier/IEndpointStateChangeSubscriber - onChange {} / {} = {}", new Object[]{endpoint, state, value});
               Gossiper.this.clusterVersionBarrier.scheduleUpdateVersions();
            }

         }

         public void onAlive(InetAddress endpoint, EndpointState state) {
            Gossiper.logger.trace("ClusterVersionBarrier/IEndpointStateChangeSubscriber - onAlive {} / {}", endpoint, state);
            Gossiper.this.clusterVersionBarrier.scheduleUpdateVersions();
         }

         public void onDead(InetAddress endpoint, EndpointState state) {
         }

         public void onRemove(InetAddress endpoint) {
         }

         public void afterRemove(InetAddress endpoint) {
            Gossiper.logger.trace("ClusterVersionBarrier/IEndpointStateChangeSubscriber - onRemove {}", endpoint);
            Gossiper.this.clusterVersionBarrier.scheduleUpdateVersions();
         }

         public void onRestart(InetAddress endpoint, EndpointState state) {
            Gossiper.logger.trace("ClusterVersionBarrier/IEndpointStateChangeSubscriber - onRestart {} / {}", endpoint, state);
            Gossiper.this.clusterVersionBarrier.scheduleUpdateVersions();
         }
      });
   }

   public void onNewMessageProcessed() {
      this.lastProcessedMessageAt = System.currentTimeMillis();
   }

   public boolean seenAnySeed() {
      Iterator var1 = this.endpointStateMap.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var1.next();
         if(this.seeds.contains(entry.getKey())) {
            return true;
         }

         try {
            VersionedValue internalIp = ((EndpointState)entry.getValue()).getApplicationState(ApplicationState.INTERNAL_IP);
            if(internalIp != null && this.seeds.contains(InetAddress.getByName(internalIp.value))) {
               return true;
            }
         } catch (UnknownHostException var4) {
            throw new RuntimeException(var4);
         }
      }

      return false;
   }

   public void register(IEndpointStateChangeSubscriber subscriber) {
      this.subscribers.add(subscriber);
   }

   public void unregister(IEndpointStateChangeSubscriber subscriber) {
      this.subscribers.remove(subscriber);
   }

   public Iterable<InetAddress> getAllEndpoints() {
      return Iterables.concat(Collections.singleton(FBUtilities.getBroadcastAddress()), this.liveEndpoints, this.unreachableEndpoints.keySet());
   }

   public Set<InetAddress> getLiveMembers() {
      Set<InetAddress> liveMembers = new HashSet(this.liveEndpoints);
      if(!liveMembers.contains(FBUtilities.getBroadcastAddress())) {
         liveMembers.add(FBUtilities.getBroadcastAddress());
      }

      return liveMembers;
   }

   public Set<InetAddress> getLiveTokenOwners() {
      return StorageService.instance.getLiveRingMembers(true);
   }

   public Set<InetAddress> getUnreachableMembers() {
      return this.unreachableEndpoints.keySet();
   }

   public Set<InetAddress> getUnreachableTokenOwners() {
      Set<InetAddress> tokenOwners = new HashSet();
      Iterator var2 = this.unreachableEndpoints.keySet().iterator();

      while(var2.hasNext()) {
         InetAddress endpoint = (InetAddress)var2.next();
         if(StorageService.instance.getTokenMetadata().isMember(endpoint)) {
            tokenOwners.add(endpoint);
         }
      }

      return tokenOwners;
   }

   public long getEndpointDowntime(InetAddress ep) {
      Long downtime = (Long)this.unreachableEndpoints.get(ep);
      return downtime != null?TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - downtime.longValue()):0L;
   }

   private boolean isShutdown(InetAddress endpoint) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(epState == null) {
         return false;
      } else if(epState.getApplicationState(ApplicationState.STATUS) == null) {
         return false;
      } else {
         String value = epState.getApplicationState(ApplicationState.STATUS).value;
         String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);

         assert pieces.length > 0;

         String state = pieces[0];
         return state.equals("shutdown");
      }
   }

   public void convict(InetAddress endpoint, double phi) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(epState != null) {
         if(epState.isAlive()) {
            logger.debug("Convicting {} with status {} - alive {}", new Object[]{endpoint, getGossipStatus(epState), Boolean.valueOf(epState.isAlive())});
            if(this.isShutdown(endpoint)) {
               this.markAsShutdown(endpoint);
            } else {
               this.markDead(endpoint, epState);
            }

         }
      }
   }

   protected void markAsShutdown(InetAddress endpoint) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(epState != null) {
         epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
         epState.addApplicationState(ApplicationState.NATIVE_TRANSPORT_READY, StorageService.instance.valueFactory.nativeTransportReady(false));
         epState.getHeartBeatState().forceHighestPossibleVersionUnsafe();
         this.markDead(endpoint, epState);
         FailureDetector.instance.forceConviction(endpoint);
      }
   }

   int getMaxEndpointStateVersion(EndpointState epState) {
      int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();

      Entry state;
      for(Iterator var3 = epState.states().iterator(); var3.hasNext(); maxVersion = Math.max(maxVersion, ((VersionedValue)state.getValue()).version)) {
         state = (Entry)var3.next();
      }

      return maxVersion;
   }

   private void evictFromMembership(InetAddress endpoint) {
      this.unreachableEndpoints.remove(endpoint);
      this.endpointStateMap.remove(endpoint);
      this.expireTimeEndpointMap.remove(endpoint);
      FailureDetector.instance.remove(endpoint);
      this.quarantineEndpoint(endpoint);
      if(logger.isDebugEnabled()) {
         logger.debug("evicting {} from gossip", endpoint);
      }

   }

   public void removeEndpoint(InetAddress endpoint) {
      Iterator var2 = this.subscribers.iterator();

      IEndpointStateChangeSubscriber subscriber;
      while(var2.hasNext()) {
         subscriber = (IEndpointStateChangeSubscriber)var2.next();
         subscriber.onRemove(endpoint);
      }

      if(this.seeds.contains(endpoint)) {
         this.buildSeedsList(this.seeds);
         this.seeds.remove(endpoint);
         logger.info("removed {} from seeds, updated seeds list = {}", endpoint, this.seeds);
      }

      this.liveEndpoints.remove(endpoint);
      this.unreachableEndpoints.remove(endpoint);
      MessagingService.instance().resetVersion(endpoint);
      this.quarantineEndpoint(endpoint);
      this.destroyMessagingConnection(endpoint);
      var2 = this.subscribers.iterator();

      while(var2.hasNext()) {
         subscriber = (IEndpointStateChangeSubscriber)var2.next();
         subscriber.afterRemove(endpoint);
      }

      if(logger.isDebugEnabled()) {
         logger.debug("removing endpoint {}", endpoint);
      }

   }

   private void destroyMessagingConnection(InetAddress endpoint) {
      long delay = Long.getLong("cassandra.messaging_destroy_delay_in_ms", DatabaseDescriptor.getMaxRpcTimeout()).longValue();
      if(delay <= 0L) {
         MessagingService.instance().destroyConnectionPool(endpoint);
      } else {
         ScheduledExecutors.optionalTasks.schedule(() -> {
            if(!this.liveEndpoints.contains(endpoint) && !this.unreachableEndpoints.containsKey(endpoint)) {
               logger.info("Destroying messaging connection to {} due to endpoint being removed from cluster", endpoint);
               MessagingService.instance().destroyConnectionPool(endpoint);
            } else {
               logger.info("Not destroying messaging connection to {} due to endpoint starting to gossip again", endpoint);
            }

         }, delay, TimeUnit.MILLISECONDS);
      }

   }

   private void quarantineEndpoint(InetAddress endpoint) {
      this.quarantineEndpoint(endpoint, System.currentTimeMillis());
   }

   private void quarantineEndpoint(InetAddress endpoint, long quarantineExpiration) {
      this.justRemovedEndpoints.put(endpoint, Long.valueOf(quarantineExpiration));
   }

   public void replacementQuarantine(InetAddress endpoint) {
      logger.debug("");
      this.quarantineEndpoint(endpoint, System.currentTimeMillis() + (long)QUARANTINE_DELAY);
   }

   public void replacedEndpoint(InetAddress endpoint) {
      this.removeEndpoint(endpoint);
      this.evictFromMembership(endpoint);
      this.replacementQuarantine(endpoint);
   }

   private void makeRandomGossipDigest(List<GossipDigest> gDigests) {
      int generation = 0;
      int maxVersion = 0;
      List<InetAddress> endpoints = new ArrayList(this.endpointStateMap.keySet());
      Collections.shuffle(endpoints, this.random);

      InetAddress endpoint;
      for(Iterator var6 = endpoints.iterator(); var6.hasNext(); gDigests.add(new GossipDigest(endpoint, generation, maxVersion))) {
         endpoint = (InetAddress)var6.next();
         EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
         if(epState != null) {
            generation = epState.getHeartBeatState().getGeneration();
            maxVersion = this.getMaxEndpointStateVersion(epState);
         }
      }

      if(logger.isTraceEnabled()) {
         StringBuilder sb = new StringBuilder();
         Iterator var10 = gDigests.iterator();

         while(var10.hasNext()) {
            GossipDigest gDigest = (GossipDigest)var10.next();
            sb.append(gDigest);
            sb.append(" ");
         }

         logger.trace("Gossip Digests are : {}", sb);
      }

   }

   public void advertiseRemoving(InetAddress endpoint, UUID hostId, UUID localHostId) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      int generation = epState.getHeartBeatState().getGeneration();
      logger.info("Removing host: {}", hostId);
      logger.info("Sleeping for {}ms to ensure {} does not change", Integer.valueOf(StorageService.RING_DELAY), endpoint);
      Uninterruptibles.sleepUninterruptibly((long)StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
      epState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(epState.getHeartBeatState().getGeneration() != generation) {
         throw new RuntimeException("Endpoint " + endpoint + " generation changed while trying to remove it");
      } else {
         logger.info("Advertising removal for {}", endpoint);
         epState.updateTimestamp();
         epState.getHeartBeatState().forceNewerGenerationUnsafe();
         Map<ApplicationState, VersionedValue> states = new EnumMap(ApplicationState.class);
         states.put(ApplicationState.STATUS, StorageService.instance.valueFactory.removingNonlocal(hostId));
         states.put(ApplicationState.REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(localHostId));
         epState.addApplicationStates((Map)states);
         this.endpointStateMap.put(endpoint, epState);
      }
   }

   public void advertiseTokenRemoved(InetAddress endpoint, UUID hostId) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      epState.updateTimestamp();
      epState.getHeartBeatState().forceNewerGenerationUnsafe();
      long expireTime = computeExpireTime();
      epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.removedNonlocal(hostId, expireTime));
      logger.info("Completing removal of {}", endpoint);
      this.addExpireTimeForEndpoint(endpoint, expireTime);
      this.endpointStateMap.put(endpoint, epState);
      Uninterruptibles.sleepUninterruptibly(2000L, TimeUnit.MILLISECONDS);
   }

   public void unsafeAssassinateEndpoint(String address) throws UnknownHostException {
      logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinateEndpoint instead");
      this.assassinateEndpoint(address);
   }

   public void assassinateEndpoint(String address) throws UnknownHostException {
      InetAddress endpoint = InetAddress.getByName(address);
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      logger.warn("Assassinating {} via gossip", endpoint);
      if(epState == null) {
         epState = new EndpointState(new HeartBeatState((int)((System.currentTimeMillis() + 60000L) / 1000L), 9999));
      } else {
         int generation = epState.getHeartBeatState().getGeneration();
         int heartbeat = epState.getHeartBeatState().getHeartBeatVersion();
         logger.info("Sleeping for {}ms to ensure {} does not change", Integer.valueOf(StorageService.RING_DELAY), endpoint);
         Uninterruptibles.sleepUninterruptibly((long)StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
         EndpointState newState = (EndpointState)this.endpointStateMap.get(endpoint);
         if(newState == null) {
            logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
         } else {
            if(newState.getHeartBeatState().getGeneration() != generation) {
               throw new RuntimeException("Endpoint still alive: " + endpoint + " generation changed while trying to assassinate it");
            }

            if(newState.getHeartBeatState().getHeartBeatVersion() != heartbeat) {
               throw new RuntimeException("Endpoint still alive: " + endpoint + " heartbeat changed while trying to assassinate it");
            }
         }

         epState.updateTimestamp();
         epState.getHeartBeatState().forceNewerGenerationUnsafe();
      }

      Object tokens = null;

      try {
         tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
      } catch (Throwable var7) {
         JVMStabilityInspector.inspectThrowable(var7);
      }

      if(tokens == null || ((Collection)tokens).isEmpty()) {
         logger.warn("Trying to assassinate an endpoint {} that does not have any tokens assigned. This should not have happened, trying to continue with a random token.", address);
         tokens = Collections.singletonList(StorageService.instance.getTokenMetadata().partitioner.getRandomToken());
      }

      epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.left((Collection)tokens, computeExpireTime()));
      this.handleMajorStateChange(endpoint, epState);
      Uninterruptibles.sleepUninterruptibly(4000L, TimeUnit.MILLISECONDS);
      logger.warn("Finished assassinating {}", endpoint);
   }

   public void reviveEndpoint(String address) throws UnknownHostException {
      InetAddress endpoint = InetAddress.getByName(address);
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      logger.warn("Reviving {} via gossip", endpoint);
      if(epState == null) {
         throw new RuntimeException("Cannot revive endpoint " + endpoint + ": no endpoint-state");
      } else {
         int generation = epState.getHeartBeatState().getGeneration();
         int heartbeat = epState.getHeartBeatState().getHeartBeatVersion();
         logger.info("Have endpoint-state for {}: status={}, generation={}, heartbeat={}", new Object[]{endpoint, epState.getStatus(), Integer.valueOf(generation), Integer.valueOf(heartbeat)});
         if(!this.isSilentShutdownState(epState)) {
            throw new RuntimeException("Cannot revive endpoint " + endpoint + ": not in a (silent) shutdown state: " + epState.getStatus());
         } else if(FailureDetector.instance.isAlive(endpoint)) {
            throw new RuntimeException("Cannot revive endpoint " + endpoint + ": still alive (failure-detector)");
         } else {
            logger.info("Sleeping for {}ms to ensure {} does not change", Integer.valueOf(StorageService.RING_DELAY), endpoint);
            Uninterruptibles.sleepUninterruptibly((long)StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
            EndpointState newState = (EndpointState)this.endpointStateMap.get(endpoint);
            if(newState == null) {
               throw new RuntimeException("Cannot revive endpoint " + endpoint + ": endpoint-state disappeared");
            } else if(newState.getHeartBeatState().getGeneration() != generation) {
               throw new RuntimeException("Cannot revive endpoint " + endpoint + ": still alive, generation changed while trying to reviving it");
            } else if(newState.getHeartBeatState().getHeartBeatVersion() != heartbeat) {
               throw new RuntimeException("Cannot revive endpoint " + endpoint + ": still alive, heartbeat changed while trying to reviving it");
            } else {
               epState.updateTimestamp();
               epState.getHeartBeatState().forceNewerGenerationUnsafe();
               Collection<Token> tokens = this.getTokensFromEndpointState(epState, DatabaseDescriptor.getPartitioner());
               if(tokens != null && !tokens.isEmpty()) {
                  epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.normal(tokens));
                  this.handleMajorStateChange(endpoint, epState);
                  Uninterruptibles.sleepUninterruptibly(4000L, TimeUnit.MILLISECONDS);
                  logger.warn("Finished reviving {}, status={}, generation={}, heartbeat={}", new Object[]{endpoint, epState.getStatus(), Integer.valueOf(generation), Integer.valueOf(heartbeat)});
               } else {
                  throw new RuntimeException("Cannot revive endpoint " + endpoint + ": no tokens from TokenMetadata");
               }
            }
         }
      }
   }

   public void unsafeSetEndpointState(String address, String status) throws UnknownHostException {
      logger.warn("Forcibly changing gossip status of " + address + " to " + status);
      InetAddress endpoint = InetAddress.getByName(address);
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(epState == null) {
         throw new RuntimeException("No state for endpoint " + endpoint);
      } else {
         int generation = epState.getHeartBeatState().getGeneration();
         int heartbeat = epState.getHeartBeatState().getHeartBeatVersion();
         logger.info("Have endpoint-state for {}: status={}, generation={}, heartbeat={}", new Object[]{endpoint, epState.getStatus(), Integer.valueOf(generation), Integer.valueOf(heartbeat)});
         if(FailureDetector.instance.isAlive(endpoint)) {
            throw new RuntimeException("Cannot update status for endpoint " + endpoint + ": still alive (failure-detector)");
         } else {
            Collection<Token> tokens = this.getTokensFromEndpointState(epState, DatabaseDescriptor.getPartitioner());
            String var9 = status.toLowerCase();
            byte var10 = -1;
            switch(var9.hashCode()) {
            case -1039745817:
               if(var9.equals("normal")) {
                  var10 = 1;
               }
               break;
            case -903964590:
               if(var9.equals("hibernate")) {
                  var10 = 0;
               }
               break;
            case -169343402:
               if(var9.equals("shutdown")) {
                  var10 = 3;
               }
               break;
            case 3317767:
               if(var9.equals("left")) {
                  var10 = 2;
               }
            }

            VersionedValue newStatus;
            switch(var10) {
            case 0:
               newStatus = StorageService.instance.valueFactory.hibernate(true);
               break;
            case 1:
               newStatus = StorageService.instance.valueFactory.normal(tokens);
               break;
            case 2:
               newStatus = StorageService.instance.valueFactory.left(tokens, computeExpireTime());
               break;
            case 3:
               newStatus = StorageService.instance.valueFactory.shutdown(true);
               break;
            default:
               throw new IllegalArgumentException("Unknown status '" + status + '\'');
            }

            epState.updateTimestamp();
            epState.getHeartBeatState().forceNewerGenerationUnsafe();
            epState.addApplicationState(ApplicationState.STATUS, newStatus);
            this.handleMajorStateChange(endpoint, epState);
            logger.warn("Forcibly changed gossip status of " + endpoint + " to " + newStatus);
         }
      }
   }

   public boolean isKnownEndpoint(InetAddress endpoint) {
      return this.endpointStateMap.containsKey(endpoint);
   }

   public Collection<Token> getTokensFor(InetAddress endpoint, IPartitioner partitioner) {
      EndpointState state = this.getEndpointStateForEndpoint(endpoint);
      return (Collection)(state == null?Collections.emptyList():this.getTokensFromEndpointState(state, partitioner));
   }

   public Collection<Token> getTokensFromEndpointState(EndpointState state, IPartitioner partitioner) {
      try {
         VersionedValue versionedValue = state.getApplicationState(ApplicationState.TOKENS);
         return (Collection)(versionedValue == null?Collections.emptyList():TokenSerializer.deserialize(partitioner, new DataInputStream(new ByteArrayInputStream(versionedValue.toBytes()))));
      } catch (IOException var4) {
         throw new RuntimeException(var4);
      }
   }

   public int getCurrentGenerationNumber(InetAddress endpoint) {
      return ((EndpointState)this.endpointStateMap.get(endpoint)).getHeartBeatState().getGeneration();
   }

   private boolean sendGossip(GossipDigestSyn message, Set<InetAddress> epSet) {
      List<InetAddress> liveEndpoints = ImmutableList.copyOf(epSet);
      int size = liveEndpoints.size();
      if(size < 1) {
         return false;
      } else {
         int index = size == 1?0:this.random.nextInt(size);
         InetAddress to = (InetAddress)liveEndpoints.get(index);
         if(logger.isTraceEnabled()) {
            logger.trace("Sending a GossipDigestSyn to {} ...", to);
         }

         if(this.firstSynSendAt == 0L) {
            this.firstSynSendAt = System.nanoTime();
         }

         MessagingService.instance().send(Verbs.GOSSIP.SYN.newRequest(to, (Object)message));
         return this.seeds.contains(to);
      }
   }

   private boolean doGossipToLiveMember(GossipDigestSyn message) {
      int size = this.liveEndpoints.size();
      return size == 0?false:this.sendGossip(message, this.liveEndpoints);
   }

   private void maybeGossipToUnreachableMember(GossipDigestSyn message) {
      double liveEndpointCount = (double)this.liveEndpoints.size();
      double unreachableEndpointCount = (double)this.unreachableEndpoints.size();
      if(unreachableEndpointCount > 0.0D) {
         double prob = unreachableEndpointCount / (liveEndpointCount + 1.0D);
         double randDbl = this.random.nextDouble();
         if(randDbl < prob) {
            this.sendGossip(message, this.unreachableEndpoints.keySet());
         }
      }

   }

   private void gossipToSeed(GossipDigestSyn prod) {
      int size = this.seeds.size();
      if(size > 0) {
         if(size == 1 && this.seeds.contains(FBUtilities.getBroadcastAddress())) {
            return;
         }

         double probability = this.getSeedGossipProbability();
         double randDbl = this.random.nextDouble();
         if(randDbl <= probability) {
            this.sendGossip(prod, this.seeds);
         }
      }

   }

   public boolean isGossipOnlyMember(InetAddress endpoint) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
      return epState == null?false:!this.isDeadState(epState) && !StorageService.instance.getTokenMetadata().isMember(endpoint);
   }

   public boolean isSafeForStartup(InetAddress endpoint, UUID localHostUUID, boolean isBootstrapping, Map<InetAddress, EndpointState> epStates) {
      EndpointState epState = (EndpointState)epStates.get(endpoint);
      if(epState != null && !this.isDeadState(epState)) {
         if(isBootstrapping) {
            String status = getGossipStatus(epState);
            List<String> unsafeStatuses = new ArrayList<String>() {
               {
                  this.add("");
                  this.add("NORMAL");
                  this.add("shutdown");
               }
            };
            return !unsafeStatuses.contains(status);
         } else {
            VersionedValue previous = epState.getApplicationState(ApplicationState.HOST_ID);
            return UUID.fromString(previous.value).equals(localHostUUID);
         }
      } else {
         return true;
      }
   }

   private void doStatusCheck() {
      if(logger.isTraceEnabled()) {
         logger.trace("Performing status check ...");
      }

      long now = System.currentTimeMillis();
      long nowNano = System.nanoTime();
      long pending = ((Long)((JMXEnabledThreadPoolExecutor)StageManager.getStage(Stage.GOSSIP)).metrics.pendingTasks.getValue()).longValue();
      if(pending > 0L && this.lastProcessedMessageAt < now - 1000L) {
         Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
         if(this.lastProcessedMessageAt < now - 1000L) {
            logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)", Long.valueOf(pending));
            return;
         }
      }

      Set<InetAddress> eps = this.endpointStateMap.keySet();
      Iterator var8 = eps.iterator();

      while(var8.hasNext()) {
         InetAddress endpoint = (InetAddress)var8.next();
         if(!endpoint.equals(FBUtilities.getBroadcastAddress())) {
            FailureDetector.instance.interpret(endpoint);
            EndpointState epState = (EndpointState)this.endpointStateMap.get(endpoint);
            if(epState != null) {
               if(this.isGossipOnlyMember(endpoint) && !this.justRemovedEndpoints.containsKey(endpoint) && TimeUnit.NANOSECONDS.toMillis(nowNano - epState.getUpdateTimestamp()) > this.fatClientTimeout) {
                  logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, Long.valueOf(this.fatClientTimeout));
                  this.removeEndpoint(endpoint);
                  this.evictFromMembership(endpoint);
               }

               long expireTime = this.getExpireTimeForEndpoint(endpoint);
               if(!epState.isAlive() && now > expireTime && !StorageService.instance.getTokenMetadata().isMember(endpoint)) {
                  if(logger.isDebugEnabled()) {
                     logger.debug("time is expiring for endpoint : {} ({})", endpoint, Long.valueOf(expireTime));
                  }

                  this.evictFromMembership(endpoint);
               }
            }
         }
      }

      if(!this.justRemovedEndpoints.isEmpty()) {
         var8 = this.justRemovedEndpoints.entrySet().iterator();

         while(var8.hasNext()) {
            Entry<InetAddress, Long> entry = (Entry)var8.next();
            if(now - ((Long)entry.getValue()).longValue() > (long)QUARANTINE_DELAY) {
               if(logger.isDebugEnabled()) {
                  logger.debug("{} elapsed, {} gossip quarantine over", Integer.valueOf(QUARANTINE_DELAY), entry.getKey());
               }

               this.justRemovedEndpoints.remove(entry.getKey());
            }
         }
      }

   }

   protected long getExpireTimeForEndpoint(InetAddress endpoint) {
      Long storedTime = (Long)this.expireTimeEndpointMap.get(endpoint);
      return storedTime == null?computeExpireTime():storedTime.longValue();
   }

   public EndpointState getEndpointStateForEndpoint(InetAddress ep) {
      return (EndpointState)this.endpointStateMap.get(ep);
   }

   public Set<Entry<InetAddress, EndpointState>> getEndpointStates() {
      return this.endpointStateMap.entrySet();
   }

   public UUID getHostId(InetAddress endpoint) {
      return this.getHostId(endpoint, this.endpointStateMap);
   }

   public UUID getHostId(InetAddress endpoint, Map<InetAddress, EndpointState> epStates) {
      return UUID.fromString(((EndpointState)epStates.get(endpoint)).getApplicationState(ApplicationState.HOST_ID).value);
   }

   EndpointState getStateForVersionBiggerThan(InetAddress forEndpoint, int version) {
      EndpointState epState = (EndpointState)this.endpointStateMap.get(forEndpoint);
      EndpointState reqdEndpointState = null;
      if(epState != null) {
         HeartBeatState heartBeatState = epState.getHeartBeatState();
         int localHbGeneration = heartBeatState.getGeneration();
         int localHbVersion = heartBeatState.getHeartBeatVersion();
         if(localHbVersion > version) {
            reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
            if(logger.isTraceEnabled()) {
               logger.trace("local heartbeat version {} greater than {} for {}", new Object[]{Integer.valueOf(localHbVersion), Integer.valueOf(version), forEndpoint});
            }
         }

         Map<ApplicationState, VersionedValue> states = new EnumMap(ApplicationState.class);
         Iterator var9 = epState.states().iterator();

         while(var9.hasNext()) {
            Entry<ApplicationState, VersionedValue> entry = (Entry)var9.next();
            VersionedValue value = (VersionedValue)entry.getValue();
            if(value.version > version) {
               if(reqdEndpointState == null) {
                  reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
               }

               ApplicationState key = (ApplicationState)entry.getKey();
               if(logger.isTraceEnabled()) {
                  logger.trace("Adding state {}: {}", key, value.value);
               }

               states.put(key, value);
            }
         }

         if(reqdEndpointState != null) {
            reqdEndpointState.addApplicationStates((Map)states);
         }
      }

      return reqdEndpointState;
   }

   public int compareEndpointStartup(InetAddress addr1, InetAddress addr2) {
      EndpointState ep1 = this.getEndpointStateForEndpoint(addr1);
      EndpointState ep2 = this.getEndpointStateForEndpoint(addr2);

      assert ep1 != null && ep2 != null;

      return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
   }

   void notifyFailureDetector(Map<InetAddress, EndpointState> remoteEpStateMap) {
      Iterator var2 = remoteEpStateMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var2.next();
         this.notifyFailureDetector((InetAddress)entry.getKey(), (EndpointState)entry.getValue());
      }

   }

   void notifyFailureDetector(InetAddress endpoint, EndpointState remoteEndpointState) {
      EndpointState localEndpointState = (EndpointState)this.endpointStateMap.get(endpoint);
      if(localEndpointState != null) {
         IFailureDetector fd = FailureDetector.instance;
         int localGeneration = localEndpointState.getHeartBeatState().getGeneration();
         int remoteGeneration = remoteEndpointState.getHeartBeatState().getGeneration();
         if(remoteGeneration > localGeneration) {
            localEndpointState.updateTimestamp();
            if(!localEndpointState.isAlive()) {
               logger.debug("Clearing interval times for {} due to generation change", endpoint);
               fd.remove(endpoint);
            }

            fd.report(endpoint);
            return;
         }

         if(remoteGeneration == localGeneration) {
            int localVersion = this.getMaxEndpointStateVersion(localEndpointState);
            int remoteVersion = remoteEndpointState.getHeartBeatState().getHeartBeatVersion();
            if(remoteVersion > localVersion) {
               localEndpointState.updateTimestamp();
               fd.report(endpoint);
            }
         }
      }

   }

   private void markAlive(InetAddress addr, EndpointState localState) {
      localState.markDead();
      logger.trace("Sending a EchoMessage to {}", addr);
      MessagingService.instance().sendSingleTarget(Verbs.GOSSIP.ECHO.newRequest(addr, EmptyPayload.instance)).thenAccept((msg) -> {
         this.realMarkAlive(addr, localState);
         this.pendingEcho.decrementAndGet();
         this.successEchos.incrementAndGet();
      }).exceptionally((th) -> {
         this.pendingEcho.decrementAndGet();
         this.failedEchos.incrementAndGet();
         return null;
      });
   }

   @VisibleForTesting
   public void realMarkAlive(InetAddress addr, EndpointState localState) {
      if(logger.isTraceEnabled()) {
         logger.trace("marking as alive {}", addr);
      }

      localState.markAlive();
      localState.updateTimestamp();
      this.liveEndpoints.add(addr);
      this.unreachableEndpoints.remove(addr);
      this.expireTimeEndpointMap.remove(addr);
      logger.debug("removing expire time for endpoint : {}", addr);
      logger.info("InetAddress {} is now UP", addr);
      Iterator var3 = this.subscribers.iterator();

      while(var3.hasNext()) {
         IEndpointStateChangeSubscriber subscriber = (IEndpointStateChangeSubscriber)var3.next();
         subscriber.onAlive(addr, localState);
      }

      if(logger.isTraceEnabled()) {
         logger.trace("Notified {}", this.subscribers);
      }

   }

   @VisibleForTesting
   public void markDead(InetAddress addr, EndpointState localState) {
      if(logger.isTraceEnabled()) {
         logger.trace("marking as down {}", addr);
      }

      localState.markDead();
      this.liveEndpoints.remove(addr);
      this.unreachableEndpoints.put(addr, Long.valueOf(System.nanoTime()));
      logger.info("InetAddress {} is now DOWN", addr);
      Iterator var3 = this.subscribers.iterator();

      while(var3.hasNext()) {
         IEndpointStateChangeSubscriber subscriber = (IEndpointStateChangeSubscriber)var3.next();
         subscriber.onDead(addr, localState);
      }

      if(logger.isTraceEnabled()) {
         logger.trace("Notified {}", this.subscribers);
      }

   }

   private void handleMajorStateChange(InetAddress ep, EndpointState epState) {
      assert epState != null;

      EndpointState localEpState = (EndpointState)this.endpointStateMap.get(ep);
      if(!this.isDeadState(epState)) {
         if(localEpState != null) {
            logger.info("Node {} has restarted, now UP", ep);
         } else {
            logger.info("Node {} is now part of the cluster", ep);
         }
      }

      if(logger.isTraceEnabled()) {
         logger.trace("Adding endpoint state for {}", ep);
      }

      this.endpointStateMap.put(ep, epState);
      Iterator var4 = this.subscribers.iterator();

      IEndpointStateChangeSubscriber subscriber;
      while(var4.hasNext()) {
         subscriber = (IEndpointStateChangeSubscriber)var4.next();
         subscriber.onStarted(ep, localEpState == null, epState);
      }

      if(localEpState != null) {
         var4 = this.subscribers.iterator();

         while(var4.hasNext()) {
            subscriber = (IEndpointStateChangeSubscriber)var4.next();
            subscriber.onRestart(ep, localEpState);
         }
      }

      if(!this.isDeadState(epState)) {
         this.markAlive(ep, epState);
      } else {
         logger.debug("Not marking {} alive due to dead state", ep);
         this.markDead(ep, epState);
      }

      logger.info("WRITING LOCAL JOIN INFO to " + this.subscribers);
      var4 = this.subscribers.iterator();

      while(var4.hasNext()) {
         subscriber = (IEndpointStateChangeSubscriber)var4.next();
         subscriber.onJoin(ep, epState);
      }

      if(this.isShutdown(ep)) {
         this.markAsShutdown(ep);
      }

   }

   public boolean isAlive(InetAddress endpoint) {
      EndpointState epState = this.getEndpointStateForEndpoint(endpoint);
      return epState == null?false:epState.isAlive() && !this.isDeadState(epState);
   }

   public boolean isDeadState(EndpointState epState) {
      String status = getGossipStatus(epState);
      return status.isEmpty()?false:DEAD_STATES.contains(status);
   }

   public boolean isSilentShutdownState(EndpointState epState) {
      String status = getGossipStatus(epState);
      return status.isEmpty()?false:SILENT_SHUTDOWN_STATES.contains(status);
   }

   private static String getGossipStatus(EndpointState epState) {
      if(epState != null && epState.getApplicationState(ApplicationState.STATUS) != null) {
         String value = epState.getApplicationState(ApplicationState.STATUS).value;
         String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);

         assert pieces.length > 0;

         return pieces[0];
      } else {
         return "";
      }
   }

   void applyStateLocally(Map<InetAddress, EndpointState> epStateMap) {
      Iterator var2 = epStateMap.entrySet().iterator();

      while(true) {
         Entry entry;
         InetAddress ep;
         do {
            if(!var2.hasNext()) {
               return;
            }

            entry = (Entry)var2.next();
            ep = (InetAddress)entry.getKey();
         } while(ep.equals(FBUtilities.getBroadcastAddress()) && !this.isInShadowRound());

         if(this.justRemovedEndpoints.containsKey(ep)) {
            if(logger.isTraceEnabled()) {
               logger.trace("Ignoring gossip for {} because it is quarantined", ep);
            }
         } else {
            EndpointState localEpStatePtr = (EndpointState)this.endpointStateMap.get(ep);
            EndpointState remoteState = (EndpointState)entry.getValue();
            if(localEpStatePtr != null) {
               int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
               int remoteGeneration = remoteState.getHeartBeatState().getGeneration();
               long localTime = System.currentTimeMillis() / 1000L;
               if(logger.isTraceEnabled()) {
                  logger.trace("{} local generation {}, remote generation {}", new Object[]{ep, Integer.valueOf(localGeneration), Integer.valueOf(remoteGeneration)});
               }

               if((long)remoteGeneration > localTime + 31536000L) {
                  logger.warn("received an invalid gossip generation for peer {}; local time = {}, received generation = {}", new Object[]{ep, Long.valueOf(localTime), Integer.valueOf(remoteGeneration)});
               } else if(remoteGeneration > localGeneration) {
                  if(logger.isTraceEnabled()) {
                     logger.trace("Updating heartbeat state generation to {} from {} for {}", new Object[]{Integer.valueOf(remoteGeneration), Integer.valueOf(localGeneration), ep});
                  }

                  this.handleMajorStateChange(ep, remoteState);
               } else if(remoteGeneration == localGeneration) {
                  int localMaxVersion = this.getMaxEndpointStateVersion(localEpStatePtr);
                  int remoteMaxVersion = this.getMaxEndpointStateVersion(remoteState);
                  if(remoteMaxVersion > localMaxVersion) {
                     this.applyNewStates(ep, localEpStatePtr, remoteState);
                  } else if(logger.isTraceEnabled()) {
                     logger.trace("Ignoring remote version {} <= {} for {}", new Object[]{Integer.valueOf(remoteMaxVersion), Integer.valueOf(localMaxVersion), ep});
                  }

                  if(!localEpStatePtr.isAlive() && !this.isDeadState(localEpStatePtr)) {
                     this.markAlive(ep, localEpStatePtr);
                  }
               } else if(logger.isTraceEnabled()) {
                  logger.trace("Ignoring remote generation {} < {}", Integer.valueOf(remoteGeneration), Integer.valueOf(localGeneration));
               }
            } else {
               FailureDetector.instance.report(ep);
               this.handleMajorStateChange(ep, remoteState);
            }
         }
      }
   }

   private void applyNewStates(InetAddress addr, EndpointState localState, EndpointState remoteState) {
      int oldVersion = localState.getHeartBeatState().getHeartBeatVersion();
      localState.setHeartBeatState(remoteState.getHeartBeatState());
      if(logger.isTraceEnabled()) {
         logger.trace("Updating heartbeat state version to {} from {} for {} ...", new Object[]{Integer.valueOf(localState.getHeartBeatState().getHeartBeatVersion()), Integer.valueOf(oldVersion), addr});
      }

      Set<Entry<ApplicationState, VersionedValue>> remoteStates = remoteState.states();

      assert remoteState.getHeartBeatState().getGeneration() == localState.getHeartBeatState().getGeneration();

      localState.addApplicationStates(remoteStates);
      Iterator var6 = remoteStates.iterator();

      while(var6.hasNext()) {
         Entry<ApplicationState, VersionedValue> remoteEntry = (Entry)var6.next();
         this.doOnChangeNotifications(addr, (ApplicationState)remoteEntry.getKey(), (VersionedValue)remoteEntry.getValue());
      }

   }

   private void doBeforeChangeNotifications(InetAddress addr, EndpointState epState, ApplicationState apState, VersionedValue newValue) {
      Iterator var5 = this.subscribers.iterator();

      while(var5.hasNext()) {
         IEndpointStateChangeSubscriber subscriber = (IEndpointStateChangeSubscriber)var5.next();
         subscriber.beforeChange(addr, epState, apState, newValue);
      }

   }

   private void doOnChangeNotifications(InetAddress addr, ApplicationState state, VersionedValue value) {
      Iterator var4 = this.subscribers.iterator();

      while(var4.hasNext()) {
         IEndpointStateChangeSubscriber subscriber = (IEndpointStateChangeSubscriber)var4.next();
         subscriber.onChange(addr, state, value);
      }

   }

   private void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration) {
      deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, 0));
      if(logger.isTraceEnabled()) {
         logger.trace("requestAll for {}", gDigest.getEndpoint());
      }

   }

   private void sendAll(GossipDigest gDigest, Map<InetAddress, EndpointState> deltaEpStateMap, int maxRemoteVersion) {
      EndpointState localEpStatePtr = this.getStateForVersionBiggerThan(gDigest.getEndpoint(), maxRemoteVersion);
      if(localEpStatePtr != null) {
         deltaEpStateMap.put(gDigest.getEndpoint(), localEpStatePtr);
      }

   }

   void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddress, EndpointState> deltaEpStateMap) {
      Iterator var4;
      if(gDigestList.size() == 0) {
         logger.debug("Shadow request received, adding all states");
         var4 = this.endpointStateMap.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<InetAddress, EndpointState> entry = (Entry)var4.next();
            gDigestList.add(new GossipDigest((InetAddress)entry.getKey(), 0, 0));
         }
      }

      var4 = gDigestList.iterator();

      while(true) {
         int remoteGeneration;
         int maxRemoteVersion;
         int localGeneration;
         int maxLocalVersion;
         GossipDigest gDigest;
         label43:
         do {
            while(var4.hasNext()) {
               gDigest = (GossipDigest)var4.next();
               remoteGeneration = gDigest.getGeneration();
               maxRemoteVersion = gDigest.getMaxVersion();
               EndpointState epStatePtr = (EndpointState)this.endpointStateMap.get(gDigest.getEndpoint());
               if(epStatePtr != null) {
                  localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                  maxLocalVersion = this.getMaxEndpointStateVersion(epStatePtr);
                  continue label43;
               }

               this.requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }

            return;
         } while(remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion);

         if(remoteGeneration > localGeneration) {
            this.requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
         } else if(remoteGeneration < localGeneration) {
            this.sendAll(gDigest, deltaEpStateMap, 0);
         } else if(remoteGeneration == localGeneration) {
            if(maxRemoteVersion > maxLocalVersion) {
               deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, maxLocalVersion));
            } else if(maxRemoteVersion < maxLocalVersion) {
               this.sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
            }
         }
      }
   }

   public void start(int generationNumber) {
      this.start(generationNumber, new EnumMap(ApplicationState.class));
   }

   public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates) {
      this.buildSeedsList(this.seeds);
      this.maybeInitializeLocalState(generationNbr);
      EndpointState localState = (EndpointState)this.endpointStateMap.get(FBUtilities.getBroadcastAddress());
      localState.addApplicationStates(preloadLocalStates);
      DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
      if(logger.isTraceEnabled()) {
         logger.trace("gossip started with generation {}", Integer.valueOf(localState.getHeartBeatState().getGeneration()));
      }

      this.scheduledGossipTask = executor.scheduleWithFixedDelay(new Gossiper.GossipTask(null), 1000L, 1000L, TimeUnit.MILLISECONDS);
   }

   @VisibleForTesting
   public void startForTest() {
      this.scheduledGossipTask = executor.scheduleWithFixedDelay(() -> {
      }, 1000L, 1000L, TimeUnit.MILLISECONDS);
   }

   public synchronized Map<InetAddress, EndpointState> doShadowRound() {
      return this.doShadowRound(Collections.emptySet());
   }

   public synchronized Map<InetAddress, EndpointState> doShadowRound(Set<InetAddress> peers) {
      this.buildSeedsList(this.seeds);
      if(this.seeds.isEmpty() && peers.isEmpty()) {
         return this.endpointShadowStateMap;
      } else {
         boolean isSeed = DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress());
         int shadowRoundDelay = isSeed?StorageService.RING_DELAY:StorageService.RING_DELAY * 2;
         this.seedsInShadowRound.clear();
         this.endpointShadowStateMap.clear();
         List<GossipDigest> gDigests = new ArrayList();
         GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(), DatabaseDescriptor.getPartitionerName(), gDigests);
         this.inShadowRound = true;
         boolean includePeers = false;
         int slept = 0;

         try {
            while(true) {
               if(slept % 5000 == 0) {
                  logger.trace("Sending shadow round GOSSIP DIGEST SYN to seeds {}", this.seeds);
                  MessagingService.instance().send(Verbs.GOSSIP.SYN.newDispatcher(this.seeds, digestSynMessage));
                  if(includePeers) {
                     logger.trace("Sending shadow round GOSSIP DIGEST SYN to known peers {}", peers);
                     MessagingService.instance().send(Verbs.GOSSIP.SYN.newDispatcher(peers, digestSynMessage));
                  }

                  includePeers = true;
               }

               Thread.sleep(1000L);
               if(!this.inShadowRound) {
                  break;
               }

               slept += 1000;
               if(slept > shadowRoundDelay) {
                  if(!isSeed) {
                     throw new RuntimeException("Unable to gossip with any peers");
                  }

                  this.inShadowRound = false;
                  break;
               }
            }
         } catch (InterruptedException var9) {
            throw new RuntimeException(var9);
         }

         return ImmutableMap.copyOf(this.endpointShadowStateMap);
      }
   }

   @VisibleForTesting
   void buildSeedsList(Set<InetAddress> toBuild) {
      Iterator var2 = DatabaseDescriptor.getSeeds().iterator();

      while(var2.hasNext()) {
         InetAddress seed = (InetAddress)var2.next();
         if(!seed.equals(FBUtilities.getBroadcastAddress())) {
            toBuild.add(seed);
         }
      }

   }

   public List<String> reloadSeeds() {
      logger.debug("Triggering reload of seed node list...");
      HashSet tmp = new HashSet();

      try {
         this.buildSeedsList(tmp);
      } catch (Throwable var3) {
         JVMStabilityInspector.inspectThrowable(var3);
         logger.warn("Error while getting seed node list: {}", var3.getLocalizedMessage());
         return null;
      }

      if(tmp.size() == 0) {
         logger.debug("New seed node list is empty. Not updating seed list.");
         return this.getSeeds();
      } else if(tmp.equals(this.seeds)) {
         logger.debug("New seed node list matches the existing list.");
         return this.getSeeds();
      } else {
         this.seeds.addAll(tmp);
         this.seeds.retainAll(tmp);
         logger.debug("New seed node list after reload {}", this.seeds);
         return this.getSeeds();
      }
   }

   public List<String> getSeeds() {
      List<String> seedList = new ArrayList();
      Iterator var2 = this.seeds.iterator();

      while(var2.hasNext()) {
         InetAddress seed = (InetAddress)var2.next();
         seedList.add(seed.toString());
      }

      return seedList;
   }

   public void maybeInitializeLocalState(int generationNbr) {
      HeartBeatState hbState = new HeartBeatState(generationNbr);
      EndpointState localState = new EndpointState(hbState);
      localState.markAlive();
      this.endpointStateMap.putIfAbsent(FBUtilities.getBroadcastAddress(), localState);
   }

   public void forceNewerGeneration() {
      EndpointState epstate = (EndpointState)this.endpointStateMap.get(FBUtilities.getBroadcastAddress());
      epstate.getHeartBeatState().forceNewerGenerationUnsafe();
   }

   public void addSavedEndpoint(InetAddress ep) {
      if(ep.equals(FBUtilities.getBroadcastAddress())) {
         logger.debug("Attempt to add self as saved endpoint");
      } else {
         EndpointState epState = (EndpointState)this.endpointStateMap.get(ep);
         if(epState != null) {
            logger.debug("not replacing a previous epState for {}, but reusing it: {}", ep, epState);
            epState.setHeartBeatState(new HeartBeatState(0));
         } else {
            epState = new EndpointState(new HeartBeatState(0));
         }

         epState.markDead();
         this.endpointStateMap.put(ep, epState);
         this.unreachableEndpoints.put(ep, Long.valueOf(System.nanoTime()));
         if(logger.isTraceEnabled()) {
            logger.trace("Adding saved endpoint {} {}", ep, Integer.valueOf(epState.getHeartBeatState().getGeneration()));
         }

      }
   }

   private void addLocalApplicationStateInternal(ApplicationState state, VersionedValue value) {
      assert taskLock.isHeldByCurrentThread();

      EndpointState epState = (EndpointState)this.endpointStateMap.get(FBUtilities.getBroadcastAddress());
      InetAddress epAddr = FBUtilities.getBroadcastAddress();

      assert epState != null;

      this.doBeforeChangeNotifications(epAddr, epState, state, value);
      value = StorageService.instance.valueFactory.cloneWithHigherVersion(value);
      epState.addApplicationState(state, value);
      this.doOnChangeNotifications(epAddr, state, value);
   }

   public void addLocalApplicationState(ApplicationState applicationState, VersionedValue value) {
      this.addLocalApplicationStates(Arrays.asList(new Pair[]{Pair.create(applicationState, value)}));
   }

   public void addLocalApplicationStates(List<Pair<ApplicationState, VersionedValue>> states) {
      taskLock.lock();

      try {
         Iterator var2 = states.iterator();

         while(var2.hasNext()) {
            Pair<ApplicationState, VersionedValue> pair = (Pair)var2.next();
            this.addLocalApplicationStateInternal((ApplicationState)pair.left, (VersionedValue)pair.right);
         }
      } finally {
         taskLock.unlock();
      }

   }

   public void stop() {
      EndpointState mystate = (EndpointState)this.endpointStateMap.get(FBUtilities.getBroadcastAddress());
      if(mystate != null && !this.isSilentShutdownState(mystate) && StorageService.instance.isJoined()) {
         logger.info("Announcing shutdown");
         this.addLocalApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
         MessagingService.instance().send(Verbs.GOSSIP.SHUTDOWN.newDispatcher(this.liveEndpoints, EmptyPayload.instance));
         Uninterruptibles.sleepUninterruptibly((long)Integer.getInteger("cassandra.shutdown_announce_in_ms", 2000).intValue(), TimeUnit.MILLISECONDS);
      } else {
         logger.warn("No local state, state is in silent shutdown, or node hasn't joined, not announcing shutdown");
      }

      this.stopWithoutAnnouncing();
   }

   public void stopWithoutAnnouncing() {
      if(this.scheduledGossipTask != null) {
         this.scheduledGossipTask.cancel(false);
      }

   }

   public boolean isEnabled() {
      return this.scheduledGossipTask != null && !this.scheduledGossipTask.isCancelled();
   }

   protected void maybeFinishShadowRound(InetAddress respondent, boolean isInShadowRound, Map<InetAddress, EndpointState> epStateMap) {
      if(this.inShadowRound) {
         if(!isInShadowRound) {
            if(!this.seeds.contains(respondent)) {
               logger.warn("Received an ack from {}, who isn't a seed. Ensure your seed list includes a live node. Exiting shadow round", respondent);
            }

            logger.debug("Received a regular ack from {}, can now exit shadow round", respondent);
            this.endpointShadowStateMap.putAll(epStateMap);
            this.inShadowRound = false;
            this.seedsInShadowRound.clear();
         } else {
            logger.debug("Received an ack from {} indicating it is also in shadow round", respondent);
            this.seedsInShadowRound.add(respondent);
            if(this.seedsInShadowRound.containsAll(this.seeds)) {
               logger.debug("All seeds are in a shadow round, clearing this node to exit its own");
               this.inShadowRound = false;
               this.seedsInShadowRound.clear();
            }
         }
      }

   }

   protected boolean isInShadowRound() {
      return this.inShadowRound;
   }

   @VisibleForTesting
   public void initializeNodeUnsafe(InetAddress addr, UUID uuid, int generationNbr) {
      HeartBeatState hbState = new HeartBeatState(generationNbr);
      EndpointState newState = new EndpointState(hbState);
      newState.markAlive();
      EndpointState oldState = (EndpointState)this.endpointStateMap.putIfAbsent(addr, newState);
      EndpointState localState = oldState == null?newState:oldState;
      Map<ApplicationState, VersionedValue> states = new EnumMap(ApplicationState.class);
      states.put(ApplicationState.NET_VERSION, StorageService.instance.valueFactory.networkVersion());
      states.put(ApplicationState.HOST_ID, StorageService.instance.valueFactory.hostId(uuid));
      localState.addApplicationStates((Map)states);
   }

   @VisibleForTesting
   public void injectApplicationState(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      EndpointState localState = (EndpointState)this.endpointStateMap.get(endpoint);
      localState.addApplicationState(state, value);
   }

   public long getEndpointDowntime(String address) throws UnknownHostException {
      return this.getEndpointDowntime(InetAddress.getByName(address));
   }

   public int getCurrentGenerationNumber(String address) throws UnknownHostException {
      return this.getCurrentGenerationNumber(InetAddress.getByName(address));
   }

   public void addExpireTimeForEndpoint(InetAddress endpoint, long expireTime) {
      if(logger.isDebugEnabled()) {
         logger.debug("adding expire time for endpoint : {} ({})", endpoint, Long.valueOf(expireTime));
      }

      this.expireTimeEndpointMap.put(endpoint, Long.valueOf(expireTime));
   }

   public static long computeExpireTime() {
      return System.currentTimeMillis() + 259200000L;
   }

   @Nullable
   public ClusterVersionBarrier.EndpointInfo getEndpointInfo(InetAddress ep) {
      EndpointState state = this.getEndpointStateForEndpoint(ep);
      CassandraVersion releaseVersion = state != null?state.getReleaseVersion():null;
      if(releaseVersion == null) {
         releaseVersion = SystemKeyspace.getReleaseVersion(ep);
      }

      UUID schemaVersion = state != null?state.getSchemaVersion():null;
      if(schemaVersion == null) {
         schemaVersion = SystemKeyspace.getSchemaVersion(ep);
      }

      return state == null && releaseVersion == null?null:new ClusterVersionBarrier.EndpointInfo(releaseVersion, schemaVersion);
   }

   @Nullable
   public CassandraVersion getReleaseVersion(InetAddress ep) {
      EndpointState state = this.getEndpointStateForEndpoint(ep);
      return state != null?state.getReleaseVersion():null;
   }

   @Nullable
   public UUID getSchemaVersion(InetAddress ep) {
      EndpointState state = this.getEndpointStateForEndpoint(ep);
      return state != null?state.getSchemaVersion():null;
   }

   public double getSeedGossipProbability() {
      return DatabaseDescriptor.getSeedGossipProbability();
   }

   public void setSeedGossipProbability(double probability) {
      DatabaseDescriptor.setSeedGossipProbability(probability);
   }

   public static void waitToSettle(String waitingFor) {
      if(!FBUtilities.getBroadcastAddress().equals(InetAddress.getLoopbackAddress())) {
         int forceAfter = Integer.getInteger("cassandra.skip_wait_for_gossip_to_settle", -1).intValue();
         if(forceAfter != 0) {
            int GOSSIP_SETTLE_MIN_WAIT_MS = true;
            long GOSSIP_SETTLE_POLL_INTERVAL_NS = FailureDetector.getMaxLocalPause();
            int GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = Integer.getInteger("cassandra.rounds_to_wait_for_gossip_to_settle", 3).intValue();
            int MAX_BACKLOG_CHECKS = true;
            int MAX_UNSTABLE_FLIPS = Integer.getInteger("cassandra.max_gossip_settle_state_flips", GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED - 1).intValue();
            Map<InetAddress, Integer> unstableTracking = new HashMap();
            Set<InetAddress> unstableEndpoints = new HashSet();
            Set<InetAddress> stableEndpoints = new HashSet();
            logger.info("Waiting for gossip to settle before {}...", waitingFor);
            Uninterruptibles.sleepUninterruptibly(5000L, TimeUnit.MILLISECONDS);
            int totalPolls = 0;
            int numOkay = 0;
            int backlogChecks = 0;
            long epSize = (long)instance.getLiveMembers().size();
            JMXEnabledThreadPoolExecutor gossipStage = (JMXEnabledThreadPoolExecutor)StageManager.getStage(Stage.GOSSIP);
            long startingCompletedTasks = 0L;

            while(numOkay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED) {
               if(numOkay == 0) {
                  startingCompletedTasks = ((Long)((JMXEnabledThreadPoolExecutor)StageManager.getStage(Stage.GOSSIP)).metrics.completedTasks.getValue()).longValue();
               }

               Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_NS, TimeUnit.NANOSECONDS);
               Set<InetAddress> noLongerStable = Sets.intersection(stableEndpoints, instance.getUnreachableMembers());
               Iterator var20 = noLongerStable.iterator();

               while(var20.hasNext()) {
                  InetAddress downEp = (InetAddress)var20.next();
                  int count = ((Integer)unstableTracking.getOrDefault(downEp, Integer.valueOf(0))).intValue();
                  ++count;
                  unstableTracking.put(downEp, Integer.valueOf(count));
                  if(count >= MAX_UNSTABLE_FLIPS) {
                     unstableEndpoints.add(downEp);
                  }
               }

               stableEndpoints.addAll(instance.getLiveMembers());
               stableEndpoints.removeAll(unstableEndpoints);
               int currentSize = stableEndpoints.size();
               ++totalPolls;
               if((long)currentSize == epSize) {
                  logger.debug("Gossip looks settled. Live endpoints: {}", Integer.valueOf(currentSize));
                  ++numOkay;
               } else {
                  logger.info("Gossip not settled after {} polls.", Integer.valueOf(totalPolls));
                  numOkay = 0;
               }

               epSize = (long)currentSize;
               if(forceAfter > 0 && totalPolls > forceAfter) {
                  logger.warn("Gossip not settled but startup forced by cassandra.skip_wait_for_gossip_to_settle. Gossip total polls: {}", Integer.valueOf(totalPolls));
                  break;
               }

               if(numOkay == GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED) {
                  long pendingTasks = 0L;
                  long activeTasks = 0L;
                  boolean backlogClear = false;

                  for(int i = 0; i < 50; ++i) {
                     Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
                     pendingTasks = ((Long)gossipStage.metrics.pendingTasks.getValue()).longValue();
                     activeTasks = (long)((Integer)gossipStage.metrics.activeTasks.getValue()).intValue();
                     if(pendingTasks == 0L && activeTasks == 0L) {
                        logger.info("No gossip backlog");
                        backlogClear = true;
                        break;
                     }

                     logger.debug("Gossip backlog not clear. Pending {}, active {}", Long.valueOf(pendingTasks), Long.valueOf(activeTasks));
                  }

                  long completedTasks = ((Long)gossipStage.metrics.completedTasks.getValue()).longValue();
                  if(backlogClear || backlogChecks >= 10 && completedTasks > startingCompletedTasks + 2L) {
                     noLongerStable = Sets.intersection(stableEndpoints, instance.getUnreachableMembers());
                     unstableEndpoints.addAll(noLongerStable);
                     stableEndpoints.addAll(instance.getLiveMembers());
                     stableEndpoints.removeAll(unstableEndpoints);
                     currentSize = stableEndpoints.size();
                     if(backlogChecks == 10) {
                        logger.warn("Skipping backlog check after {} checks, completed tasks {}", Integer.valueOf(10), Long.valueOf(completedTasks));
                     }

                     if((long)currentSize != epSize) {
                        logger.info("After waiting for gossip backlog to clear, endpoint size no longer stable. Previously {}, now {}", Long.valueOf(epSize), Integer.valueOf(currentSize));
                        epSize = (long)currentSize;
                        numOkay = 0;
                     }
                  } else {
                     logger.info("Gossip backlog did not stabilize. Pending {}, active {}, completed {}", new Object[]{Long.valueOf(pendingTasks), Long.valueOf(activeTasks), Long.valueOf(completedTasks)});
                  }

                  ++backlogChecks;
               }
            }

            for(int i = 0; i <= forceAfter || i == 0; ++i) {
               logger.debug("Waiting for echo replies");
               Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_NS, TimeUnit.NANOSECONDS);
               if(instance.pendingEcho.get() <= 0) {
                  logger.info("No pending echos; proceeding.  Echos failed {}, Echos succeeded {}", Integer.valueOf(instance.failedEchos.get()), Integer.valueOf(instance.successEchos.get()));
                  break;
               }

               if(i == forceAfter || forceAfter < 0) {
                  logger.warn("Pending echos did not reach 0 after {} tries, forcing. {} outstanding echos", Integer.valueOf(forceAfter), Integer.valueOf(instance.pendingEcho.get()));
               }
            }

            if(totalPolls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED) {
               logger.info("Gossip settled after {} extra polls; proceeding", Integer.valueOf(totalPolls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED));
            } else {
               logger.info("Gossip settled; proceeding");
            }

         }
      }
   }

   static {
      SILENT_SHUTDOWN_STATES.addAll(DEAD_STATES);
      SILENT_SHUTDOWN_STATES.add("BOOT");
      SILENT_SHUTDOWN_STATES.add("BOOT_REPLACE");
      taskLock = new ReentrantLock();
      QUARANTINE_DELAY = StorageService.RING_DELAY * 2;
      logger = LoggerFactory.getLogger(Gossiper.class);
      instance = new Gossiper(true);
   }

   private class GossipTask implements Runnable {
      private GossipTask() {
      }

      public void run() {
         try {
            MessagingService.instance().waitUntilListening();
            Gossiper.taskLock.lock();
            ((EndpointState)Gossiper.this.endpointStateMap.get(FBUtilities.getBroadcastAddress())).getHeartBeatState().updateHeartBeat();
            if(Gossiper.logger.isTraceEnabled()) {
               Gossiper.logger.trace("My heartbeat is now {}", Integer.valueOf(((EndpointState)Gossiper.this.endpointStateMap.get(FBUtilities.getBroadcastAddress())).getHeartBeatState().getHeartBeatVersion()));
            }

            List<GossipDigest> gDigests = new ArrayList();
            Gossiper.instance.makeRandomGossipDigest(gDigests);
            if(gDigests.size() > 0) {
               GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(), DatabaseDescriptor.getPartitionerName(), gDigests);
               boolean gossipedToSeed = Gossiper.this.doGossipToLiveMember(digestSynMessage);
               Gossiper.this.maybeGossipToUnreachableMember(digestSynMessage);
               if(!gossipedToSeed || Gossiper.this.liveEndpoints.size() < Gossiper.this.seeds.size()) {
                  Gossiper.this.gossipToSeed(digestSynMessage);
               }

               Gossiper.this.doStatusCheck();
            }
         } catch (Exception var7) {
            JVMStabilityInspector.inspectThrowable(var7);
            Gossiper.logger.error("Gossip error", var7);
         } finally {
            Gossiper.taskLock.unlock();
         }

      }
   }
}
