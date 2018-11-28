package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.StreamingRepairTask;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.SnapshotMessage;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveRepairService implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener, ActiveRepairServiceMBean {
   public final ActiveRepairService.ConsistentSessions consistent = new ActiveRepairService.ConsistentSessions();
   private boolean registeredForEndpointChanges = false;
   public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");
   private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
   public static final ActiveRepairService instance;
   public static final long UNREPAIRED_SSTABLE = 0L;
   public static final UUID NO_PENDING_REPAIR;
   private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap();
   private final ConcurrentMap<UUID, ActiveRepairService.ParentRepairSession> parentRepairSessions = new ConcurrentHashMap();
   public static final ExecutorService repairCommandExecutor;
   private final IFailureDetector failureDetector;
   private final Gossiper gossiper;
   private final Cache<Integer, Pair<ActiveRepairService.ParentRepairStatus, List<String>>> repairStatusByCmd;

   public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper) {
      this.failureDetector = failureDetector;
      this.gossiper = gossiper;
      this.repairStatusByCmd = CacheBuilder.newBuilder().expireAfterWrite(Long.getLong("cassandra.parent_repair_status_expiry_seconds", TimeUnit.SECONDS.convert(1L, TimeUnit.DAYS)).longValue(), TimeUnit.SECONDS).maximumSize(Long.getLong("cassandra.parent_repair_status_cache_size", 100000L).longValue()).build();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=RepairService"));
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }
   }

   public void start() {
      this.consistent.local.start();
      logger.debug("Scheduling consistent repair cleanup with interval: {}", Integer.valueOf(LocalSessions.CLEANUP_INTERVAL));
      DebuggableScheduledThreadPoolExecutor var10000 = ScheduledExecutors.optionalTasks;
      LocalSessions var10001 = this.consistent.local;
      this.consistent.local.getClass();
      var10000.scheduleAtFixedRate(var10001::cleanup, (long)LocalSessions.CLEANUP_INTERVAL, (long)LocalSessions.CLEANUP_INTERVAL, TimeUnit.SECONDS);
   }

   public List<Map<String, String>> getSessions(boolean all) {
      return this.consistent.local.sessionInfo(all);
   }

   public void failSession(String session, boolean force) {
      UUID sessionID = UUID.fromString(session);
      this.consistent.local.cancelSession(sessionID, force);
   }

   public RepairSession submitRepairSession(UUID parentRepairSession, Collection<Range<Token>> range, String keyspace, RepairParallelism parallelismDegree, Set<InetAddress> endpoints, boolean isIncremental, boolean pullRepair, PreviewKind previewKind, ListeningExecutorService executor, String... cfnames) {
      if(endpoints.isEmpty()) {
         return null;
      } else if(cfnames.length == 0) {
         return null;
      } else {
         final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, isIncremental, pullRepair, previewKind, cfnames);
         this.sessions.put(session.getId(), session);
         this.registerOnFdAndGossip(session);
         session.addListener(new Runnable() {
            public void run() {
               ActiveRepairService.this.sessions.remove(session.getId());
            }
         }, MoreExecutors.directExecutor());
         session.start(executor);
         return session;
      }
   }

   private <T extends AbstractFuture & IEndpointStateChangeSubscriber & IFailureDetectionEventListener> void registerOnFdAndGossip(final T task) {
      this.gossiper.register((IEndpointStateChangeSubscriber)task);
      this.failureDetector.registerFailureDetectionEventListener((IFailureDetectionEventListener)task);
      task.addListener(new Runnable() {
         public void run() {
            ActiveRepairService.this.failureDetector.unregisterFailureDetectionEventListener((IFailureDetectionEventListener)task);
            ActiveRepairService.this.gossiper.unregister((IEndpointStateChangeSubscriber)task);
         }
      }, MoreExecutors.sameThreadExecutor());
   }

   public synchronized void terminateSessions() {
      Throwable cause = new IOException("Terminate session is called");
      Iterator var2 = this.sessions.values().iterator();

      while(var2.hasNext()) {
         RepairSession session = (RepairSession)var2.next();
         session.forceShutdown(cause);
      }

      this.parentRepairSessions.clear();
   }

   public void recordRepairStatus(int cmd, ActiveRepairService.ParentRepairStatus parentRepairStatus, List<String> messages) {
      this.repairStatusByCmd.put(Integer.valueOf(cmd), Pair.create(parentRepairStatus, messages));
   }

   Pair<ActiveRepairService.ParentRepairStatus, List<String>> getRepairStatus(Integer cmd) {
      return (Pair)this.repairStatusByCmd.getIfPresent(cmd);
   }

   public static Set<InetAddress> getNeighbors(String keyspaceName, Collection<Range<Token>> keyspaceLocalRanges, Range<Token> toRepair, Collection<String> dataCenters, Collection<String> hosts) {
      StorageService ss = StorageService.instance;
      Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
      Range<Token> rangeSuperSet = null;
      Iterator var8 = keyspaceLocalRanges.iterator();

      while(var8.hasNext()) {
         Range<Token> range = (Range)var8.next();
         if(range.contains((AbstractBounds)toRepair)) {
            rangeSuperSet = range;
            break;
         }

         if(range.intersects(toRepair)) {
            throw new IllegalArgumentException(String.format("Requested range %s intersects a local range (%s) but is not fully contained in one; this would lead to imprecise repair. keyspace: %s", new Object[]{toRepair.toString(), range.toString(), keyspaceName}));
         }
      }

      if(rangeSuperSet != null && replicaSets.containsKey(rangeSuperSet)) {
         Set<InetAddress> neighbors = new HashSet((Collection)replicaSets.get(rangeSuperSet));
         neighbors.remove(FBUtilities.getBroadcastAddress());
         if(dataCenters != null && !dataCenters.isEmpty()) {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String, InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            Iterator var22 = dataCenters.iterator();

            while(var22.hasNext()) {
               String dc = (String)var22.next();
               Collection<InetAddress> c = dcEndpointsMap.get(dc);
               if(c != null) {
                  dcEndpoints.addAll(c);
               }
            }

            return Sets.intersection(neighbors, dcEndpoints);
         } else if(hosts != null && !hosts.isEmpty()) {
            Set<InetAddress> specifiedHost = new HashSet();
            Iterator var10 = hosts.iterator();

            while(var10.hasNext()) {
               String host = (String)var10.next();

               try {
                  InetAddress endpoint = InetAddress.getByName(host.trim());
                  if(endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint)) {
                     specifiedHost.add(endpoint);
                  }
               } catch (UnknownHostException var15) {
                  throw new IllegalArgumentException("Unknown host specified " + host, var15);
               }
            }

            if(!specifiedHost.contains(FBUtilities.getBroadcastAddress())) {
               throw new IllegalArgumentException("The current host must be part of the repair");
            } else if(specifiedHost.size() <= 1) {
               String msg = "Specified hosts %s do not share range %s needed for repair. Either restrict repair ranges with -st/-et options, or specify one of the neighbors that share this range with this node: %s.";
               throw new IllegalArgumentException(String.format(msg, new Object[]{hosts, toRepair, neighbors}));
            } else {
               specifiedHost.remove(FBUtilities.getBroadcastAddress());
               return specifiedHost;
            }
         } else {
            return neighbors;
         }
      } else {
         return Collections.emptySet();
      }
   }

   @VisibleForTesting
   static long getRepairedAt(RepairOption options, boolean skippedReplicas) {
      return options.isIncremental() && options.getDataCenters().isEmpty() && options.getHosts().isEmpty() && !skippedReplicas?Clock.instance.currentTimeMillis():0L;
   }

   public UUID prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores, boolean skippedReplicas) {
      long repairedAt = getRepairedAt(options, skippedReplicas);
      this.registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), repairedAt, options.getPreviewKind());
      final CountDownLatch prepareLatch = new CountDownLatch(endpoints.size());
      final AtomicBoolean status = new AtomicBoolean(true);
      final Set<String> failedNodes = Collections.synchronizedSet(new HashSet());
      MessageCallback<EmptyPayload> callback = new MessageCallback<EmptyPayload>() {
         public void onResponse(Response<EmptyPayload> msg) {
            prepareLatch.countDown();
         }

         public void onFailure(FailureResponse<EmptyPayload> failureResponse) {
            status.set(false);
            failedNodes.add(failureResponse.from().getHostAddress());
            prepareLatch.countDown();
         }
      };
      List<TableId> tableIds = new ArrayList(columnFamilyStores.size());
      Iterator var14 = columnFamilyStores.iterator();

      while(var14.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var14.next();
         tableIds.add(cfs.metadata.id);
      }

      var14 = endpoints.iterator();

      while(var14.hasNext()) {
         InetAddress neighbour = (InetAddress)var14.next();
         if(FailureDetector.instance.isAlive(neighbour)) {
            PrepareMessage message = new PrepareMessage(parentRepairSession, tableIds, options.getRanges(), options.isIncremental(), repairedAt, options.getPreviewKind());
            MessagingService.instance().send(Verbs.REPAIR.PREPARE.newRequest(neighbour, message), callback);
         } else if(options.isForcedRepair()) {
            prepareLatch.countDown();
         } else {
            this.failRepair(parentRepairSession, "Endpoint not alive: " + neighbour);
         }
      }

      try {
         if(!prepareLatch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS)) {
            this.failRepair(parentRepairSession, "Did not get replies from all endpoints.");
         }
      } catch (InterruptedException var17) {
         this.failRepair(parentRepairSession, "Interrupted while waiting for prepare repair response.");
      }

      if(!status.get()) {
         this.failRepair(parentRepairSession, "Got negative replies from endpoints " + failedNodes);
      }

      return parentRepairSession;
   }

   private void failRepair(UUID parentRepairSession, String errorMsg) {
      this.removeParentRepairSession(parentRepairSession);
      throw new RuntimeException(errorMsg);
   }

   public synchronized void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, PreviewKind previewKind) {
      assert isIncremental || repairedAt == 0L;

      if(!this.registeredForEndpointChanges) {
         Gossiper.instance.register(this);
         FailureDetector.instance.registerFailureDetectionEventListener(this);
         this.registeredForEndpointChanges = true;
      }

      if(!this.parentRepairSessions.containsKey(parentRepairSession)) {
         this.parentRepairSessions.put(parentRepairSession, new ActiveRepairService.ParentRepairSession(coordinator, columnFamilyStores, ranges, isIncremental, repairedAt, previewKind));
      } else {
         logger.info("Parent repair session with id = {} has already been registered.", parentRepairSession);
      }

   }

   public ActiveRepairService.ParentRepairSession getParentRepairSession(UUID parentSessionId) {
      ActiveRepairService.ParentRepairSession session = (ActiveRepairService.ParentRepairSession)this.parentRepairSessions.get(parentSessionId);
      if(session == null) {
         throw new RuntimeException("Parent repair session with id = " + parentSessionId + " has failed.");
      } else {
         return session;
      }
   }

   public boolean hasParentRepairSession(UUID parentSessionId) {
      return this.parentRepairSessions.containsKey(parentSessionId);
   }

   public synchronized void removeParentRepairSession(UUID parentSessionId) {
      if(this.parentRepairSessions.containsKey(parentSessionId)) {
         String snapshotName = parentSessionId.toString();
         Iterator var3 = this.getParentRepairSession(parentSessionId).columnFamilyStores.values().iterator();

         while(var3.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
            if(cfs.snapshotExists(snapshotName)) {
               cfs.clearSnapshot(snapshotName);
            }
         }

         this.parentRepairSessions.remove(parentSessionId);
      }

   }

   private boolean isIncremental(UUID sessionID) {
      return this.consistent.local.isSessionInProgress(sessionID);
   }

   public void handleValidationRequest(InetAddress from, ValidationRequest request) {
      RepairJobDesc desc = request.desc;
      logger.debug("Validating {}", request);
      ColumnFamilyStore store = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
      if(store == null) {
         logger.error("Table {}.{} was dropped during snapshot phase of repair", desc.keyspace, desc.columnFamily);
         MessagingService.instance().send(Verbs.REPAIR.VALIDATION_COMPLETE.newRequest(from, (new ValidationComplete(desc))));
      } else {
         instance.consistent.local.maybeSetRepairing(desc.parentSessionId);
         Validator validator = new Validator(desc, from, request.nowInSec, this.isIncremental(desc.parentSessionId), this.previewKind(desc.parentSessionId));
         CompactionManager.instance.submitValidation(store, validator);
      }
   }

   private PreviewKind previewKind(UUID sessionID) {
      ActiveRepairService.ParentRepairSession prs = instance.getParentRepairSession(sessionID);
      return prs != null?prs.previewKind:PreviewKind.NONE;
   }

   public void handleValidationComplete(InetAddress from, ValidationComplete validation) {
      RepairJobDesc desc = validation.desc;
      RepairSession session = (RepairSession)this.sessions.get(desc.sessionId);
      if(session != null) {
         session.validationComplete(desc, from, validation.trees);
      }
   }

   public void handleSyncRequest(InetAddress from, SyncRequest sync) {
      RepairJobDesc desc = sync.desc;
      logger.debug("Syncing {}", sync);
      StreamingRepairTask task = new StreamingRepairTask(desc, sync, this.isIncremental(desc.parentSessionId)?desc.parentSessionId:null, sync.previewKind);
      task.run();
   }

   public void handleSyncComplete(InetAddress from, SyncComplete sync) {
      RepairJobDesc desc = sync.desc;
      RepairSession session = (RepairSession)this.sessions.get(desc.sessionId);
      if(session != null) {
         session.syncComplete(desc, sync.nodes, sync.success, sync.summaries);
      }
   }

   public void handlePrepare(InetAddress from, PrepareMessage prepareMessage) {
      logger.debug("Preparing, {}", prepareMessage);
      List<ColumnFamilyStore> columnFamilyStores = new ArrayList(prepareMessage.tableIds.size());
      Iterator var4 = prepareMessage.tableIds.iterator();

      while(var4.hasNext()) {
         TableId id = (TableId)var4.next();
         ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.getIfExists(id);
         if(columnFamilyStore == null) {
            throw new ActiveRepairService.DroppedTableException(id);
         }

         columnFamilyStores.add(columnFamilyStore);
      }

      this.registerParentRepairSession(prepareMessage.parentRepairSession, from, columnFamilyStores, prepareMessage.ranges, prepareMessage.isIncremental, prepareMessage.timestamp, prepareMessage.previewKind);
   }

   public void handleSnapshot(InetAddress from, SnapshotMessage snapshotMessage) {
      RepairJobDesc desc = snapshotMessage.desc;
      logger.debug("Snapshotting {}", desc);
      ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
      if(cfs == null) {
         throw new ActiveRepairService.DroppedTableException(desc.keyspace, desc.columnFamily);
      } else {
         ActiveRepairService.ParentRepairSession prs = this.getParentRepairSession(desc.parentSessionId);
         prs.maybeSnapshot(cfs.metadata.id, desc.parentSessionId);
      }
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
   }

   public void onRemove(InetAddress endpoint) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public void onRestart(InetAddress endpoint, EndpointState state) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public void convict(InetAddress ep, double phi) {
      if(phi >= 2.0D * DatabaseDescriptor.getPhiConvictThreshold() && !this.parentRepairSessions.isEmpty()) {
         Set<UUID> toRemove = new HashSet();
         Iterator var5 = this.parentRepairSessions.entrySet().iterator();

         while(var5.hasNext()) {
            Entry<UUID, ActiveRepairService.ParentRepairSession> repairSessionEntry = (Entry)var5.next();
            if(((ActiveRepairService.ParentRepairSession)repairSessionEntry.getValue()).coordinator.equals(ep)) {
               toRemove.add(repairSessionEntry.getKey());
            }
         }

         if(!toRemove.isEmpty()) {
            logger.debug("Removing {} in parent repair sessions", toRemove);
            var5 = toRemove.iterator();

            while(var5.hasNext()) {
               UUID id = (UUID)var5.next();
               this.removeParentRepairSession(id);
            }
         }

      }
   }

   static {
      instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);
      NO_PENDING_REPAIR = null;
      Config.RepairCommandPoolFullStrategy strategy = DatabaseDescriptor.getRepairCommandPoolFullStrategy();
      Object queue;
      if(strategy == Config.RepairCommandPoolFullStrategy.reject) {
         queue = new SynchronousQueue();
      } else {
         queue = new LinkedBlockingQueue();
      }

      repairCommandExecutor = new JMXEnabledThreadPoolExecutor(1, DatabaseDescriptor.getRepairCommandPoolSize(), 1L, TimeUnit.HOURS, (BlockingQueue)queue, new NamedThreadFactory("Repair-Task"), "internal", new AbortPolicy());
   }

   public static class ParentRepairSession {
      private final Map<TableId, ColumnFamilyStore> columnFamilyStores = new HashMap();
      private final Collection<Range<Token>> ranges;
      public final boolean isIncremental;
      public final long repairedAt;
      public final InetAddress coordinator;
      public final PreviewKind previewKind;

      public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, PreviewKind previewKind) {
         this.coordinator = coordinator;
         Iterator var8 = columnFamilyStores.iterator();

         while(var8.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var8.next();
            this.columnFamilyStores.put(cfs.metadata.id, cfs);
         }

         this.ranges = ranges;
         this.repairedAt = repairedAt;
         this.isIncremental = isIncremental;
         this.previewKind = previewKind;
      }

      public boolean isPreview() {
         return this.previewKind != PreviewKind.NONE;
      }

      public Predicate<SSTableReader> getPreviewPredicate() {
         switch (this.previewKind) {
            case ALL: {
               return s -> true;
            }
            case REPAIRED: {
               return s -> s.isRepaired();
            }
            case UNREPAIRED: {
               return s -> !s.isRepaired();
            }
         }
         throw new RuntimeException("Can't get preview predicate for preview kind " + (Object)((Object)this.previewKind));
      }


      public synchronized void maybeSnapshot(TableId tableId, UUID parentSessionId) {
         String snapshotName = parentSessionId.toString();
         if(!((ColumnFamilyStore)this.columnFamilyStores.get(tableId)).snapshotExists(snapshotName)) {
            ((ColumnFamilyStore)this.columnFamilyStores.get(tableId)).snapshot(snapshotName, new Predicate<SSTableReader>() {
               public boolean apply(SSTableReader sstable) {
                  return sstable != null && (!ParentRepairSession.this.isIncremental || !sstable.isRepaired()) && !sstable.metadata().isIndex() && (new Bounds(sstable.first.getToken(), sstable.last.getToken())).intersects(ParentRepairSession.this.ranges);
               }
            }, true, false, new HashSet());
         }

      }

      public Collection<ColumnFamilyStore> getColumnFamilyStores() {
         return ImmutableSet.<ColumnFamilyStore>builder().addAll(this.columnFamilyStores.values()).build();
      }

      public Set<TableId> getTableIds() {
         return ImmutableSet.copyOf(Iterables.transform(this.getColumnFamilyStores(), (cfs) -> {
            return cfs.metadata.id;
         }));
      }

      public Collection<Range<Token>> getRanges() {
         return ImmutableSet.copyOf(this.ranges);
      }

      public String toString() {
         return "ParentRepairSession{columnFamilyStores=" + this.columnFamilyStores + ", ranges=" + this.ranges + ", repairedAt=" + this.repairedAt + '}';
      }
   }

   public static class ConsistentSessions {
      public final LocalSessions local = new LocalSessions();
      public final CoordinatorSessions coordinated;

      public ConsistentSessions() {
         this.coordinated = new CoordinatorSessions(FailureDetector.instance, Gossiper.instance);
      }
   }

   public static enum ParentRepairStatus {
      IN_PROGRESS,
      COMPLETED,
      FAILED;

      private ParentRepairStatus() {
      }
   }

   private static class DroppedTableException extends InternalRequestExecutionException {
      DroppedTableException(TableId tableId) {
         super(RequestFailureReason.UNKNOWN, String.format("Table with id %s was dropped during repair", new Object[]{tableId}));
      }

      DroppedTableException(String keyspace, String table) {
         super(RequestFailureReason.UNKNOWN, String.format("Table %s.%s was dropped during repair", new Object[]{keyspace, table}));
      }
   }
}
