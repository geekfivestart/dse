package org.apache.cassandra.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.hook.DelayingShutdownHook;
import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Completable;
import io.reactivex.functions.Action;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthSchemaChangeListener;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.StreamStateStore;
import org.apache.cassandra.dht.StreamingOptions;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.CallbackExpiredException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.interceptors.AbstractInterceptor;
import org.apache.cassandra.net.interceptors.InterceptionContext;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.net.interceptors.MessageDirection;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.DefaultDiskErrorHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.WindowsTimer;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXProgressSupport;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber, StorageServiceMBean {
   private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
   public static final int RING_DELAY = getRingDelay();
   private final JMXProgressSupport progressSupport = new JMXProgressSupport(this);
   private TokenMetadata tokenMetadata = new TokenMetadata();
   public volatile VersionedValue.VersionedValueFactory valueFactory;
   private volatile boolean bootstrapSafeToReplyEchos;
   private volatile boolean isShutdown;
   private final List<Runnable> preShutdownHooks;
   private final List<Runnable> postShutdownHooks;
   public static final StorageService instance = new StorageService();
   private final Set<InetAddress> replicatingNodes;
   private CassandraDaemon daemon;
   private InetAddress removingNode;
   private volatile boolean isBootstrapMode;
   private boolean isSurveyMode;
   private final AtomicReference<RangeStreamer> currentRebuild;
   private final AtomicBoolean isDecommissioning;
   private volatile boolean initialized;
   private volatile boolean joined;
   private volatile boolean gossipActive;
   private volatile boolean authSetupComplete;
   private double traceProbability;
   private volatile StorageService.Mode operationMode;
   private volatile int totalCFs;
   private volatile int remainingCFs;
   private static final AtomicInteger nextRepairCommand = new AtomicInteger();
   private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers;
   private final ObjectName jmxObjectName;
   private Collection<Token> bootstrapTokens;
   private static final boolean useStrictConsistency = Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement", "true"));
   private static final boolean allowSimultaneousMoves = Boolean.parseBoolean(System.getProperty("cassandra.consistent.simultaneousmoves.allow", "false"));
   private static final boolean joinRing = Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true"));
   private boolean replacing;
   private final StreamStateStore streamStateStore;
   private final AtomicBoolean doneAuthSetup;
   public final NodeSyncService nodeSyncService;

   private static int getRingDelay() {
      String newdelay = System.getProperty("cassandra.ring_delay_ms");
      if(newdelay != null) {
         logger.info("Overriding RING_DELAY to {}ms", newdelay);
         return Integer.parseInt(newdelay);
      } else {
         return 30000;
      }
   }

   /** @deprecated */
   @Deprecated
   public boolean isInShutdownHook() {
      return this.isShutdown();
   }

   public boolean isShutdown() {
      return this.isShutdown;
   }

   public boolean isSafeToReplyEchos() {
      return this.initialized || this.bootstrapSafeToReplyEchos;
   }

   public Collection<Range<Token>> getLocalRanges(String keyspaceName) {
      return this.getRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddress());
   }

   public Collection<Range<Token>> getNormalizedLocalRanges(String keyspaceName) {
      return Keyspace.open(keyspaceName).getReplicationStrategy().getNormalizedLocalRanges();
   }

   public Collection<Range<Token>> getPrimaryRanges(String keyspace) {
      return this.getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddress());
   }

   public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace) {
      return this.getPrimaryRangeForEndpointWithinDC(keyspace, FBUtilities.getBroadcastAddress());
   }

   public void installDiskErrorHandler() {
      JVMStabilityInspector.setDiskErrorHandler(new DefaultDiskErrorHandler(JVMStabilityInspector.killer(), this));
   }

   private void setBootstrapStateBlocking(SystemKeyspace.BootstrapState state) {
      TPCUtils.blockingAwait(SystemKeyspace.setBootstrapState(state));
   }

   private Multimap<InetAddress, Token> loadTokensBlocking() {
      return (Multimap)TPCUtils.blockingGet(SystemKeyspace.loadTokens());
   }

   private void updateTokensBlocking(InetAddress endpoint, Collection<Token> tokens) {
      TPCUtils.blockingAwait(SystemKeyspace.updateTokens(endpoint, tokens));
   }

   private void updateTokensBlocking(Collection<Token> tokens) {
      TPCUtils.blockingAwait(SystemKeyspace.updateTokens(tokens));
   }

   public void setTokens(Collection<Token> tokens) {
      assert tokens != null && !tokens.isEmpty() : "Node needs at least one token.";

      if(logger.isDebugEnabled()) {
         logger.debug("Setting tokens to {}", tokens);
      }

      this.updateTokensBlocking(tokens);
      Collection<Token> localTokens = this.getLocalTokensBlocking();
      this.setGossipTokens(localTokens);
      this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
      this.setMode(StorageService.Mode.NORMAL, false);
   }

   public void setGossipTokens(Collection<Token> tokens) {
      List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
      states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
      states.add(Pair.create(ApplicationState.STATUS, this.valueFactory.normal(tokens)));
      Gossiper.instance.addLocalApplicationStates(states);
   }

   public StorageService() {
      super(Executors.newSingleThreadExecutor());
      this.valueFactory = new VersionedValue.VersionedValueFactory(this.tokenMetadata.partitioner);
      this.bootstrapSafeToReplyEchos = false;
      this.isShutdown = false;
      this.preShutdownHooks = new ArrayList();
      this.postShutdownHooks = new ArrayList();
      this.replicatingNodes = Collections.synchronizedSet(new HashSet());
      this.isSurveyMode = Boolean.parseBoolean(System.getProperty("cassandra.write_survey", "false"));
      this.currentRebuild = new AtomicReference();
      this.isDecommissioning = new AtomicBoolean();
      this.initialized = false;
      this.joined = false;
      this.gossipActive = false;
      this.authSetupComplete = false;
      this.traceProbability = 0.0D;
      this.operationMode = StorageService.Mode.STARTING;
      this.lifecycleSubscribers = new CopyOnWriteArrayList();
      this.bootstrapTokens = null;
      this.streamStateStore = new StreamStateStore();
      this.doneAuthSetup = new AtomicBoolean(false);
      this.nodeSyncService = new NodeSyncService();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         this.jmxObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
         mbs.registerMBean(this, this.jmxObjectName);
         mbs.registerMBean(StreamManager.instance, new ObjectName("org.apache.cassandra.net:type=StreamManager"));
         mbs.registerMBean(MemoryOnlyStatus.instance, new ObjectName("org.apache.cassandra.db:type=MemoryOnlyStatus"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public void registerDaemon(CassandraDaemon daemon) {
      this.daemon = daemon;
   }

   public void register(IEndpointLifecycleSubscriber subscriber) {
      this.lifecycleSubscribers.add(subscriber);
   }

   public void unregister(IEndpointLifecycleSubscriber subscriber) {
      this.lifecycleSubscribers.remove(subscriber);
   }

   public void stopGossiping() {
      this.stopGossiping("by operator request");
   }

   private void stopGossiping(String reason) {
      if(this.gossipActive) {
         logger.warn("Stopping gossip {}", reason);
         Gossiper.instance.stop();
         this.gossipActive = false;
      }

   }

   public synchronized void startGossiping() {
      if(!this.gossipActive) {
         this.checkServiceAllowedToStart("gossip");
         logger.warn("Starting gossip by operator request");
         Collection<Token> tokens = this.getSavedTokensBlocking();
         boolean validTokens = tokens != null && !tokens.isEmpty();

         assert !this.joined && !joinRing || validTokens : "Cannot start gossiping for a node intended to join without valid tokens";

         if(validTokens) {
            this.setGossipTokens(tokens);
         }

         Gossiper.instance.forceNewerGeneration();
         Gossiper.instance.start((int)(System.currentTimeMillis() / 1000L));
         addLocalFakeDseGossipState();
         this.gossipActive = true;
      }

   }

   public boolean isGossipRunning() {
      return Gossiper.instance.isEnabled();
   }

   public synchronized void startNativeTransport() {
      this.checkServiceAllowedToStart("native transport");
      if(this.daemon == null) {
         throw new IllegalStateException("No configured daemon");
      } else {
         try {
            this.daemon.startNativeTransport();
         } catch (Exception var2) {
            throw new RuntimeException("Error starting native transport: " + var2.getMessage());
         }
      }
   }

   public void stopNativeTransport() {
      if(this.daemon == null) {
         throw new IllegalStateException("No configured daemon");
      } else {
         this.daemon.stopNativeTransport();
      }
   }

   public boolean isNativeTransportRunning() {
      return this.daemon == null?false:this.daemon.isNativeTransportRunning();
   }

   public CompletableFuture stopTransportsAsync() {
      return CompletableFuture.allOf(new CompletableFuture[]{this.stopGossipingAsync(), this.stopNativeTransportAsync()});
   }

   private CompletableFuture stopGossipingAsync() {
      return !this.isGossipActive()?TPCUtils.completedFuture():CompletableFuture.supplyAsync(() -> {
         this.stopGossiping("by internal request (typically an unrecoverable error)");
         return null;
      }, StageManager.getStage(Stage.GOSSIP));
   }

   private CompletableFuture stopNativeTransportAsync() {
      if(!this.isNativeTransportRunning()) {
         return TPCUtils.completedFuture();
      } else {
         logger.error("Stopping native transport");
         return this.daemon.stopNativeTransportAsync();
      }
   }

   private void shutdownClientServers() {
      this.setNativeTransportReady(false);
      this.stopNativeTransport();
   }

   public void stopClient() {
      Gossiper.instance.unregister(this);
      Gossiper.instance.stop();
      MessagingService.instance().shutdown();
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
      StageManager.shutdownNow();
   }

   public boolean isInitialized() {
      return this.initialized;
   }

   public boolean isGossipActive() {
      return this.gossipActive;
   }

   public boolean isDaemonSetupCompleted() {
      return this.daemon == null?false:this.daemon.setupCompleted();
   }

   public void stopDaemon() {
      if(this.daemon == null) {
         throw new IllegalStateException("No configured daemon");
      } else {
         this.daemon.deactivate();
      }
   }

   @VisibleForTesting
   public CassandraDaemon getDaemon() {
      return this.daemon;
   }

   private synchronized UUID prepareForReplacement(UUID localHostId) throws ConfigurationException {
      if(SystemKeyspace.bootstrapComplete()) {
         throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");
      } else if(!joinRing) {
         throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");
      } else if(!DatabaseDescriptor.isAutoBootstrap() && !Boolean.getBoolean("cassandra.allow_unsafe_replace")) {
         throw new RuntimeException("Replacing a node without bootstrapping risks invalidating consistency guarantees as the expected data may not be present until repair is run. To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true");
      } else {
         InetAddress replaceAddress = DatabaseDescriptor.getReplaceAddress();
         logger.info("Gathering node replacement information for {}", replaceAddress);
         Map<InetAddress, EndpointState> epStates = Gossiper.instance.doShadowRound();
         if(epStates.get(replaceAddress) == null) {
            throw new RuntimeException(String.format("Cannot replace_address %s because it doesn't exist in gossip", new Object[]{replaceAddress}));
         } else {
            try {
               VersionedValue tokensVersionedValue = ((EndpointState)epStates.get(replaceAddress)).getApplicationState(ApplicationState.TOKENS);
               if(tokensVersionedValue == null) {
                  throw new RuntimeException(String.format("Could not find tokens for %s to replace", new Object[]{replaceAddress}));
               }

               this.bootstrapTokens = TokenSerializer.deserialize(this.tokenMetadata.partitioner, new DataInputStream(new ByteArrayInputStream(tokensVersionedValue.toBytes())));
            } catch (IOException var5) {
               throw new RuntimeException(var5);
            }

            if(isReplacingSameAddress()) {
               localHostId = Gossiper.instance.getHostId(replaceAddress, epStates);
               TPCUtils.blockingGet(SystemKeyspace.setLocalHostId(localHostId));
            }

            return localHostId;
         }
      }
   }

   private synchronized void checkForEndpointCollision(UUID localHostId, Set<InetAddress> peers) throws ConfigurationException {
      if(Boolean.getBoolean("cassandra.allow_unsafe_join")) {
         logger.warn("Skipping endpoint collision check as cassandra.allow_unsafe_join=true");
      } else {
         logger.debug("Starting shadow gossip round to check for endpoint collision");
         Map<InetAddress, EndpointState> epStates = Gossiper.instance.doShadowRound(peers);
         if(epStates.isEmpty() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())) {
            logger.info("Unable to gossip with any peers but continuing anyway since node is in its own seed list");
         }

         if(!Gossiper.instance.isSafeForStartup(FBUtilities.getBroadcastAddress(), localHostId, this.shouldBootstrap(), epStates)) {
            throw new RuntimeException(String.format("A node with address %s already exists, cancelling join. Use cassandra.replace_address if you want to replace this node.", new Object[]{FBUtilities.getBroadcastAddress()}));
         } else {
            if(this.shouldBootstrap() && useStrictConsistency && !this.allowSimultaneousMoves()) {
               Iterator var4 = epStates.entrySet().iterator();

               while(var4.hasNext()) {
                  Entry<InetAddress, EndpointState> entry = (Entry)var4.next();
                  if(!((InetAddress)entry.getKey()).equals(FBUtilities.getBroadcastAddress()) && ((EndpointState)entry.getValue()).getApplicationState(ApplicationState.STATUS) != null) {
                     String[] pieces = splitValue(((EndpointState)entry.getValue()).getApplicationState(ApplicationState.STATUS));

                     assert pieces.length > 0;

                     String state = pieces[0];
                     if(state.equals("BOOT") || state.equals("LEAVING") || state.equals("MOVING")) {
                        throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
                     }
                  }
               }
            }

         }
      }
   }

   private boolean allowSimultaneousMoves() {
      return allowSimultaneousMoves && DatabaseDescriptor.getNumTokens() == 1;
   }

   public void unsafeInitialize() throws ConfigurationException {
      this.initialized = true;
      this.gossipActive = true;
      Gossiper.instance.register(this);
      Gossiper.instance.start((int)(System.currentTimeMillis() / 1000L));
      Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, this.valueFactory.networkVersion());
      addLocalFakeDseGossipState();
      if(!MessagingService.instance().isListening()) {
         MessagingService.instance().listen();
      }

   }

   public void populateTokenMetadata() {
      if(Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true"))) {
         logger.info("Populating token metadata from system tables");
         Multimap<InetAddress, Token> loadedTokens = this.loadTokensBlocking();
         if(!this.shouldBootstrap()) {
            loadedTokens.putAll(FBUtilities.getBroadcastAddress(), this.getSavedTokensBlocking());
         }

         Iterator var2 = loadedTokens.keySet().iterator();

         while(var2.hasNext()) {
            InetAddress ep = (InetAddress)var2.next();
            this.tokenMetadata.updateNormalTokens(loadedTokens.get(ep), ep);
         }

         logger.info("Token metadata: {}", this.tokenMetadata);
      }

   }

   public synchronized void initServer() throws ConfigurationException {
      this.initServer(RING_DELAY);
   }

   public synchronized void initServer(int delay) throws ConfigurationException {
      SystemKeyspace.finishStartupBlocking();
      logger.info("DSE DB version: {}", FBUtilities.getReleaseVersionString());
      logger.info("CQL supported versions: {} (default: {})", StringUtils.join(ClientState.getCQLSupportedVersion(), ", "), ClientState.DEFAULT_CQL_VERSION);
      logger.info("Native protocol supported versions: {} (default: {})", StringUtils.join(ProtocolVersion.supportedVersions(), ", "), ProtocolVersion.CURRENT);

      try {
         Class.forName("org.apache.cassandra.service.StorageProxy");
      } catch (ClassNotFoundException var6) {
         throw new AssertionError(var6);
      }

      Thread drainOnShutdown = NamedThreadFactory.createThread(new WrappedRunnable() {
         public void runMayThrow() throws InterruptedException, ExecutionException, IOException {
            StorageService.this.drain(true);
            if(FBUtilities.isWindows) {
               WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
            }

            DelayingShutdownHook logbackHook = new DelayingShutdownHook();
            logbackHook.setContext((LoggerContext)LoggerFactory.getILoggerFactory());
            logbackHook.run();
         }
      }, "StorageServiceShutdownHook");
      JVMStabilityInspector.registerShutdownHook(drainOnShutdown, this::onShutdownHookRemoved);
      this.replacing = this.isReplacing();
      if(!Boolean.parseBoolean(System.getProperty("cassandra.start_gossip", "true"))) {
         logger.info("Not starting gossip as requested.");
         this.loadRingState();
         this.initialized = true;
      } else {
         this.prepareToJoin();

         try {
            CacheService.instance.counterCache.loadSavedAsync().get();
         } catch (Throwable var5) {
            JVMStabilityInspector.inspectThrowable(var5);
            logger.warn("Error loading counter cache", var5);
         }

         if(joinRing) {
            this.joinTokenRing(delay);
         } else {
            Collection<Token> tokens = this.getSavedTokensBlocking();
            if(!tokens.isEmpty()) {
               this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
               List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
               states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
               states.add(Pair.create(ApplicationState.STATUS, this.valueFactory.hibernate(true)));
               Gossiper.instance.addLocalApplicationStates(states);
            }

            TPCUtils.blockingAwait(this.doAuthSetup());
            TPCUtils.blockingAwait(this.doAuditLoggingSetup());
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
         }

         logger.info("Snitch information: {}, local DC:{} / rack:{}", new Object[]{DatabaseDescriptor.getEndpointSnitch(), DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter(), DatabaseDescriptor.getEndpointSnitch().getLocalRack()});
         this.initialized = true;
      }
   }

   private void primeConnections() {
      Set<InetAddress> liveRingMembers = Collections.emptySet();
      int MAX_PRIMING_ATTEMPTS = Integer.getInteger("cassandra.max_gossip_priming_attempts", 3).intValue();
      String key = "NULL";
      String longKey = Strings.repeat(key, (int)OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD / key.length() + 1);
      longKey = longKey.substring(0, (int)OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD - 1);
      TableMetadata cf = SystemKeyspace.metadata().tables.getNullable("local");
      DecoratedKey dKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.wrap(key.getBytes()));
      DecoratedKey dLongKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.wrap(longKey.getBytes()));
      int i = 0;

      label74:
      while(i < MAX_PRIMING_ATTEMPTS) {
         liveRingMembers = this.getLiveRingMembers();
         SinglePartitionReadCommand qs = SinglePartitionReadCommand.create(cf, FBUtilities.nowInSeconds(), dKey, ColumnFilter.selection(RegularAndStaticColumns.NONE), new ClusteringIndexSliceFilter(Slices.ALL, false));
         SinglePartitionReadCommand ql = SinglePartitionReadCommand.create(cf, FBUtilities.nowInSeconds(), dLongKey, ColumnFilter.selection(RegularAndStaticColumns.NONE), new ClusteringIndexSliceFilter(Slices.ALL, false));
         HashMultimap<InetAddress, CompletableFuture<ReadResponse>> responses = HashMultimap.create();
         Iterator var12 = liveRingMembers.iterator();

         while(true) {
            InetAddress ep;
            do {
               if(!var12.hasNext()) {
                  try {
                     FBUtilities.waitOnFutures(Lists.newArrayList(responses.values()), DatabaseDescriptor.getReadRpcTimeout());
                  } catch (Throwable var16) {
                     Iterator var21 = responses.entries().iterator();

                     while(var21.hasNext()) {
                        Entry<InetAddress, CompletableFuture<ReadResponse>> entry = (Entry)var21.next();
                        if(!((CompletableFuture)entry.getValue()).isCompletedExceptionally()) {
                           logger.debug("Timeout waiting for priming response from {}", entry.getKey());
                        }
                     }

                     ++i;
                     continue label74;
                  }

                  logger.debug("All priming requests succeeded");
                  break label74;
               }

               ep = (InetAddress)var12.next();
            } while(ep.equals(FBUtilities.getBroadcastAddress()));

            OutboundTcpConnectionPool pool = (OutboundTcpConnectionPool)MessagingService.instance().getConnectionPool(ep).join();

            try {
               pool.waitForStarted();
            } catch (IllegalStateException var17) {
               logger.warn("Outgoing Connection pool failed to start for {}", ep);
               continue;
            }

            responses.put(ep, MessagingService.instance().sendSingleTarget(Verbs.READS.SINGLE_READ.newRequest(ep, qs)));
            responses.put(ep, MessagingService.instance().sendSingleTarget(Verbs.READS.SINGLE_READ.newRequest(ep, ql)));
         }
      }

      Iterator var18 = liveRingMembers.iterator();

      while(var18.hasNext()) {
         InetAddress ep = (InetAddress)var18.next();
         if(!ep.equals(FBUtilities.getBroadcastAddress())) {
            OutboundTcpConnectionPool pool = (OutboundTcpConnectionPool)MessagingService.instance().getConnectionPool(ep).join();
            if(!pool.gossip().isSocketOpen()) {
               logger.warn("Gossip connection to {} not open", ep);
            }

            if(!pool.small().isSocketOpen()) {
               logger.warn("Small message connection to {} not open", ep);
            }

            if(!pool.large().isSocketOpen()) {
               logger.warn("Large message connection to {} not open", ep);
            }
         }
      }

   }

   private void loadRingState() {
      if(Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true"))) {
         logger.info("Loading persisted ring state");
         Multimap<InetAddress, Token> loadedTokens = this.loadTokensBlocking();
         Map<InetAddress, UUID> loadedHostIds = SystemKeyspace.getHostIds();
         Iterator var3 = loadedTokens.keySet().iterator();

         while(var3.hasNext()) {
            InetAddress ep = (InetAddress)var3.next();
            if(ep.equals(FBUtilities.getBroadcastAddress())) {
               this.removeEndpointBlocking(ep, false);
            } else {
               if(loadedHostIds.containsKey(ep)) {
                  this.tokenMetadata.updateHostId((UUID)loadedHostIds.get(ep), ep);
               }

               Gossiper.instance.addSavedEndpoint(ep);
            }
         }
      }

   }

   public boolean isReplacing() {
      if(System.getProperty("cassandra.replace_address_first_boot", (String)null) != null && SystemKeyspace.bootstrapComplete()) {
         logger.info("Replace address on first boot requested; this node is already bootstrapped");
         return false;
      } else {
         return DatabaseDescriptor.getReplaceAddress() != null;
      }
   }

   public void onShutdownHookRemoved() {
      if(FBUtilities.isWindows) {
         WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
      }

   }

   private boolean shouldBootstrap() {
      return DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && !isSeed();
   }

   public static boolean isSeed() {
      return DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress());
   }

   private void prepareToJoin() throws ConfigurationException {
      if(!this.joined) {
         Map<ApplicationState, VersionedValue> appStates = new EnumMap(ApplicationState.class);
         if(SystemKeyspace.wasDecommissioned()) {
            if(!Boolean.getBoolean("cassandra.override_decommission")) {
               throw new ConfigurationException("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true has been set, or all existing data is removed and the node is bootstrapped again");
            }

            logger.warn("This node was decommissioned, but overriding by operator request.");
            this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.COMPLETED);
         }

         if(DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null) {
            throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");
         }

         Interceptor gossiperInitGuard = newGossiperInitGuard();
         MessagingService.instance().addInterceptor(gossiperInitGuard);
         if(!MessagingService.instance().isListening()) {
            MessagingService.instance().listen();
         }

         UUID localHostId = (UUID)TPCUtils.blockingGet(SystemKeyspace.setLocalHostId());
         if(this.replacing) {
            localHostId = this.prepareForReplacement(localHostId);
            appStates.put(ApplicationState.TOKENS, this.valueFactory.tokens(this.bootstrapTokens));
            if(!DatabaseDescriptor.isAutoBootstrap()) {
               this.updateTokensBlocking(this.bootstrapTokens);
            } else if(isReplacingSameAddress()) {
               logger.warn("Writes will not be forwarded to this node during replacement because it has the same address as the node to be replaced ({}). If the previous node has been down for longer than max_hint_window_in_ms, repair must be run after the replacement process in order to make this node consistent.", DatabaseDescriptor.getReplaceAddress());
               appStates.put(ApplicationState.STATUS, this.valueFactory.hibernate(true));
            }
         } else {
            this.checkForEndpointCollision(localHostId, SystemKeyspace.getHostIds().keySet());
         }

         this.getTokenMetadata().updateHostId(localHostId, FBUtilities.getBroadcastAddress());
         appStates.put(ApplicationState.NET_VERSION, this.valueFactory.networkVersion());
         appStates.put(ApplicationState.HOST_ID, this.valueFactory.hostId(localHostId));
         appStates.put(ApplicationState.NATIVE_TRANSPORT_ADDRESS, this.valueFactory.rpcaddress(FBUtilities.getNativeTransportBroadcastAddress()));
         appStates.put(ApplicationState.RELEASE_VERSION, this.valueFactory.releaseVersion());
         appStates.put(ApplicationState.SCHEMA_COMPATIBILITY_VERSION, this.valueFactory.schemaCompatibilityVersion());
         if(!this.shouldBootstrap()) {
            appStates.put(ApplicationState.STATUS, this.valueFactory.hibernate(true));
         }

         appStates.put(ApplicationState.NATIVE_TRANSPORT_PORT, this.valueFactory.nativeTransportPort(DatabaseDescriptor.getNativeTransportPort()));
         appStates.put(ApplicationState.NATIVE_TRANSPORT_PORT_SSL, this.valueFactory.nativeTransportPortSSL(DatabaseDescriptor.getNativeTransportPortSSL()));
         appStates.put(ApplicationState.STORAGE_PORT, this.valueFactory.storagePort(DatabaseDescriptor.getStoragePort()));
         appStates.put(ApplicationState.STORAGE_PORT_SSL, this.valueFactory.storagePortSSL(DatabaseDescriptor.getSSLStoragePort()));
         DatabaseDescriptor.getJMXPort().ifPresent((port) -> {
            VersionedValue var10000 = (VersionedValue)appStates.put(ApplicationState.JMX_PORT, this.valueFactory.jmxPort(port.intValue()));
         });
         this.loadRingState();
         logger.info("Starting up server gossip");
         Gossiper.instance.register(this);
         int generation = ((Integer)TPCUtils.blockingGet(SystemKeyspace.incrementAndGetGeneration())).intValue();
         Gossiper.instance.start(generation, appStates);
         addLocalFakeDseGossipState();
         this.gossipActive = true;
         if(this.shouldBootstrap()) {
            this.bootstrapSafeToReplyEchos = true;
         }

         MessagingService.instance().removeInterceptor(gossiperInitGuard);
         this.gossipSnitchInfo();
         Schema.instance.updateVersionAndAnnounce();
         LoadBroadcaster.instance.startBroadcasting();
         HintsService.instance.startDispatch();
         Gossiper.waitToSettle("accepting client requests");
         BatchlogManager.instance.start();
      }

   }

   private static void addLocalFakeDseGossipState() {
      EndpointState currentState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
      if(currentState == null || currentState.getApplicationState(ApplicationState.X_11_PADDING) == null) {
         VersionedValue fakeDseGossipState = instance.valueFactory.datacenter("{\"dse_version\":\"" + FBUtilities.getDSEVersionString() + "\"}");
         Gossiper.instance.addLocalApplicationState(ApplicationState.X_11_PADDING, fakeDseGossipState);
      }
   }

   public void waitForSchema(int delay) {
      logger.debug("Waiting for schema (max {} seconds)", Integer.valueOf(delay));

      for(int i = 0; i < delay; i += 1000) {
         if(!Schema.instance.isEmpty()) {
            logger.debug("current schema version: {}", Schema.instance.getVersion());
            break;
         }

         Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
      }

      if(!MigrationManager.isReadyForBootstrap()) {
         this.setMode(StorageService.Mode.JOINING, "waiting for schema information to complete", true);
         MigrationManager.waitUntilReadyForBootstrap();
      }

      logger.info("Has schema with version {}", Schema.instance.getVersion());
   }

   private void joinTokenRing(int delay) throws ConfigurationException {
      this.joined = true;
      long nanoTimeNodeUpdatedOffset = System.nanoTime();
      Set<InetAddress> current = new HashSet();
      if(logger.isDebugEnabled()) {
         logger.debug("Bootstrap variables: {} {} {} {}", new Object[]{Boolean.valueOf(DatabaseDescriptor.isAutoBootstrap()), Boolean.valueOf(SystemKeyspace.bootstrapInProgress()), Boolean.valueOf(SystemKeyspace.bootstrapComplete()), Boolean.valueOf(DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()))});
      }

      if(DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())) {
         logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
      }

      boolean dataAvailable = true;
      boolean bootstrap = this.shouldBootstrap();
      if(bootstrap) {
         if(SystemKeyspace.bootstrapInProgress()) {
            logger.warn("Detected previous bootstrap failure; retrying");
         } else {
            this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.IN_PROGRESS);
         }

         this.setMode(StorageService.Mode.JOINING, "waiting for ring information", true);
         this.waitForSchema(delay);
         this.setMode(StorageService.Mode.JOINING, "schema complete, ready to bootstrap", true);
         this.setMode(StorageService.Mode.JOINING, "waiting for pending range calculation", true);
         PendingRangeCalculatorService.instance.blockUntilFinished();
         this.setMode(StorageService.Mode.JOINING, "calculation complete, ready to bootstrap", true);
         logger.debug("... got ring + schema info ({})", Schema.instance.getVersion());
         String FAILED_REPLACE_MSG;
         if(useStrictConsistency && !this.allowSimultaneousMoves() && (this.tokenMetadata.getBootstrapTokens().valueSet().size() > 0 || this.tokenMetadata.getSizeOfLeavingEndpoints() > 0 || this.tokenMetadata.getSizeOfMovingEndpoints() > 0)) {
            FAILED_REPLACE_MSG = StringUtils.join(this.tokenMetadata.getBootstrapTokens().valueSet(), ',');
            String leavingTokens = StringUtils.join(this.tokenMetadata.getLeavingEndpoints(), ',');
            String movingTokens = StringUtils.join(this.tokenMetadata.getMovingEndpoints().stream().map((e) -> {
               return (InetAddress)e.right;
            }).toArray(), ',');
            throw new UnsupportedOperationException(String.format("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true. Nodes detected, bootstrapping: %s; leaving: %s; moving: %s;", new Object[]{FAILED_REPLACE_MSG, leavingTokens, movingTokens}));
         }

         if(!this.replacing) {
            if(this.tokenMetadata.isMember(FBUtilities.getBroadcastAddress())) {
               FAILED_REPLACE_MSG = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
               throw new UnsupportedOperationException(FAILED_REPLACE_MSG);
            }

            this.setMode(StorageService.Mode.JOINING, "getting bootstrap token", true);
            this.bootstrapTokens = BootStrapper.getBootstrapTokens(this.tokenMetadata, FBUtilities.getBroadcastAddress(), delay);
         } else {
            if(!isReplacingSameAddress()) {
               FAILED_REPLACE_MSG = String.format("If this node failed replace recently, wait at least %ds before starting a new replace operation.", new Object[]{Long.valueOf(TimeUnit.MILLISECONDS.toSeconds((long)Gossiper.QUARANTINE_DELAY))});
               Set<InetAddress> previousAddresses = new HashSet();
               Iterator var9 = this.bootstrapTokens.iterator();

               while(var9.hasNext()) {
                  Token token = (Token)var9.next();
                  InetAddress existing = this.tokenMetadata.getEndpoint(token);
                  if(existing == null) {
                     throw new UnsupportedOperationException("Cannot replace token " + token + " which does not exist! " + FAILED_REPLACE_MSG);
                  }

                  previousAddresses.add(existing);
               }

               long timeSleepOffset = System.currentTimeMillis();
               long timeEnd = timeSleepOffset + (long)delay + (long)Math.min(LoadBroadcaster.BROADCAST_INTERVAL, 1000);

               do {
                  Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
                  Iterator var13 = previousAddresses.iterator();

                  while(var13.hasNext()) {
                     InetAddress existing = (InetAddress)var13.next();
                     long updateTimestamp = Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp();
                     if(nanoTimeNodeUpdatedOffset - updateTimestamp < 0L) {
                        logger.error("Cannot replace a live node {}. Endpoint state changed since {} (nanotime={}). {}", new Object[]{existing, new Date(timeSleepOffset), Long.valueOf(updateTimestamp), FAILED_REPLACE_MSG});
                        throw new UnsupportedOperationException("Cannot replace a live node... " + FAILED_REPLACE_MSG);
                     }
                  }
               } while(System.currentTimeMillis() < timeEnd);

               current.addAll(previousAddresses);
            } else {
               Uninterruptibles.sleepUninterruptibly((long)RING_DELAY, TimeUnit.MILLISECONDS);
            }

            this.setMode(StorageService.Mode.JOINING, "Replacing a node with token(s): " + this.bootstrapTokens, true);
         }

         dataAvailable = this.bootstrap(this.bootstrapTokens);
      } else {
         this.bootstrapTokens = this.getSavedTokensBlocking();
         if(this.bootstrapTokens.isEmpty()) {
            this.bootstrapTokens = BootStrapper.getBootstrapTokens(this.tokenMetadata, FBUtilities.getBroadcastAddress(), delay);
         } else {
            if(this.bootstrapTokens.size() != DatabaseDescriptor.getNumTokens()) {
               throw new ConfigurationException("Cannot change the number of tokens from " + this.bootstrapTokens.size() + " to " + DatabaseDescriptor.getNumTokens());
            }

            logger.info("Using saved tokens {}", this.bootstrapTokens);
         }
      }

      this.maybeAddOrUpdateKeyspace(TraceKeyspace.metadata()).andThen(this.maybeAddOrUpdateKeyspace(SystemDistributedKeyspace.metadata())).blockingAwait();
      if(!this.isSurveyMode) {
         if(dataAvailable) {
            this.finishJoiningRing(bootstrap, this.bootstrapTokens);
            if(!current.isEmpty()) {
               Iterator var17 = current.iterator();

               while(var17.hasNext()) {
                  InetAddress existing = (InetAddress)var17.next();
                  Gossiper.instance.replacedEndpoint(existing);
               }
            }

            logger.info("Startup with data available + schema info ({})", Schema.instance.getVersion());
         } else {
            logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. For more, see `nodetool help bootstrap`. {}", SystemKeyspace.getBootstrapState());
            TPCUtils.blockingAwait(this.doAuthSetup());
         }
      } else {
         logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
         TPCUtils.blockingAwait(this.doAuthSetup());
      }

   }

   public static boolean isReplacingSameAddress() {
      InetAddress replaceAddress = DatabaseDescriptor.getReplaceAddress();
      return replaceAddress != null && replaceAddress.equals(FBUtilities.getBroadcastAddress());
   }

   public void gossipSnitchInfo() {
      String dc = DatabaseDescriptor.getLocalDataCenter();
      String rack = DatabaseDescriptor.getLocalRack();
      Gossiper.instance.addLocalApplicationState(ApplicationState.DC, instance.valueFactory.datacenter(dc));
      Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, instance.valueFactory.rack(rack));
   }

   public void joinRing() throws IOException {
      SystemKeyspace.BootstrapState state = SystemKeyspace.getBootstrapState();
      this.joinRing(state.equals(SystemKeyspace.BootstrapState.IN_PROGRESS));
   }

   private synchronized void joinRing(boolean resumedBootstrap) throws IOException {
      if(!this.joined) {
         logger.info("Joining ring by operator request");

         try {
            this.joinTokenRing(0);
         } catch (ConfigurationException var3) {
            throw new IOException(var3.getMessage());
         }
      } else if(this.isSurveyMode) {
         logger.info("Leaving write survey mode and joining ring at operator request");
         this.finishJoiningRing(resumedBootstrap, this.getSavedTokensBlocking());
         this.isSurveyMode = false;
      }

   }

   private void executePreJoinTasks(boolean bootstrap) {
      StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false).filter((cfs) -> {
         return Schema.instance.getUserKeyspaces().contains(cfs.keyspace.getName());
      }).forEach((cfs) -> {
         cfs.indexManager.executePreJoinTasksBlocking(bootstrap);
      });
   }

   private void finishJoiningRing(boolean didBootstrap, Collection<Token> tokens) {
      this.primeConnections();
      this.setMode(StorageService.Mode.JOINING, "Finish joining ring", true);
      this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.COMPLETED);
      this.executePreJoinTasks(didBootstrap);
      this.setTokens(tokens);

      assert this.tokenMetadata.sortedTokens().size() > 0;

      TPCUtils.blockingAwait(this.doAuthSetup());
      TPCUtils.blockingAwait(this.doAuditLoggingSetup());
   }

   private Completable doAuthSetup() {
      return this.doneAuthSetup.getAndSet(true)?Completable.complete():this.maybeAddOrUpdateKeyspace(AuthKeyspace.metadata(), AuthKeyspace.tablesIfNotExist(), 0L).doOnComplete(() -> {
         DatabaseDescriptor.getRoleManager().setup();
         DatabaseDescriptor.getAuthenticator().setup();
         DatabaseDescriptor.getAuthorizer().setup();
         Schema.instance.registerListener(new AuthSchemaChangeListener());
         this.authSetupComplete = true;
      });
   }

   public boolean isAuthSetupComplete() {
      return this.authSetupComplete;
   }

   private Completable doAuditLoggingSetup() {
      DatabaseDescriptor.getAuditLogger().setup();
      return Completable.complete();
   }

   private Completable maybeAddKeyspace(KeyspaceMetadata ksm) {
      return MigrationManager.announceNewKeyspace(ksm, 0L, false).onErrorResumeNext((e) -> {
         if(e instanceof AlreadyExistsException) {
            logger.debug("Attempted to create new keyspace {}, but it already exists", ksm.name);
            return Completable.complete();
         } else {
            return Completable.error(e);
         }
      });
   }

   private Completable maybeAddOrUpdateKeyspace(KeyspaceMetadata expected) {
      return this.maybeAddOrUpdateKeyspace(expected, Collections.emptyList(), 0L);
   }

   public Completable maybeAddOrUpdateKeyspace(KeyspaceMetadata expected, List<TableMetadata> tablesIfNotExist, long timestamp) {
      Completable migration;
      if(Schema.instance.getKeyspaceMetadata(expected.name) == null) {
         migration = this.maybeAddKeyspace(expected);
      } else {
         migration = Completable.complete();
      }

      return migration.andThen(Completable.defer(() -> {
         KeyspaceMetadata defined = Schema.instance.getKeyspaceMetadata(expected.name);
         return this.maybeAddOrUpdateTypes(expected.types, defined.types, timestamp);
      })).andThen(Completable.defer(() -> {
         KeyspaceMetadata defined = Schema.instance.getKeyspaceMetadata(expected.name);
         Tables expectedTables = expected.tables;
         Iterator var7 = tablesIfNotExist.iterator();

         while(var7.hasNext()) {
            TableMetadata tableIfNotExists = (TableMetadata)var7.next();
            if(defined.tables.getNullable(tableIfNotExists.name) == null) {
               expectedTables = expectedTables.with(tableIfNotExists);
            }
         }

         return this.maybeAddOrUpdateTables(expectedTables, defined.tables, timestamp);
      }));
   }

   private Completable maybeAddOrUpdateTypes(Types expected, Types defined, long timestamp) {
      List<Completable> migrations = new ArrayList();
      Iterator var6 = expected.iterator();

      while(true) {
         UserType expectedType;
         UserType definedType;
         do {
            if(!var6.hasNext()) {
               return migrations.isEmpty()?Completable.complete():Completable.merge(migrations);
            }

            expectedType = (UserType)var6.next();
            definedType = (UserType)defined.get(expectedType.name).orElse((Object)null);
         } while(definedType != null && definedType.equals(expectedType));

         migrations.add(MigrationManager.forceAnnounceNewType(expectedType, timestamp));
      }
   }

   private Completable maybeAddOrUpdateTables(Tables expected, Tables defined, long timestamp) {
      List<Completable> migrations = new ArrayList();
      Iterator var6 = expected.iterator();

      while(var6.hasNext()) {
         TableMetadata expectedTable = (TableMetadata)var6.next();
         TableMetadata definedTable = (TableMetadata)defined.get(expectedTable.name).orElse((Object)null);
         if(definedTable == null) {
            migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable, timestamp));
         } else if(!definedTable.equalsIgnoringNodeSync(expectedTable)) {
            TableParams newParams = expectedTable.params.unbuild().nodeSync(definedTable.params.nodeSync).build();
            migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable.unbuild().params(newParams).build(), timestamp));
         }
      }

      return migrations.isEmpty()?Completable.complete():Completable.concat(migrations);
   }

   public boolean isJoined() {
      return this.joined && !this.isSurveyMode;
   }

   public void rebuild(String sourceDc) {
      this.rebuild(sourceDc, (String)null, (String)null, (String)null);
   }

   public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources) {
      this.rebuild(keyspace != null?Collections.singletonList(keyspace):Collections.emptyList(), tokens, RebuildMode.NORMAL, 0, StreamingOptions.forRebuild(this.tokenMetadata.cloneOnlyTokenMap(), sourceDc, specificSources));
   }

   public String rebuild(List<String> keyspaces, String tokens, String mode, List<String> srcDcNames, List<String> excludeDcNames, List<String> specifiedSources, List<String> excludeSources) {
      return this.rebuild(keyspaces, tokens, mode, 0, srcDcNames, excludeDcNames, specifiedSources, excludeSources);
   }

   public String rebuild(List<String> keyspaces, String tokens, String mode, int streamingConnectionsPerHost, List<String> srcDcNames, List<String> excludeDcNames, List<String> specifiedSources, List<String> excludeSources) {
      return this.rebuild(keyspaces != null?keyspaces:Collections.emptyList(), tokens, RebuildMode.getMode(mode), streamingConnectionsPerHost, StreamingOptions.forRebuild(this.tokenMetadata.cloneOnlyTokenMap(), srcDcNames, excludeDcNames, specifiedSources, excludeSources));
   }

   private String rebuild(List<String> keyspaces, String tokens, RebuildMode mode, int streamingConnectionsPerHost, StreamingOptions options) {
      keyspaces = keyspaces != null?keyspaces:Collections.emptyList();
      if(keyspaces.isEmpty() && tokens != null) {
         throw new IllegalArgumentException("Cannot specify tokens without keyspace.");
      } else {
         if(streamingConnectionsPerHost <= 0) {
            streamingConnectionsPerHost = DatabaseDescriptor.getStreamingConnectionsPerHost();
         }

         String msg = String.format("%s, %s, %d streaming connections, %s, %s", new Object[]{!keyspaces.isEmpty()?keyspaces:"(All keyspaces)", tokens == null?"(All tokens)":tokens, Integer.valueOf(streamingConnectionsPerHost), mode, options});
         logger.info("starting rebuild for {}", msg);
         long t0 = System.currentTimeMillis();
         RangeStreamer streamer = new RangeStreamer(this.tokenMetadata, (Collection)null, FBUtilities.getBroadcastAddress(), StreamOperation.REBUILD, useStrictConsistency && !this.replacing, DatabaseDescriptor.getEndpointSnitch(), this.streamStateStore, false, streamingConnectionsPerHost, options.toSourceFilter(DatabaseDescriptor.getEndpointSnitch(), FailureDetector.instance));
         if(!this.currentRebuild.compareAndSet((Object)null, streamer)) {
            throw new IllegalStateException("Node is still rebuilding. Check nodetool netstats.");
         } else {
            try {
               if(keyspaces.isEmpty()) {
                  keyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
               }

               String keyspaceName;
               if(tokens == null) {
                  Iterator var10 = keyspaces.iterator();

                  while(var10.hasNext()) {
                     String keyspaceName = (String)var10.next();
                     streamer.addRanges(keyspaceName, this.getLocalRanges(keyspaceName));
                  }

                  mode.beforeStreaming(keyspaces);
               } else {
                  List<Range<Token>> ranges = new ArrayList();
                  Token.TokenFactory factory = this.getTokenFactory();
                  Pattern rangePattern = Pattern.compile("\\(\\s*(-?\\w+)\\s*,\\s*(-?\\w+)\\s*\\]");
                  Scanner tokenScanner = new Scanner(tokens);
                  Throwable var14 = null;

                  try {
                     while(tokenScanner.findInLine(rangePattern) != null) {
                        MatchResult range = tokenScanner.match();
                        Token startToken = factory.fromString(range.group(1));
                        Token endToken = factory.fromString(range.group(2));
                        logger.info("adding range: ({},{}]", startToken, endToken);
                        ranges.add(new Range(startToken, endToken));
                     }

                     if(tokenScanner.hasNext()) {
                        throw new IllegalArgumentException("Unexpected string: " + tokenScanner.next());
                     }
                  } catch (Throwable var42) {
                     var14 = var42;
                     throw var42;
                  } finally {
                     if(tokenScanner != null) {
                        if(var14 != null) {
                           try {
                              tokenScanner.close();
                           } catch (Throwable var41) {
                              var14.addSuppressed(var41);
                           }
                        } else {
                           tokenScanner.close();
                        }
                     }

                  }

                  Map<String, Collection<Range<Token>>> keyspaceRanges = new HashMap();
                  Iterator var55 = keyspaces.iterator();

                  while(var55.hasNext()) {
                     keyspaceName = (String)var55.next();
                     Collection<Range<Token>> localRanges = this.getLocalRanges(keyspaceName);
                     Set<Range<Token>> specifiedNotFoundRanges = new HashSet(ranges);
                     Iterator var18 = ranges.iterator();

                     while(true) {
                        while(var18.hasNext()) {
                           Range<Token> specifiedRange = (Range)var18.next();
                           Iterator var20 = localRanges.iterator();

                           while(var20.hasNext()) {
                              Range<Token> localRange = (Range)var20.next();
                              if(localRange.contains((AbstractBounds)specifiedRange)) {
                                 specifiedNotFoundRanges.remove(specifiedRange);
                                 break;
                              }
                           }
                        }

                        if(!specifiedNotFoundRanges.isEmpty()) {
                           throw new IllegalArgumentException(String.format("The specified range(s) %s is not a range that is owned by this node. Please ensure that all token ranges specified to be rebuilt belong to this node.", new Object[]{specifiedNotFoundRanges}));
                        }

                        streamer.addRanges(keyspaceName, ranges);
                        keyspaceRanges.put(keyspaceName, ranges);
                        break;
                     }
                  }

                  mode.beforeStreaming((Map)keyspaceRanges);
               }

               StreamResultFuture resultFuture = streamer.fetchAsync();
               resultFuture.get();
               long t = System.currentTimeMillis() - t0;
               long totalBytes = 0L;

               SessionInfo session;
               for(Iterator var57 = resultFuture.getCurrentState().sessions.iterator(); var57.hasNext(); totalBytes += session.getTotalSizeReceived()) {
                  session = (SessionInfo)var57.next();
               }

               keyspaceName = String.format("finished rebuild for %s after %d seconds receiving %s.", new Object[]{msg, Long.valueOf(t / 1000L), FileUtils.stringifyFileSize((double)totalBytes)});
               logger.info("{}", keyspaceName);
               String var60 = keyspaceName;
               return var60;
            } catch (InterruptedException var44) {
               throw new RuntimeException("Interrupted while waiting on rebuild streaming");
            } catch (IllegalStateException | IllegalArgumentException var45) {
               logger.warn("Parameter error while rebuilding node", var45);
               throw new RuntimeException("Parameter error while rebuilding node: " + var45);
            } catch (ExecutionException var46) {
               logger.error("Error while rebuilding node", var46.getCause());
               throw new RuntimeException("Error while rebuilding node: " + var46.getCause().getMessage());
            } catch (RuntimeException var47) {
               logger.error("Error while rebuilding node", var47);
               throw var47;
            } finally {
               this.currentRebuild.set((Object)null);
            }
         }
      }
   }

   public void setRpcTimeout(long value) {
      DatabaseDescriptor.setRpcTimeout(value);
      logger.info("set rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getRpcTimeout() {
      return DatabaseDescriptor.getRpcTimeout();
   }

   public void setReadRpcTimeout(long value) {
      DatabaseDescriptor.setReadRpcTimeout(value);
      logger.info("set read rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getReadRpcTimeout() {
      return DatabaseDescriptor.getReadRpcTimeout();
   }

   public void setRangeRpcTimeout(long value) {
      DatabaseDescriptor.setRangeRpcTimeout(value);
      logger.info("set range rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getRangeRpcTimeout() {
      return DatabaseDescriptor.getRangeRpcTimeout();
   }

   public void setWriteRpcTimeout(long value) {
      DatabaseDescriptor.setWriteRpcTimeout(value);
      logger.info("set write rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getWriteRpcTimeout() {
      return DatabaseDescriptor.getWriteRpcTimeout();
   }

   public void setCounterWriteRpcTimeout(long value) {
      DatabaseDescriptor.setCounterWriteRpcTimeout(value);
      logger.info("set counter write rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getCounterWriteRpcTimeout() {
      return DatabaseDescriptor.getCounterWriteRpcTimeout();
   }

   public void setCasContentionTimeout(long value) {
      DatabaseDescriptor.setCasContentionTimeout(value);
      logger.info("set cas contention rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getCasContentionTimeout() {
      return DatabaseDescriptor.getCasContentionTimeout();
   }

   public void setTruncateRpcTimeout(long value) {
      DatabaseDescriptor.setTruncateRpcTimeout(value);
      logger.info("set truncate rpc timeout to {} ms", Long.valueOf(value));
   }

   public long getTruncateRpcTimeout() {
      return DatabaseDescriptor.getTruncateRpcTimeout();
   }

   public void abortRebuild(String reason) {
      if(reason == null) {
         reason = "Manually aborted";
      }

      RangeStreamer streamer = (RangeStreamer)this.currentRebuild.get();
      if(streamer == null) {
         throw new IllegalStateException("No active rebuild");
      } else {
         streamer.abort(reason);
      }
   }

   public void setStreamThroughputMbPerSec(int value) {
      DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
      logger.info("setstreamthroughput: throttle set to {}", Integer.valueOf(value));
   }

   public int getStreamThroughputMbPerSec() {
      return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
   }

   public void setStreamingConnectionsPerHost(int value) {
      DatabaseDescriptor.setStreamingConnectionsPerHost(value);
   }

   public int getStreamingConnectionsPerHost() {
      return DatabaseDescriptor.getStreamingConnectionsPerHost();
   }

   public void setInterDCStreamThroughputMbPerSec(int value) {
      DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(value);
      logger.info("setinterdcstreamthroughput: throttle set to {}", Integer.valueOf(value));
   }

   public int getInterDCStreamThroughputMbPerSec() {
      return DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec();
   }

   public int getCompactionThroughputMbPerSec() {
      return DatabaseDescriptor.getCompactionThroughputMbPerSec();
   }

   public void setCompactionThroughputMbPerSec(int value) {
      DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
      CompactionManager.instance.setRate((double)value);
   }

   public int getBatchlogReplayThrottleInKB() {
      return DatabaseDescriptor.getBatchlogReplayThrottleInKB();
   }

   public void setBatchlogReplayThrottleInKB(int throttleInKB) {
      DatabaseDescriptor.setBatchlogReplayThrottleInKB(throttleInKB);
      BatchlogManager.instance.setRate(throttleInKB);
   }

   public int getConcurrentCompactors() {
      return DatabaseDescriptor.getConcurrentCompactors();
   }

   public void setConcurrentCompactors(int value) {
      if(value <= 0) {
         throw new IllegalArgumentException("Number of concurrent compactors should be greater than 0.");
      } else {
         DatabaseDescriptor.setConcurrentCompactors(value);
         CompactionManager.instance.setConcurrentCompactors(value);
      }
   }

   public int getConcurrentValidators() {
      return DatabaseDescriptor.getConcurrentValidations();
   }

   public void setConcurrentValidators(int value) {
      DatabaseDescriptor.setConcurrentValidations(value);
      CompactionManager.instance.setConcurrentValidations(DatabaseDescriptor.getConcurrentValidations());
   }

   public int getConcurrentViewBuilders() {
      return DatabaseDescriptor.getConcurrentViewBuilders();
   }

   public void setConcurrentViewBuilders(int value) {
      if(value <= 0) {
         throw new IllegalArgumentException("Number of concurrent view builders should be greater than 0.");
      } else {
         DatabaseDescriptor.setConcurrentViewBuilders(value);
         CompactionManager.instance.setConcurrentViewBuilders(DatabaseDescriptor.getConcurrentViewBuilders());
      }
   }

   public boolean isIncrementalBackupsEnabled() {
      return DatabaseDescriptor.isIncrementalBackupsEnabled();
   }

   public void setIncrementalBackupsEnabled(boolean value) {
      DatabaseDescriptor.setIncrementalBackupsEnabled(value);
   }

   private void setMode(StorageService.Mode m, boolean log) {
      this.setMode(m, (String)null, log);
   }

   private void setMode(StorageService.Mode m, String msg, boolean log) {
      this.operationMode = m;
      String logMsg = msg == null?m.toString():String.format("%s: %s", new Object[]{m, msg});
      if(log) {
         logger.info(logMsg);
      } else {
         logger.debug(logMsg);
      }

   }

   private boolean bootstrap(final Collection<Token> tokens) {
      this.isBootstrapMode = true;
      this.updateTokensBlocking(tokens);
      if(this.replacing && isReplacingSameAddress()) {
         this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
         this.removeEndpointBlocking(DatabaseDescriptor.getReplaceAddress(), false);
      } else {
         List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
         states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
         states.add(Pair.create(ApplicationState.STATUS, this.replacing?this.valueFactory.bootReplacing(DatabaseDescriptor.getReplaceAddress()):this.valueFactory.bootstrapping(tokens)));
         Gossiper.instance.addLocalApplicationStates(states);
         this.setMode(StorageService.Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
         Uninterruptibles.sleepUninterruptibly((long)RING_DELAY, TimeUnit.MILLISECONDS);
      }

      if(!Gossiper.instance.seenAnySeed()) {
         throw new IllegalStateException("Unable to contact any seeds!");
      } else {
         if(Boolean.getBoolean("cassandra.reset_bootstrap_progress")) {
            logger.info("Resetting bootstrap progress to start fresh");
            SystemKeyspace.resetAvailableRangesBlocking();
         }

         this.invalidateDiskBoundaries();
         this.setMode(StorageService.Mode.JOINING, "Starting to bootstrap...", true);
         BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, this.tokenMetadata);
         bootstrapper.addProgressListener(this.progressSupport);
         ListenableFuture<StreamState> bootstrapStream = bootstrapper.bootstrap(this.streamStateStore, useStrictConsistency && !this.replacing);
         Futures.addCallback(bootstrapStream, new FutureCallback<StreamState>() {
            public void onSuccess(StreamState streamState) {
               StorageService.this.bootstrapFinished();
               StorageService.logger.info("Bootstrap completed! for the tokens {}", tokens);
            }

            public void onFailure(Throwable e) {
               StorageService.logger.warn("Error during bootstrap.", e);
            }
         });

         try {
            bootstrapStream.get();
            return true;
         } catch (Throwable var5) {
            logger.error("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.", var5);
            return false;
         }
      }
   }

   private void invalidateDiskBoundaries() {
      Iterator var1 = Keyspace.all().iterator();

      while(var1.hasNext()) {
         Keyspace keyspace = (Keyspace)var1.next();
         Iterator var3 = keyspace.getColumnFamilyStores().iterator();

         while(var3.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
            Iterator var5 = cfs.concatWithIndexes().iterator();

            while(var5.hasNext()) {
               ColumnFamilyStore store = (ColumnFamilyStore)var5.next();
               store.invalidateDiskBoundaries();
            }
         }
      }

   }

   private void markViewsAsBuiltBlocking() {
      ArrayList<CompletableFuture> futures = new ArrayList();
      Iterator var2 = Schema.instance.getUserKeyspaces().iterator();

      while(var2.hasNext()) {
         String keyspace = (String)var2.next();
         Iterator var4 = Schema.instance.getKeyspaceMetadata(keyspace).views.iterator();

         while(var4.hasNext()) {
            ViewMetadata view = (ViewMetadata)var4.next();
            futures.add(SystemKeyspace.finishViewBuildStatus(view.keyspace, view.name));
         }
      }

      TPCUtils.blockingAwait(CompletableFuture.allOf((CompletableFuture[])futures.toArray(new CompletableFuture[0])));
   }

   private void bootstrapFinished() {
      this.markViewsAsBuiltBlocking();
      this.isBootstrapMode = false;
   }

   public boolean resumeBootstrap() {
      if(this.isBootstrapMode && SystemKeyspace.bootstrapInProgress()) {
         logger.info("Resuming bootstrap...");
         Collection<Token> tokens = this.getSavedTokensBlocking();
         BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, this.tokenMetadata);
         bootstrapper.addProgressListener(this.progressSupport);
         ListenableFuture<StreamState> bootstrapStream = bootstrapper.bootstrap(this.streamStateStore, useStrictConsistency && !this.replacing);
         Futures.addCallback(bootstrapStream, new FutureCallback<StreamState>() {
            public void onSuccess(StreamState streamState) {
               StorageService.this.bootstrapFinished();
               StorageService.this.isSurveyMode = true;

               try {
                  StorageService.this.progressSupport.progress("bootstrap", ProgressEvent.createNotification("Joining ring..."));
                  StorageService.this.joinRing(true);
               } catch (IOException var3) {
                  ;
               }

               StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
               StorageService.logger.info("Resume complete");
            }

            public void onFailure(Throwable e) {
               String message = "Error during bootstrap: ";
               if(e instanceof ExecutionException && e.getCause() != null) {
                  message = message + e.getCause().getMessage();
               } else {
                  message = message + e.getMessage();
               }

               StorageService.logger.error(message, e);
               StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.ERROR, 1, 1, message));
               StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
            }
         });
         return true;
      } else {
         logger.info("Resuming bootstrap is requested, but the node is already bootstrapped.");
         return false;
      }
   }

   public boolean isBootstrapMode() {
      return this.isBootstrapMode;
   }

   public TokenMetadata getTokenMetadata() {
      return this.tokenMetadata;
   }

   public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) {
      Map<List<String>, List<String>> map = new HashMap();
      Iterator var3 = this.getRangeToAddressMap(keyspace).entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Range<Token>, List<InetAddress>> entry = (Entry)var3.next();
         map.put(((Range)entry.getKey()).asList(), this.stringify((Iterable)entry.getValue()));
      }

      return map;
   }

   /** @deprecated */
   @Deprecated
   public String getRpcaddress(InetAddress endpoint) {
      return this.getNativeTransportAddress(endpoint);
   }

   public String getNativeTransportAddress(InetAddress endpoint) {
      return endpoint.equals(FBUtilities.getBroadcastAddress())?FBUtilities.getNativeTransportBroadcastAddress().getHostAddress():(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_TRANSPORT_ADDRESS) == null?endpoint.getHostAddress():Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_TRANSPORT_ADDRESS).value);
   }

   /** @deprecated */
   public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace) {
      return this.getRangeToNativeTransportAddressMap(keyspace);
   }

   public Map<List<String>, List<String>> getRangeToNativeTransportAddressMap(String keyspace) {
      Map<List<String>, List<String>> map = new HashMap();
      Iterator var3 = this.getRangeToAddressMap(keyspace).entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Range<Token>, List<InetAddress>> entry = (Entry)var3.next();
         List<String> nativeTransportAddresses = new ArrayList(((List)entry.getValue()).size());
         Iterator var6 = ((List)entry.getValue()).iterator();

         while(var6.hasNext()) {
            InetAddress endpoint = (InetAddress)var6.next();
            nativeTransportAddresses.add(this.getRpcaddress(endpoint));
         }

         map.put(((Range)entry.getKey()).asList(), nativeTransportAddresses);
      }

      return map;
   }

   public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace) {
      if(keyspace == null) {
         keyspace = (String)Schema.instance.getNonLocalStrategyKeyspaces().get(0);
      }

      Map<List<String>, List<String>> map = new HashMap();
      Iterator var3 = this.tokenMetadata.getPendingRangesMM(keyspace).asMap().entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Range<Token>, Collection<InetAddress>> entry = (Entry)var3.next();
         List<InetAddress> l = new ArrayList((Collection)entry.getValue());
         map.put(((Range)entry.getKey()).asList(), this.stringify(l));
      }

      return map;
   }

   public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace) {
      return this.getRangeToAddressMap(keyspace, this.tokenMetadata.sortedTokens());
   }

   public Map<Range<Token>, List<InetAddress>> getRangeToAddressMapInLocalDC(String keyspace) {
      com.google.common.base.Predicate<InetAddress> isLocalDC = new com.google.common.base.Predicate<InetAddress>() {
         public boolean apply(InetAddress address) {
            return StorageService.this.isLocalDC(address);
         }
      };
      Map<Range<Token>, List<InetAddress>> origMap = this.getRangeToAddressMap(keyspace, this.getTokensInLocalDC());
      Map<Range<Token>, List<InetAddress>> filteredMap = Maps.newHashMap();
      Iterator var5 = origMap.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<Range<Token>, List<InetAddress>> entry = (Entry)var5.next();
         List<InetAddress> endpointsInLocalDC = Lists.newArrayList(Collections2.filter((Collection)entry.getValue(), isLocalDC));
         filteredMap.put(entry.getKey(), endpointsInLocalDC);
      }

      return filteredMap;
   }

   private List<Token> getTokensInLocalDC() {
      List<Token> filteredTokens = Lists.newArrayList();
      Iterator var2 = this.tokenMetadata.sortedTokens().iterator();

      while(var2.hasNext()) {
         Token token = (Token)var2.next();
         InetAddress endpoint = this.tokenMetadata.getEndpoint(token);
         if(this.isLocalDC(endpoint)) {
            filteredTokens.add(token);
         }
      }

      return filteredTokens;
   }

   private boolean isLocalDC(InetAddress targetHost) {
      return DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(targetHost);
   }

   private Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens) {
      if(keyspace == null) {
         keyspace = (String)Schema.instance.getNonLocalStrategyKeyspaces().get(0);
      }

      List<Range<Token>> ranges = this.getAllRanges(sortedTokens);
      return this.constructRangeToEndpointMap(keyspace, ranges);
   }

   public List<String> describeRingJMX(String keyspace) throws IOException {
      List tokenRanges;
      try {
         tokenRanges = this.describeRing(keyspace);
      } catch (InvalidRequestException var6) {
         throw new IOException(var6.getMessage());
      }

      List<String> result = new ArrayList(tokenRanges.size());
      Iterator var4 = tokenRanges.iterator();

      while(var4.hasNext()) {
         TokenRange tokenRange = (TokenRange)var4.next();
         result.add(tokenRange.toString());
      }

      return result;
   }

   public List<TokenRange> describeRing(String keyspace) throws InvalidRequestException {
      return this.describeRing(keyspace, false);
   }

   public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException {
      return this.describeRing(keyspace, true);
   }

   private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC) throws InvalidRequestException {
      if(!Schema.instance.getKeyspaces().contains(keyspace)) {
         throw new InvalidRequestException("No such keyspace: " + keyspace);
      } else if(keyspace != null && !(Keyspace.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)) {
         List<TokenRange> ranges = new ArrayList();
         Token.TokenFactory tf = this.getTokenFactory();
         Map<Range<Token>, List<InetAddress>> rangeToAddressMap = includeOnlyLocalDC?this.getRangeToAddressMapInLocalDC(keyspace):this.getRangeToAddressMap(keyspace);
         Iterator var6 = rangeToAddressMap.entrySet().iterator();

         while(var6.hasNext()) {
            Entry<Range<Token>, List<InetAddress>> entry = (Entry)var6.next();
            ranges.add(TokenRange.create(tf, (Range)entry.getKey(), (List)entry.getValue()));
         }

         return ranges;
      } else {
         throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);
      }
   }

   public Map<String, String> getTokenToEndpointMap() {
      Map<Token, InetAddress> mapInetAddress = this.tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap();
      Map<String, String> mapString = new LinkedHashMap(mapInetAddress.size());
      List<Token> tokens = new ArrayList(mapInetAddress.keySet());
      Collections.sort(tokens);
      Iterator var4 = tokens.iterator();

      while(var4.hasNext()) {
         Token token = (Token)var4.next();
         mapString.put(token.toString(), ((InetAddress)mapInetAddress.get(token)).getHostAddress());
      }

      return mapString;
   }

   public String getLocalHostId() {
      return this.getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress()).toString();
   }

   public UUID getLocalHostUUID() {
      return this.getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress());
   }

   public Map<String, String> getHostIdMap() {
      return this.getEndpointToHostId();
   }

   public Map<String, String> getEndpointToHostId() {
      Map<String, String> mapOut = new HashMap();
      Iterator var2 = this.getTokenMetadata().getEndpointToHostIdMapForReading().entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, UUID> entry = (Entry)var2.next();
         mapOut.put(((InetAddress)entry.getKey()).getHostAddress(), ((UUID)entry.getValue()).toString());
      }

      return mapOut;
   }

   public Map<String, String> getHostIdToEndpoint() {
      Map<String, String> mapOut = new HashMap();
      Iterator var2 = this.getTokenMetadata().getEndpointToHostIdMapForReading().entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, UUID> entry = (Entry)var2.next();
         mapOut.put(((UUID)entry.getValue()).toString(), ((InetAddress)entry.getKey()).getHostAddress());
      }

      return mapOut;
   }

   private Map<Range<Token>, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges) {
      Map<Range<Token>, List<InetAddress>> rangeToEndpointMap = new HashMap(ranges.size());
      Iterator var4 = ranges.iterator();

      while(var4.hasNext()) {
         Range<Token> range = (Range)var4.next();
         rangeToEndpointMap.put(range, Keyspace.open(keyspace).getReplicationStrategy().getNaturalEndpoints(range.right));
      }

      return rangeToEndpointMap;
   }

   public void onStarted(InetAddress endpoint, boolean isNew, EndpointState state) {
      VersionedValue schemaCompatVersion = state.getApplicationState(ApplicationState.SCHEMA_COMPATIBILITY_VERSION);
      if(schemaCompatVersion != null) {
         Schema.instance.updateEndpointCompatibilityVersion(endpoint, Integer.valueOf(schemaCompatVersion.value).intValue());
      }

   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      if(state == ApplicationState.STATUS) {
         String[] pieces = splitValue(value);

         assert pieces.length > 0;

         String moveName = pieces[0];
         byte var7 = -1;
         switch(moveName.hashCode()) {
         case -2014929842:
            if(moveName.equals("MOVING")) {
               var7 = 8;
            }
            break;
         case -1986416409:
            if(moveName.equals("NORMAL")) {
               var7 = 2;
            }
            break;
         case -512818111:
            if(moveName.equals("removing")) {
               var7 = 4;
            }
            break;
         case -266992057:
            if(moveName.equals("BOOT_REPLACE")) {
               var7 = 0;
            }
            break;
         case -169343402:
            if(moveName.equals("shutdown")) {
               var7 = 3;
            }
            break;
         case 2044658:
            if(moveName.equals("BOOT")) {
               var7 = 1;
            }
            break;
         case 2332679:
            if(moveName.equals("LEFT")) {
               var7 = 7;
            }
            break;
         case 768877972:
            if(moveName.equals("LEAVING")) {
               var7 = 6;
            }
            break;
         case 1091836000:
            if(moveName.equals("removed")) {
               var7 = 5;
            }
         }

         switch(var7) {
         case 0:
            this.handleStateBootreplacing(endpoint, pieces);
            break;
         case 1:
            this.handleStateBootstrap(endpoint);
            break;
         case 2:
            this.handleStateNormal(endpoint, "NORMAL");
            break;
         case 3:
            this.handleStateNormal(endpoint, "shutdown");
            break;
         case 4:
         case 5:
            this.handleStateRemoving(endpoint, pieces);
            break;
         case 6:
            this.handleStateLeaving(endpoint);
            break;
         case 7:
            this.handleStateLeft(endpoint, pieces);
            break;
         case 8:
            this.handleStateMoving(endpoint, pieces);
         }
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
         if(epState == null || Gossiper.instance.isDeadState(epState)) {
            logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return;
         }

         if(this.getTokenMetadata().isMember(endpoint)) {
            switch(null.$SwitchMap$org$apache$cassandra$gms$ApplicationState[state.ordinal()]) {
            case 1:
               this.updatePeerInfoBlocking(endpoint, "release_version", value.value);
               break;
            case 2:
               this.updateTopology(endpoint);
               this.updatePeerInfoBlocking(endpoint, "data_center", value.value);
               break;
            case 3:
               this.updateTopology(endpoint);
               this.updatePeerInfoBlocking(endpoint, "rack", value.value);
               break;
            case 4:
               try {
                  InetAddress address = InetAddress.getByName(value.value);
                  this.updatePeerInfoBlocking(endpoint, "rpc_address", address);
                  this.updatePeerInfoBlocking(endpoint, "native_transport_address", address);
                  break;
               } catch (UnknownHostException var8) {
                  throw new RuntimeException(var8);
               }
            case 5:
               this.updatePeerInfoBlocking(endpoint, "schema_version", UUID.fromString(value.value));
               MigrationManager var10000 = MigrationManager.instance;
               MigrationManager.scheduleSchemaPull(endpoint, epState, String.format("gossip schema version change to %s", new Object[]{value.value}));
               break;
            case 6:
               this.updatePeerInfoBlocking(endpoint, "host_id", UUID.fromString(value.value));
               break;
            case 7:
               this.notifyNativeTransportChange(endpoint, epState.isRpcReady());
               break;
            case 8:
               this.updateNetVersion(endpoint, value);
               break;
            case 9:
               SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port", Integer.valueOf(Integer.parseInt(value.value)));
               break;
            case 10:
               SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port_ssl", Integer.valueOf(Integer.parseInt(value.value)));
               break;
            case 11:
               SystemKeyspace.updatePeerInfo(endpoint, "storage_port", Integer.valueOf(Integer.parseInt(value.value)));
               break;
            case 12:
               SystemKeyspace.updatePeerInfo(endpoint, "storage_port_ssl", Integer.valueOf(Integer.parseInt(value.value)));
               break;
            case 13:
               SystemKeyspace.updatePeerInfo(endpoint, "jmx_port", Integer.valueOf(Integer.parseInt(value.value)));
            }
         }
      }

   }

   private static String[] splitValue(VersionedValue value) {
      return value.value.split(VersionedValue.DELIMITER_STR, -1);
   }

   private void updateNetVersion(InetAddress endpoint, VersionedValue value) {
      try {
         org.apache.cassandra.net.ProtocolVersion v = org.apache.cassandra.net.ProtocolVersion.fromHandshakeVersion(Integer.parseInt(value.value));
         MessagingService.instance().setVersion(endpoint, MessagingVersion.from(v));
      } catch (NumberFormatException var4) {
         throw new AssertionError("Got invalid value for NET_VERSION application state: " + value.value);
      }
   }

   public void updateTopology(InetAddress endpoint) {
      if(this.getTokenMetadata().isMember(endpoint)) {
         this.getTokenMetadata().updateTopology(endpoint);
      }

   }

   public void updateTopology() {
      this.getTokenMetadata().updateTopology();
   }

   private void updatePeerInfoBlocking(InetAddress ep, String columnName, Object value) {
      TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(ep, columnName, value));
   }

   private void updatePeerInfoBlocking(InetAddress endpoint) {
      EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
      Iterator var3 = epState.states().iterator();

      while(var3.hasNext()) {
         Entry<ApplicationState, VersionedValue> entry = (Entry)var3.next();
         switch(null.$SwitchMap$org$apache$cassandra$gms$ApplicationState[((ApplicationState)entry.getKey()).ordinal()]) {
         case 1:
            this.updatePeerInfoBlocking(endpoint, "release_version", ((VersionedValue)entry.getValue()).value);
            break;
         case 2:
            this.updatePeerInfoBlocking(endpoint, "data_center", ((VersionedValue)entry.getValue()).value);
            break;
         case 3:
            this.updatePeerInfoBlocking(endpoint, "rack", ((VersionedValue)entry.getValue()).value);
            break;
         case 4:
            try {
               InetAddress address = InetAddress.getByName(((VersionedValue)entry.getValue()).value);
               this.updatePeerInfoBlocking(endpoint, "rpc_address", address);
               this.updatePeerInfoBlocking(endpoint, "native_transport_address", address);
               break;
            } catch (UnknownHostException var6) {
               throw new RuntimeException(var6);
            }
         case 5:
            this.updatePeerInfoBlocking(endpoint, "schema_version", UUID.fromString(((VersionedValue)entry.getValue()).value));
            break;
         case 6:
            this.updatePeerInfoBlocking(endpoint, "host_id", UUID.fromString(((VersionedValue)entry.getValue()).value));
         case 7:
         case 8:
         default:
            break;
         case 9:
            SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port", Integer.valueOf(Integer.parseInt(((VersionedValue)entry.getValue()).value)));
            break;
         case 10:
            SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port_ssl", Integer.valueOf(Integer.parseInt(((VersionedValue)entry.getValue()).value)));
            break;
         case 11:
            SystemKeyspace.updatePeerInfo(endpoint, "storage_port", Integer.valueOf(Integer.parseInt(((VersionedValue)entry.getValue()).value)));
            break;
         case 12:
            SystemKeyspace.updatePeerInfo(endpoint, "storage_port_ssl", Integer.valueOf(Integer.parseInt(((VersionedValue)entry.getValue()).value)));
            break;
         case 13:
            SystemKeyspace.updatePeerInfo(endpoint, "jmx_port", Integer.valueOf(Integer.parseInt(((VersionedValue)entry.getValue()).value)));
         }
      }

   }

   private void notifyNativeTransportChange(InetAddress endpoint, boolean ready) {
      if(ready) {
         this.notifyUp(endpoint);
      } else {
         this.notifyDown(endpoint);
      }

   }

   private void notifyUp(InetAddress endpoint) {
      if(this.isRpcReady(endpoint) && Gossiper.instance.isAlive(endpoint)) {
         Iterator var2 = this.lifecycleSubscribers.iterator();

         while(var2.hasNext()) {
            IEndpointLifecycleSubscriber subscriber = (IEndpointLifecycleSubscriber)var2.next();
            subscriber.onUp(endpoint);
         }

      }
   }

   private void notifyDown(InetAddress endpoint) {
      Iterator var2 = this.lifecycleSubscribers.iterator();

      while(var2.hasNext()) {
         IEndpointLifecycleSubscriber subscriber = (IEndpointLifecycleSubscriber)var2.next();
         subscriber.onDown(endpoint);
      }

   }

   private void notifyJoined(InetAddress endpoint) {
      if(this.isStatus(endpoint, "NORMAL")) {
         Iterator var2 = this.lifecycleSubscribers.iterator();

         while(var2.hasNext()) {
            IEndpointLifecycleSubscriber subscriber = (IEndpointLifecycleSubscriber)var2.next();
            subscriber.onJoinCluster(endpoint);
         }

      }
   }

   private void notifyMoved(InetAddress endpoint) {
      Iterator var2 = this.lifecycleSubscribers.iterator();

      while(var2.hasNext()) {
         IEndpointLifecycleSubscriber subscriber = (IEndpointLifecycleSubscriber)var2.next();
         subscriber.onMove(endpoint);
      }

   }

   private void notifyLeft(InetAddress endpoint) {
      Iterator var2 = this.lifecycleSubscribers.iterator();

      while(var2.hasNext()) {
         IEndpointLifecycleSubscriber subscriber = (IEndpointLifecycleSubscriber)var2.next();
         subscriber.onLeaveCluster(endpoint);
      }

   }

   private boolean isStatus(InetAddress endpoint, String status) {
      EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
      return state != null && state.getStatus().equals(status);
   }

   public boolean isRpcReady(InetAddress endpoint) {
      EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
      return state != null && state.isRpcReady();
   }

   public void setNativeTransportReady(boolean value) {
      EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());

      assert !value || state != null;

      if(state != null) {
         Gossiper.instance.addLocalApplicationState(ApplicationState.NATIVE_TRANSPORT_READY, this.valueFactory.nativeTransportReady(value));
      }

   }

   private Collection<Token> getTokensFor(InetAddress endpoint) {
      return Gossiper.instance.getTokensFor(endpoint, this.tokenMetadata.partitioner);
   }

   private void handleStateBootstrap(InetAddress endpoint) {
      Collection<Token> tokens = this.getTokensFor(endpoint);
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);
      }

      if(this.tokenMetadata.isMember(endpoint)) {
         if(!this.tokenMetadata.isLeaving(endpoint)) {
            logger.info("Node {} state jump to bootstrap", endpoint);
         }

         this.tokenMetadata.removeEndpoint(endpoint);
      }

      this.tokenMetadata.addBootstrapTokens(tokens, endpoint);
      PendingRangeCalculatorService.instance.update();
      this.tokenMetadata.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
   }

   private void handleStateBootreplacing(InetAddress newNode, String[] pieces) {
      InetAddress oldNode;
      try {
         oldNode = InetAddress.getByName(pieces[1]);
      } catch (Exception var6) {
         logger.error("Node {} tried to replace malformed endpoint {}.", new Object[]{newNode, pieces[1], var6});
         return;
      }

      if(FailureDetector.instance.isAlive(oldNode)) {
         throw new RuntimeException(String.format("Node %s is trying to replace alive node %s.", new Object[]{newNode, oldNode}));
      } else {
         Optional<InetAddress> replacingNode = this.tokenMetadata.getReplacingNode(newNode);
         if(replacingNode.isPresent() && !((InetAddress)replacingNode.get()).equals(oldNode)) {
            throw new RuntimeException(String.format("Node %s is already replacing %s but is trying to replace %s.", new Object[]{newNode, replacingNode.get(), oldNode}));
         } else {
            Collection<Token> tokens = this.getTokensFor(newNode);
            if(logger.isDebugEnabled()) {
               logger.debug("Node {} is replacing {}, tokens {}", new Object[]{newNode, oldNode, tokens});
            }

            this.tokenMetadata.addReplaceTokens(tokens, newNode, oldNode);
            PendingRangeCalculatorService.instance.update();
            this.tokenMetadata.updateHostId(Gossiper.instance.getHostId(newNode), newNode);
         }
      }
   }

   private void handleStateNormal(InetAddress endpoint, String status) {
      Collection<Token> tokens = this.getTokensFor(endpoint);
      Set<Token> tokensToUpdateInMetadata = new HashSet();
      Set<Token> tokensToUpdateInSystemKeyspace = new HashSet();
      Set<InetAddress> endpointsToRemove = new HashSet();
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} state {}, token {}", new Object[]{endpoint, status, tokens});
      }

      if(this.tokenMetadata.isMember(endpoint)) {
         logger.info("Node {} state jump to {}", endpoint, status);
      }

      if(tokens.isEmpty() && status.equals("NORMAL")) {
         logger.error("Node {} is in state normal but it has no tokens, state: {}", endpoint, Gossiper.instance.getEndpointStateForEndpoint(endpoint));
      }

      Optional<InetAddress> replacingNode = this.tokenMetadata.getReplacingNode(endpoint);
      if(replacingNode.isPresent()) {
         assert !endpoint.equals(replacingNode.get()) : "Pending replacement endpoint with same address is not supported";

         logger.info("Node {} will complete replacement of {} for tokens {}", new Object[]{endpoint, replacingNode.get(), tokens});
         if(FailureDetector.instance.isAlive((InetAddress)replacingNode.get())) {
            logger.error("Node {} cannot complete replacement of alive node {}.", endpoint, replacingNode.get());
            return;
         }

         endpointsToRemove.add(replacingNode.get());
      }

      Optional<InetAddress> replacementNode = this.tokenMetadata.getReplacementNode(endpoint);
      if(replacementNode.isPresent()) {
         logger.warn("Node {} is currently being replaced by node {}.", endpoint, replacementNode.get());
      }

      this.updatePeerInfoBlocking(endpoint);
      UUID hostId = Gossiper.instance.getHostId(endpoint);
      InetAddress existing = this.tokenMetadata.getEndpointForHostId(hostId);
      if(this.replacing && isReplacingSameAddress() && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null && hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress()))) {
         logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
      } else if(existing != null && !existing.equals(endpoint)) {
         if(existing.equals(FBUtilities.getBroadcastAddress())) {
            logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
            this.tokenMetadata.removeEndpoint(endpoint);
            endpointsToRemove.add(endpoint);
         } else if(Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0) {
            logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", new Object[]{hostId, existing, endpoint, endpoint});
            this.tokenMetadata.removeEndpoint(existing);
            endpointsToRemove.add(existing);
            this.tokenMetadata.updateHostId(hostId, endpoint);
         } else {
            logger.warn("Host ID collision for {} between {} and {}; ignored {}", new Object[]{hostId, existing, endpoint, endpoint});
            this.tokenMetadata.removeEndpoint(endpoint);
            endpointsToRemove.add(endpoint);
         }
      } else {
         this.tokenMetadata.updateHostId(hostId, endpoint);
      }

      Iterator var11 = tokens.iterator();

      while(var11.hasNext()) {
         Token token = (Token)var11.next();
         InetAddress currentOwner = this.tokenMetadata.getEndpoint(token);
         if(currentOwner == null) {
            logger.debug("New node {} at token {}", endpoint, token);
            tokensToUpdateInMetadata.add(token);
            tokensToUpdateInSystemKeyspace.add(token);
         } else if(endpoint.equals(currentOwner)) {
            tokensToUpdateInMetadata.add(token);
            tokensToUpdateInSystemKeyspace.add(token);
         } else if(Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0) {
            tokensToUpdateInMetadata.add(token);
            tokensToUpdateInSystemKeyspace.add(token);
            Multimap<InetAddress, Token> epToTokenCopy = this.getTokenMetadata().getEndpointToTokenMapForReading();
            epToTokenCopy.get(currentOwner).remove(token);
            if(epToTokenCopy.get(currentOwner).size() < 1) {
               endpointsToRemove.add(currentOwner);
            }

            logger.info("Nodes {} and {} have the same token {}.  {} is the new owner", new Object[]{endpoint, currentOwner, token, endpoint});
         } else {
            logger.info("Nodes {} and {} have the same token {}.  Ignoring {}", new Object[]{endpoint, currentOwner, token, endpoint});
         }
      }

      boolean isMember = this.tokenMetadata.isMember(endpoint);
      boolean isMoving = this.tokenMetadata.isMoving(endpoint);
      this.tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
      Iterator var17 = endpointsToRemove.iterator();

      while(var17.hasNext()) {
         InetAddress ep = (InetAddress)var17.next();
         this.removeEndpointBlocking(ep);
         if(this.replacing && DatabaseDescriptor.getReplaceAddress().equals(ep)) {
            Gossiper.instance.replacementQuarantine(ep);
         }
      }

      if(!tokensToUpdateInSystemKeyspace.isEmpty()) {
         this.updateTokensBlocking(endpoint, tokensToUpdateInSystemKeyspace);
      }

      if(!isMoving && this.operationMode != StorageService.Mode.MOVING) {
         if(!isMember) {
            this.notifyJoined(endpoint);
         }
      } else {
         this.tokenMetadata.removeFromMoving(endpoint);
         this.notifyMoved(endpoint);
      }

      PendingRangeCalculatorService.instance.update();
   }

   private void handleStateLeaving(InetAddress endpoint) {
      Collection<Token> tokens = this.getTokensFor(endpoint);
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);
      }

      if(!this.tokenMetadata.isMember(endpoint)) {
         logger.info("Node {} state jump to leaving", endpoint);
         this.tokenMetadata.updateNormalTokens(tokens, endpoint);
      } else if(!this.tokenMetadata.getTokens(endpoint).containsAll(tokens)) {
         logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
         this.tokenMetadata.updateNormalTokens(tokens, endpoint);
      }

      this.tokenMetadata.addLeavingEndpoint(endpoint);
      PendingRangeCalculatorService.instance.update();
   }

   private void handleStateLeft(InetAddress endpoint, String[] pieces) {
      assert pieces.length >= 2;

      Collection<Token> tokens = this.getTokensFor(endpoint);
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} state left, tokens {}", endpoint, tokens);
      }

      this.excise(tokens, endpoint, this.extractExpireTime(pieces));
   }

   private void handleStateMoving(InetAddress endpoint, String[] pieces) {
      assert pieces.length >= 2;

      Token token = this.getTokenFactory().fromString(pieces[1]);
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} state moving, new token {}", endpoint, token);
      }

      this.tokenMetadata.addMovingEndpoint(token, endpoint);
      PendingRangeCalculatorService.instance.update();
   }

   private void handleStateRemoving(InetAddress endpoint, String[] pieces) {
      assert pieces.length > 0;

      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");

         try {
            this.drain();
         } catch (Exception var7) {
            throw new RuntimeException(var7);
         }
      } else {
         if(this.tokenMetadata.isMember(endpoint)) {
            String state = pieces[0];
            Collection<Token> removeTokens = this.tokenMetadata.getTokens(endpoint);
            if("removed".equals(state)) {
               this.excise(removeTokens, endpoint, this.extractExpireTime(pieces));
            } else if("removing".equals(state)) {
               if(logger.isDebugEnabled()) {
                  logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);
               }

               this.tokenMetadata.addLeavingEndpoint(endpoint);
               PendingRangeCalculatorService.instance.update();
               String[] coordinator = splitValue(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR));
               UUID hostId = UUID.fromString(coordinator[1]);
               this.restoreReplicaCount(endpoint, this.tokenMetadata.getEndpointForHostId(hostId));
            }
         } else {
            if("removed".equals(pieces[0])) {
               this.addExpireTimeIfFound(endpoint, this.extractExpireTime(pieces));
            }

            this.removeEndpointBlocking(endpoint);
         }

      }
   }

   private void excise(Collection<Token> tokens, InetAddress endpoint) {
      logger.info("Removing tokens {} for {}", tokens, endpoint);
      UUID hostId = this.tokenMetadata.getHostId(endpoint);
      if(hostId != null && this.tokenMetadata.isMember(endpoint)) {
         long delay = DatabaseDescriptor.getMinRpcTimeout() + DatabaseDescriptor.getWriteRpcTimeout();
         ScheduledExecutors.optionalTasks.schedule(() -> {
            HintsService.instance.excise(hostId);
         }, delay, TimeUnit.MILLISECONDS);
      }

      this.removeEndpointBlocking(endpoint);
      this.tokenMetadata.removeEndpoint(endpoint);
      if(!tokens.isEmpty()) {
         this.tokenMetadata.removeBootstrapTokens(tokens);
      }

      this.notifyLeft(endpoint);
      PendingRangeCalculatorService.instance.update();
   }

   private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime) {
      this.addExpireTimeIfFound(endpoint, expireTime);
      this.excise(tokens, endpoint);
   }

   private void removeEndpointBlocking(InetAddress endpoint) {
      this.removeEndpointBlocking(endpoint, true);
   }

   private void removeEndpointBlocking(InetAddress endpoint, boolean notifyGossip) {
      if(notifyGossip) {
         Gossiper.instance.removeEndpoint(endpoint);
      }

      TPCUtils.blockingAwait(SystemKeyspace.removeEndpoint(endpoint));
   }

   protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime) {
      if(expireTime != 0L) {
         Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
      }

   }

   protected long extractExpireTime(String[] pieces) {
      return Long.parseLong(pieces[2]);
   }

   private Multimap<InetAddress, Range<Token>> getNewSourceRanges(String keyspaceName, Set<Range<Token>> ranges) {
      InetAddress myAddress = FBUtilities.getBroadcastAddress();
      Multimap<Range<Token>, InetAddress> rangeAddresses = Keyspace.open(keyspaceName).getReplicationStrategy().getRangeAddresses(this.tokenMetadata.cloneOnlyTokenMap());
      Multimap<InetAddress, Range<Token>> sourceRanges = HashMultimap.create();
      IFailureDetector failureDetector = FailureDetector.instance;
      Iterator var7 = ranges.iterator();

      while(true) {
         while(var7.hasNext()) {
            Range<Token> range = (Range)var7.next();
            Collection<InetAddress> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert !sources.contains(myAddress);

            Iterator var12 = sources.iterator();

            while(var12.hasNext()) {
               InetAddress source = (InetAddress)var12.next();
               if(failureDetector.isAlive(source)) {
                  sourceRanges.put(source, range);
                  break;
               }
            }
         }

         return sourceRanges;
      }
   }

   private void sendReplicationNotification(InetAddress remote) {
      Request<EmptyPayload, EmptyPayload> request = Verbs.OPERATIONS.REPLICATION_FINISHED.newRequest(remote, EmptyPayload.instance);
      IFailureDetector failureDetector = FailureDetector.instance;
      if(logger.isDebugEnabled()) {
         logger.debug("Notifying {} of replication completion\n", remote);
      }

      while(true) {
         if(failureDetector.isAlive(remote)) {
            CompletableFuture future = MessagingService.instance().sendSingleTarget(request);

            try {
               Uninterruptibles.getUninterruptibly(future);
               return;
            } catch (ExecutionException var6) {
               if(var6.getCause() instanceof CallbackExpiredException) {
                  continue;
               }

               logger.error("Unexpected exception when sending replication notification to " + remote, var6);
               return;
            }
         }

         return;
      }
   }

   private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint) {
      Multimap<String, Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();
      InetAddress myAddress = FBUtilities.getBroadcastAddress();
      Iterator var5 = Schema.instance.getNonLocalStrategyKeyspaces().iterator();

      while(var5.hasNext()) {
         String keyspaceName = (String)var5.next();
         Multimap<Range<Token>, InetAddress> changedRanges = this.getChangedRangesForLeaving(keyspaceName, endpoint);
         Set<Range<Token>> myNewRanges = new HashSet();
         Iterator var9 = changedRanges.entries().iterator();

         while(var9.hasNext()) {
            Entry<Range<Token>, InetAddress> entry = (Entry)var9.next();
            if(((InetAddress)entry.getValue()).equals(myAddress)) {
               myNewRanges.add(entry.getKey());
            }
         }

         Multimap<InetAddress, Range<Token>> sourceRanges = this.getNewSourceRanges(keyspaceName, myNewRanges);
         Iterator var20 = sourceRanges.asMap().entrySet().iterator();

         while(var20.hasNext()) {
            Entry<InetAddress, Collection<Range<Token>>> entry = (Entry)var20.next();
            rangesToFetch.put(keyspaceName, entry);
         }
      }

      StreamPlan stream = new StreamPlan(StreamOperation.RESTORE_REPLICA_COUNT, true, true);
      Iterator var14 = rangesToFetch.keySet().iterator();

      while(var14.hasNext()) {
         String keyspaceName = (String)var14.next();

         Collection ranges;
         InetAddress source;
         InetAddress preferred;
         for(Iterator var17 = rangesToFetch.get(keyspaceName).iterator(); var17.hasNext(); stream.requestRanges(source, preferred, keyspaceName, ranges)) {
            Entry<InetAddress, Collection<Range<Token>>> entry = (Entry)var17.next();
            source = (InetAddress)entry.getKey();
            preferred = SystemKeyspace.getPreferredIP(source);
            ranges = (Collection)entry.getValue();
            if(logger.isDebugEnabled()) {
               logger.debug("Requesting from {} ranges {}", source, StringUtils.join(ranges, ", "));
            }
         }
      }

      StreamResultFuture future = stream.execute();
      Futures.addCallback(future, new FutureCallback<StreamState>() {
         public void onSuccess(StreamState finalState) {
            StorageService.this.sendReplicationNotification(notifyEndpoint);
         }

         public void onFailure(Throwable t) {
            StorageService.logger.warn("Streaming to restore replica count failed", t);
            StorageService.this.sendReplicationNotification(notifyEndpoint);
         }
      });
   }

   private Multimap<Range<Token>, InetAddress> getChangedRangesForLeaving(String keyspaceName, InetAddress endpoint) {
      Collection<Range<Token>> ranges = this.getRangesForEndpoint(keyspaceName, endpoint);
      if(logger.isDebugEnabled()) {
         logger.debug("Node {} ranges [{}]", endpoint, StringUtils.join(ranges, ", "));
      }

      Map<Range<Token>, List<InetAddress>> currentReplicaEndpoints = new HashMap(ranges.size());
      TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
      Iterator var6 = ranges.iterator();

      while(var6.hasNext()) {
         Range<Token> range = (Range)var6.next();
         currentReplicaEndpoints.put(range, Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints((Token)range.right, metadata));
      }

      TokenMetadata temp = this.tokenMetadata.cloneAfterAllLeft();
      if(temp.isMember(endpoint)) {
         temp.removeEndpoint(endpoint);
      }

      Multimap<Range<Token>, InetAddress> changedRanges = HashMultimap.create();

      Range range;
      List newReplicaEndpoints;
      for(Iterator var8 = ranges.iterator(); var8.hasNext(); changedRanges.putAll(range, newReplicaEndpoints)) {
         range = (Range)var8.next();
         newReplicaEndpoints = Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints((Token)range.right, temp);
         newReplicaEndpoints.removeAll((Collection)currentReplicaEndpoints.get(range));
         if(logger.isDebugEnabled()) {
            if(newReplicaEndpoints.isEmpty()) {
               logger.debug("Range {} already in all replicas", range);
            } else {
               logger.debug("Range {} will be responsibility of {}", range, StringUtils.join(newReplicaEndpoints, ", "));
            }
         }
      }

      return changedRanges;
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
      Iterator var3 = epState.states().iterator();

      while(var3.hasNext()) {
         Entry<ApplicationState, VersionedValue> entry = (Entry)var3.next();
         this.onChange(endpoint, (ApplicationState)entry.getKey(), (VersionedValue)entry.getValue());
      }

      MigrationManager var10000 = MigrationManager.instance;
      MigrationManager.scheduleSchemaPull(endpoint, epState, "endpoint joined");
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
      MigrationManager var10000 = MigrationManager.instance;
      MigrationManager.scheduleSchemaPull(endpoint, state, "endpoint alive");
      if(this.tokenMetadata.isMember(endpoint)) {
         this.notifyUp(endpoint);
      }

   }

   public void onRemove(InetAddress endpoint) {
      this.tokenMetadata.removeEndpoint(endpoint);
      PendingRangeCalculatorService.instance.update();
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
      MessagingService.instance().convict(endpoint).join();
      this.notifyDown(endpoint);
   }

   public void onRestart(InetAddress endpoint, EndpointState state) {
      if(state.isAlive()) {
         this.onDead(endpoint, state);
      }

      VersionedValue netVersion = state.getApplicationState(ApplicationState.NET_VERSION);
      if(netVersion != null) {
         this.updateNetVersion(endpoint, netVersion);
      }

   }

   public String getLoadString() {
      return FileUtils.stringifyFileSize((double)StorageMetrics.load.getCount());
   }

   public Map<String, String> getLoadMap() {
      Map<String, String> map = new HashMap();
      Iterator var2 = LoadBroadcaster.instance.getLoadInfo().entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, Double> entry = (Entry)var2.next();
         map.put(((InetAddress)entry.getKey()).getHostAddress(), FileUtils.stringifyFileSize(((Double)entry.getValue()).doubleValue()));
      }

      map.put(FBUtilities.getBroadcastAddress().getHostAddress(), this.getLoadString());
      return map;
   }

   public final void deliverHints(String host) {
      throw new UnsupportedOperationException();
   }

   private Collection<Token> getSavedTokensBlocking() {
      return (Collection)TPCUtils.blockingGet(SystemKeyspace.getSavedTokens());
   }

   private Collection<Token> getLocalTokensBlocking() {
      Collection<Token> tokens = this.getSavedTokensBlocking();
      logger.debug("Got tokens {}", tokens);

      assert tokens != null && !tokens.isEmpty();

      return tokens;
   }

   @Nullable
   public InetAddress getEndpointForHostId(UUID hostId) {
      return this.tokenMetadata.getEndpointForHostId(hostId);
   }

   @Nullable
   public UUID getHostIdForEndpoint(InetAddress address) {
      return this.tokenMetadata.getHostId(address);
   }

   public List<String> getTokens() {
      return this.getTokens(FBUtilities.getBroadcastAddress());
   }

   public List<String> getTokens(String endpoint) throws UnknownHostException {
      return this.getTokens(InetAddress.getByName(endpoint));
   }

   private List<String> getTokens(InetAddress endpoint) {
      List<String> strTokens = new ArrayList();
      Iterator var3 = this.getTokenMetadata().getTokens(endpoint).iterator();

      while(var3.hasNext()) {
         Token tok = (Token)var3.next();
         strTokens.add(tok.toString());
      }

      return strTokens;
   }

   public String getReleaseVersion() {
      return FBUtilities.getReleaseVersionString();
   }

   public String getSchemaVersion() {
      return Schema.instance.getVersion().toString();
   }

   public List<String> getLeavingNodes() {
      return this.stringify(this.tokenMetadata.getLeavingEndpoints());
   }

   public List<String> getMovingNodes() {
      List<String> endpoints = new ArrayList();
      Iterator var2 = this.tokenMetadata.getMovingEndpoints().iterator();

      while(var2.hasNext()) {
         Pair<Token, InetAddress> node = (Pair)var2.next();
         endpoints.add(((InetAddress)node.right).getHostAddress());
      }

      return endpoints;
   }

   public List<String> getJoiningNodes() {
      return this.stringify(this.tokenMetadata.getBootstrapTokens().valueSet());
   }

   public List<String> getLiveNodes() {
      return this.stringify(Gossiper.instance.getLiveMembers());
   }

   public Set<InetAddress> getLiveRingMembers() {
      return this.getLiveRingMembers(false);
   }

   public Set<InetAddress> getLiveRingMembers(boolean excludeDeadStates) {
      Set<InetAddress> ret = new HashSet();
      Iterator var3 = Gossiper.instance.getLiveMembers().iterator();

      while(true) {
         InetAddress ep;
         EndpointState epState;
         do {
            if(!var3.hasNext()) {
               return ret;
            }

            ep = (InetAddress)var3.next();
            if(!excludeDeadStates) {
               break;
            }

            epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
         } while(epState == null || Gossiper.instance.isDeadState(epState));

         if(this.tokenMetadata.isMember(ep)) {
            ret.add(ep);
         }
      }
   }

   public List<String> getUnreachableNodes() {
      return this.stringify(Gossiper.instance.getUnreachableMembers());
   }

   public String[] getAllDataFileLocations() {
      String[] locations = DatabaseDescriptor.getAllDataFileLocations();

      for(int i = 0; i < locations.length; ++i) {
         locations[i] = FileUtils.getCanonicalPath(locations[i]);
      }

      return locations;
   }

   public String getCommitLogLocation() {
      return FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation());
   }

   public String getSavedCachesLocation() {
      return FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation());
   }

   private List<String> stringify(Iterable<InetAddress> endpoints) {
      List<String> stringEndpoints = new ArrayList();
      Iterator var3 = endpoints.iterator();

      while(var3.hasNext()) {
         InetAddress ep = (InetAddress)var3.next();
         stringEndpoints.add(ep.getHostAddress());
      }

      return stringEndpoints;
   }

   public int getCurrentGenerationNumber() {
      return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddress());
   }

   public int forceKeyspaceCleanup(String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.forceKeyspaceCleanup(0, keyspaceName, tables);
   }

   public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      if(SchemaConstants.isLocalSystemKeyspace(keyspaceName)) {
         throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");
      } else {
         CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
         Iterator var5 = this.getValidColumnFamilies(false, false, keyspaceName, tables).iterator();

         while(var5.hasNext()) {
            ColumnFamilyStore cfStore = (ColumnFamilyStore)var5.next();
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup(jobs);
            if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
               status = oneStatus;
            }
         }

         return status.statusCode;
      }
   }

   public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.scrub(disableSnapshot, skipCorrupted, true, 0, keyspaceName, tables);
   }

   public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.scrub(disableSnapshot, skipCorrupted, checkData, 0, keyspaceName, tables);
   }

   public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      return this.scrub(disableSnapshot, skipCorrupted, checkData, false, jobs, keyspaceName, tables);
   }

   public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
      CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
      Iterator var9 = this.getValidColumnFamilies(true, false, keyspaceName, tables).iterator();

      while(var9.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var9.next();
         CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, checkData, jobs);
         if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
            status = oneStatus;
         }
      }

      return status.statusCode;
   }

   public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
      Iterator var5 = this.getValidColumnFamilies(false, false, keyspaceName, tableNames).iterator();

      while(var5.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var5.next();
         CompactionManager.AllSSTableOpStatus oneStatus = cfStore.verify(extendedVerify);
         if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
            status = oneStatus;
         }
      }

      return status.statusCode;
   }

   public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      return this.upgradeSSTables(keyspaceName, excludeCurrentVersion, 0, tableNames);
   }

   public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
      Iterator var6 = this.getValidColumnFamilies(true, true, keyspaceName, tableNames).iterator();

      while(var6.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var6.next();
         CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(excludeCurrentVersion, jobs);
         if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
            status = oneStatus;
         }
      }

      return status.statusCode;
   }

   public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      Iterator var4 = this.getValidColumnFamilies(true, false, keyspaceName, tableNames).iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var4.next();
         cfStore.forceMajorCompaction(splitOutput);
      }

   }

   public int relocateSSTables(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
      return this.relocateSSTables(0, keyspaceName, columnFamilies);
   }

   public int relocateSSTables(int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
      CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
      Iterator var5 = this.getValidColumnFamilies(false, false, keyspaceName, columnFamilies).iterator();

      while(var5.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
         CompactionManager.AllSSTableOpStatus oneStatus = cfs.relocateSSTables(jobs);
         if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
            status = oneStatus;
         }
      }

      return status.statusCode;
   }

   public int garbageCollect(String tombstoneOptionString, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
      CompactionParams.TombstoneOption tombstoneOption = CompactionParams.TombstoneOption.valueOf(tombstoneOptionString);
      CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
      Iterator var7 = this.getValidColumnFamilies(false, false, keyspaceName, columnFamilies).iterator();

      while(var7.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var7.next();
         CompactionManager.AllSSTableOpStatus oneStatus = cfs.garbageCollect(tombstoneOption, jobs);
         if(oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
            status = oneStatus;
         }
      }

      return status.statusCode;
   }

   public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException {
      boolean skipFlush = Boolean.parseBoolean((String)options.getOrDefault("skipFlush", "false"));
      if(entities != null && entities.length > 0 && entities[0].contains(".")) {
         this.takeMultipleTableSnapshot(tag, skipFlush, entities);
      } else {
         this.takeSnapshot(tag, skipFlush, entities);
      }

   }

   public void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException {
      this.takeMultipleTableSnapshot(tag, false, new String[]{keyspaceName + "." + tableName});
   }

   public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException {
      Collection<Range<Token>> tokenRanges = this.createRepairRangeFrom(startToken, endToken);
      Iterator var6 = this.getValidColumnFamilies(true, false, keyspaceName, tableNames).iterator();

      while(var6.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var6.next();
         cfStore.forceCompactionForTokenRange(tokenRanges);
      }

   }

   public void takeSnapshot(String tag, String... keyspaceNames) throws IOException {
      this.takeSnapshot(tag, false, keyspaceNames);
   }

   public void takeMultipleTableSnapshot(String tag, String... tableList) throws IOException {
      this.takeMultipleTableSnapshot(tag, false, tableList);
   }

   private void takeSnapshot(String tag, boolean skipFlush, String... keyspaceNames) throws IOException {
      if(this.operationMode == StorageService.Mode.JOINING) {
         throw new IOException("Cannot snapshot until bootstrap completes");
      } else if(tag != null && !tag.equals("")) {
         Object keyspaces;
         if(keyspaceNames.length == 0) {
            keyspaces = Keyspace.all();
         } else {
            ArrayList<Keyspace> t = new ArrayList(keyspaceNames.length);
            String[] var6 = keyspaceNames;
            int var7 = keyspaceNames.length;

            for(int var8 = 0; var8 < var7; ++var8) {
               String keyspaceName = var6[var8];
               t.add(this.getValidKeyspace(keyspaceName));
            }

            keyspaces = t;
         }

         Iterator var10 = ((Iterable)keyspaces).iterator();

         while(var10.hasNext()) {
            Keyspace keyspace = (Keyspace)var10.next();
            if(keyspace.snapshotExists(tag)) {
               throw new IOException("Snapshot " + tag + " already exists.");
            }
         }

         Set<SSTableReader> snapshotted = new HashSet();
         Iterator var13 = ((Iterable)keyspaces).iterator();

         while(var13.hasNext()) {
            Keyspace keyspace = (Keyspace)var13.next();
            snapshotted.addAll(keyspace.snapshot(tag, (String)null, skipFlush, snapshotted));
         }

      } else {
         throw new IOException("You must supply a snapshot name.");
      }
   }

   private void takeMultipleTableSnapshot(String tag, boolean skipFlush, String... tableList) throws IOException {
      Map<Keyspace, List<String>> keyspaceColumnfamily = new HashMap();
      String[] var5 = tableList;
      int var6 = tableList.length;
      int var7 = 0;

      while(var7 < var6) {
         String table = var5[var7];
         String[] splittedString = StringUtils.split(table, '.');
         if(splittedString.length == 2) {
            String keyspaceName = splittedString[0];
            String tableName = splittedString[1];
            if(keyspaceName == null) {
               throw new IOException("You must supply a keyspace name");
            }

            if(this.operationMode.equals(StorageService.Mode.JOINING)) {
               throw new IOException("Cannot snapshot until bootstrap completes");
            }

            if(tableName == null) {
               throw new IOException("You must supply a table name");
            }

            if(tag != null && !tag.equals("")) {
               Keyspace keyspace = this.getValidKeyspace(keyspaceName);
               ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
               if(columnFamilyStore.snapshotExists(tag)) {
                  throw new IOException("Snapshot " + tag + " already exists.");
               }

               if(!keyspaceColumnfamily.containsKey(keyspace)) {
                  keyspaceColumnfamily.put(keyspace, new ArrayList());
               }

               ((List)keyspaceColumnfamily.get(keyspace)).add(tableName);
               ++var7;
               continue;
            }

            throw new IOException("You must supply a snapshot name.");
         }

         throw new IllegalArgumentException("Cannot take a snapshot on secondary index or invalid column family name. You must supply a column family name in the form of keyspace.columnfamily");
      }

      Set<SSTableReader> snapshotted = new HashSet();
      Iterator var15 = keyspaceColumnfamily.entrySet().iterator();

      while(var15.hasNext()) {
         Entry<Keyspace, List<String>> entry = (Entry)var15.next();
         Iterator var17 = ((List)entry.getValue()).iterator();

         while(var17.hasNext()) {
            String table = (String)var17.next();
            snapshotted.addAll(((Keyspace)entry.getKey()).snapshot(tag, table, skipFlush, snapshotted));
         }
      }

   }

   private Keyspace getValidKeyspace(String keyspaceName) throws IOException {
      if(!Schema.instance.getKeyspaces().contains(keyspaceName)) {
         throw new IOException("Keyspace " + keyspaceName + " does not exist");
      } else {
         return Keyspace.open(keyspaceName);
      }
   }

   public void clearSnapshot(String tag, String... keyspaceNames) throws IOException {
      if(tag == null) {
         tag = "";
      }

      Set<String> keyspaces = new HashSet();
      String[] var4 = DatabaseDescriptor.getAllDataFileLocations();
      int var5 = var4.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String dataDir = var4[var6];
         String[] var8 = (new File(dataDir)).list();
         int var9 = var8.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            String keyspaceDir = var8[var10];
            if(keyspaceNames.length <= 0 || Arrays.asList(keyspaceNames).contains(keyspaceDir)) {
               keyspaces.add(keyspaceDir);
            }
         }
      }

      Iterator var12 = keyspaces.iterator();

      while(var12.hasNext()) {
         String keyspace = (String)var12.next();
         Keyspace.clearSnapshot(tag, keyspace);
      }

      if(logger.isDebugEnabled()) {
         logger.debug("Cleared out snapshot directories");
      }

   }

   public Map<String, TabularData> getSnapshotDetails() {
      Map<String, TabularData> snapshotMap = new HashMap();
      Iterator var2 = Keyspace.all().iterator();

      while(var2.hasNext()) {
         Keyspace keyspace = (Keyspace)var2.next();
         Iterator var4 = keyspace.getColumnFamilyStores().iterator();

         while(var4.hasNext()) {
            ColumnFamilyStore cfStore = (ColumnFamilyStore)var4.next();

            Entry snapshotDetail;
            TabularDataSupport data;
            for(Iterator var6 = cfStore.getSnapshotDetails().entrySet().iterator(); var6.hasNext(); SnapshotDetailsTabularData.from((String)snapshotDetail.getKey(), keyspace.getName(), cfStore.getTableName(), snapshotDetail, data)) {
               snapshotDetail = (Entry)var6.next();
               data = (TabularDataSupport)snapshotMap.get(snapshotDetail.getKey());
               if(data == null) {
                  data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                  snapshotMap.put(snapshotDetail.getKey(), data);
               }
            }
         }
      }

      return snapshotMap;
   }

   public long trueSnapshotsSize() {
      long total = 0L;
      Iterator var3 = Keyspace.all().iterator();

      while(true) {
         Keyspace keyspace;
         do {
            if(!var3.hasNext()) {
               return total;
            }

            keyspace = (Keyspace)var3.next();
         } while(SchemaConstants.isLocalSystemKeyspace(keyspace.getName()));

         ColumnFamilyStore cfStore;
         for(Iterator var5 = keyspace.getColumnFamilyStores().iterator(); var5.hasNext(); total += cfStore.trueSnapshotsSize()) {
            cfStore = (ColumnFamilyStore)var5.next();
         }
      }
   }

   public void refreshSizeEstimates() throws ExecutionException {
      this.cleanupSizeEstimates();
      FBUtilities.waitOnFuture(ScheduledExecutors.optionalTasks.submit(SizeEstimatesRecorder.instance));
   }

   public void cleanupSizeEstimates() {
      SetMultimap<String, String> sizeEstimates = (SetMultimap)TPCUtils.blockingGet(SystemKeyspace.getTablesWithSizeEstimates());
      Iterator var2 = sizeEstimates.asMap().entrySet().iterator();

      while(true) {
         while(var2.hasNext()) {
            Entry<String, Collection<String>> tablesByKeyspace = (Entry)var2.next();
            String keyspace = (String)tablesByKeyspace.getKey();
            if(!Schema.instance.getKeyspaces().contains(keyspace)) {
               TPCUtils.blockingGet(SystemKeyspace.clearSizeEstimates(keyspace));
            } else {
               Iterator var5 = ((Collection)tablesByKeyspace.getValue()).iterator();

               while(var5.hasNext()) {
                  String table = (String)var5.next();
                  if(Schema.instance.getTableMetadataRef(keyspace, table) == null) {
                     TPCUtils.blockingGet(SystemKeyspace.clearSizeEstimates(keyspace, table));
                  }
               }
            }
         }

         return;
      }
   }

   public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames) throws IOException {
      Keyspace keyspace = this.getValidKeyspace(keyspaceName);
      return keyspace.getValidColumnFamilies(allowIndexes, autoAddIndexes, cfNames);
   }

   public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException {
      Iterator var3 = this.getValidColumnFamilies(true, false, keyspaceName, tableNames).iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var3.next();
         logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
         cfStore.forceBlockingFlush();
      }

   }

   public int repairAsync(String keyspace, Map<String, String> repairSpec) {
      RepairOption option = RepairOption.parse(repairSpec, this.tokenMetadata.partitioner);
      if(option.getRanges().isEmpty()) {
         if(option.isPrimaryRange()) {
            if(option.getDataCenters().isEmpty() && option.getHosts().isEmpty()) {
               option.getRanges().addAll(this.getPrimaryRanges(keyspace));
            } else {
               if(!option.isInLocalDCOnly()) {
                  throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
               }

               option.getRanges().addAll(this.getPrimaryRangesWithinDC(keyspace));
            }
         } else {
            option.getRanges().addAll(this.getLocalRanges(keyspace));
         }
      }

      if(!option.getRanges().isEmpty() && Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor() >= 2 && this.tokenMetadata.getAllEndpoints().size() >= 2) {
         if(option.isIncremental()) {
            this.failIfCannotRunIncrementalRepair(keyspace, (String[])option.getColumnFamilies().toArray(new String[0]));
         }

         int cmd = nextRepairCommand.incrementAndGet();
         ActiveRepairService.repairCommandExecutor.execute(this.createRepairTask(cmd, keyspace, option));
         return cmd;
      } else {
         return 0;
      }
   }

   protected void failIfCannotRunIncrementalRepair(String keyspace, String[] tables) {
      try {
         Set<ColumnFamilyStore> tablesToRepair = Sets.newHashSet(this.getValidColumnFamilies(false, false, keyspace, tables));
         Set<String> tablesWithViewsOrCdc = (Set)tablesToRepair.stream().filter((c) -> {
            return c.hasViews() || c.metadata.get().isView() || c.isCdcEnabled();
         }).map((c) -> {
            return c.name;
         }).collect(Collectors.toSet());
         if(!tablesWithViewsOrCdc.isEmpty()) {
            throw new IllegalArgumentException(String.format("Cannot run incremental repair on tables %s from keyspace %s because incremental repair is not supported on tables with materialized views or CDC-enabled. Please run full repair on these tables.", new Object[]{tablesWithViewsOrCdc.toString(), keyspace}));
         }
      } catch (IOException var5) {
         throw new RuntimeException("Could not fetch tables for repair.", var5);
      }
   }

   @VisibleForTesting
   Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken) {
      Token parsedBeginToken = this.getTokenFactory().fromString(beginToken);
      Token parsedEndToken = this.getTokenFactory().fromString(endToken);
      ArrayList<Range<Token>> repairingRange = new ArrayList();
      ArrayList<Token> tokens = new ArrayList(this.tokenMetadata.sortedTokens());
      if(!tokens.contains(parsedBeginToken)) {
         tokens.add(parsedBeginToken);
      }

      if(!tokens.contains(parsedEndToken)) {
         tokens.add(parsedEndToken);
      }

      Collections.sort(tokens);
      int start = tokens.indexOf(parsedBeginToken);
      int end = tokens.indexOf(parsedEndToken);

      for(int i = start; i != end; i = (i + 1) % tokens.size()) {
         Range<Token> range = new Range((RingPosition)tokens.get(i), (RingPosition)tokens.get((i + 1) % tokens.size()));
         repairingRange.add(range);
      }

      return repairingRange;
   }

   public Token.TokenFactory getTokenFactory() {
      return this.tokenMetadata.partitioner.getTokenFactory();
   }

   private FutureTask<Object> createRepairTask(int cmd, String keyspace, RepairOption options) {
      if(!options.getDataCenters().isEmpty() && !options.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter())) {
         throw new IllegalArgumentException("the local data center must be part of the repair");
      } else {
         RepairRunnable task = new RepairRunnable(this, cmd, options, keyspace);
         task.addProgressListener(this.progressSupport);
         if(options.isTraced()) {
            Runnable r = () -> {
               try {
                  task.run();
               } finally {
                  ExecutorLocals.set((ExecutorLocals)null);
               }

            };
            return new FutureTask(r, (Object)null);
         } else {
            return new FutureTask(task, (Object)null);
         }
      }
   }

   public void forceTerminateAllRepairSessions() {
      ActiveRepairService.instance.terminateSessions();
   }

   @Nullable
   public List<String> getParentRepairStatus(int cmd) {
      Pair<ActiveRepairService.ParentRepairStatus, List<String>> pair = ActiveRepairService.instance.getRepairStatus(Integer.valueOf(cmd));
      return pair == null?null:ImmutableList.builder().add(((ActiveRepairService.ParentRepairStatus)pair.left).name()).addAll((Iterable)pair.right).build();
   }

   public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep) {
      AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
      Collection<Range<Token>> primaryRanges = new HashSet();
      TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
      Iterator var6 = metadata.sortedTokens().iterator();

      while(var6.hasNext()) {
         Token token = (Token)var6.next();
         List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
         if(endpoints.size() > 0 && ((InetAddress)endpoints.get(0)).equals(ep)) {
            primaryRanges.add(new Range(metadata.getPredecessor(token), token));
         }
      }

      return primaryRanges;
   }

   public Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddress referenceEndpoint) {
      TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
      String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
      Collection<InetAddress> localDcNodes = metadata.getTopology().getDatacenterEndpoints().get(localDC);
      AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
      Collection<Range<Token>> localDCPrimaryRanges = new HashSet();
      Iterator var8 = metadata.sortedTokens().iterator();

      while(true) {
         while(var8.hasNext()) {
            Token token = (Token)var8.next();
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            Iterator var11 = endpoints.iterator();

            while(var11.hasNext()) {
               InetAddress endpoint = (InetAddress)var11.next();
               if(localDcNodes.contains(endpoint)) {
                  if(endpoint.equals(referenceEndpoint)) {
                     localDCPrimaryRanges.add(new Range(metadata.getPredecessor(token), token));
                  }
                  break;
               }
            }
         }

         return localDCPrimaryRanges;
      }
   }

   Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddress ep) {
      return Keyspace.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
   }

   public List<Range<Token>> getAllRanges(List<Token> sortedTokens) {
      if(logger.isTraceEnabled()) {
         logger.trace("computing ranges for {}", StringUtils.join(sortedTokens, ", "));
      }

      if(sortedTokens.isEmpty()) {
         return Collections.emptyList();
      } else {
         int size = sortedTokens.size();
         List<Range<Token>> ranges = new ArrayList(size + 1);

         for(int i = 1; i < size; ++i) {
            Range<Token> range = new Range((RingPosition)sortedTokens.get(i - 1), (RingPosition)sortedTokens.get(i));
            ranges.add(range);
         }

         Range<Token> range = new Range((RingPosition)sortedTokens.get(size - 1), (RingPosition)sortedTokens.get(0));
         ranges.add(range);
         return ranges;
      }
   }

   public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key) {
      KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspaceName);
      if(ksMetaData == null) {
         throw new IllegalArgumentException("Unknown keyspace '" + keyspaceName + "'");
      } else {
         TableMetadata metadata = ksMetaData.getTableOrViewNullable(cf);
         if(metadata == null) {
            throw new IllegalArgumentException("Unknown table '" + cf + "' in keyspace '" + keyspaceName + "'");
         } else {
            return this.getNaturalEndpoints((String)keyspaceName, (RingPosition)this.tokenMetadata.partitioner.getToken(metadata.partitionKeyType.fromString(key)));
         }
      }
   }

   public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key) {
      return this.getNaturalEndpoints((String)keyspaceName, (RingPosition)this.tokenMetadata.partitioner.getToken(key));
   }

   public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition pos) {
      return this.getNaturalEndpoints(Keyspace.open(keyspaceName), pos);
   }

   public List<InetAddress> getNaturalEndpoints(Keyspace keyspace, RingPosition pos) {
      return keyspace.getReplicationStrategy().getNaturalEndpoints(pos);
   }

   public Iterable<InetAddress> getNaturalAndPendingEndpoints(String keyspaceName, Token token) {
      return Iterables.concat(this.getNaturalEndpoints((String)keyspaceName, (RingPosition)token), this.tokenMetadata.pendingEndpointsFor(token, keyspaceName));
   }

   public static void addLiveNaturalEndpointsToList(Keyspace keyspace, RingPosition pos, ArrayList<InetAddress> liveEps) {
      List<InetAddress> endpoints = keyspace.getReplicationStrategy().getCachedNaturalEndpoints(pos);
      int i = 0;

      for(int size = endpoints.size(); i < size; ++i) {
         InetAddress endpoint = (InetAddress)endpoints.get(i);
         if(FailureDetector.instance.isAlive(endpoint)) {
            liveEps.add(endpoint);
         }
      }

   }

   public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception {
      ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(classQualifier);
      if(StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel)) {
         JMXConfiguratorMBean jmxConfiguratorMBean = (JMXConfiguratorMBean)JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"), JMXConfiguratorMBean.class);
         jmxConfiguratorMBean.reloadDefaultConfiguration();
      } else if(StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel)) {
         if(logBackLogger.getLevel() != null || this.hasAppenders(logBackLogger)) {
            logBackLogger.setLevel((Level)null);
         }

      } else {
         Level level = Level.toLevel(rawLevel);
         logBackLogger.setLevel(level);
         logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", new Object[]{level, classQualifier, rawLevel, rawLevel});
      }
   }

   public Map<String, String> getLoggingLevels() {
      Map<String, String> logLevelMaps = Maps.newLinkedHashMap();
      LoggerContext lc = (LoggerContext)LoggerFactory.getILoggerFactory();
      Iterator var3 = lc.getLoggerList().iterator();

      while(true) {
         ch.qos.logback.classic.Logger logger;
         do {
            if(!var3.hasNext()) {
               return logLevelMaps;
            }

            logger = (ch.qos.logback.classic.Logger)var3.next();
         } while(logger.getLevel() == null && !this.hasAppenders(logger));

         logLevelMaps.put(logger.getName(), logger.getLevel().toString());
      }
   }

   private boolean hasAppenders(ch.qos.logback.classic.Logger logger) {
      Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
      return it.hasNext();
   }

   public List<Pair<Range<Token>, Long>> getSplits(String keyspaceName, String cfName, Range<Token> range, int keysPerSplit) {
      Keyspace t = Keyspace.open(keyspaceName);
      ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
      List<DecoratedKey> keys = this.keySamples(Collections.singleton(cfs), range);
      long totalRowCountEstimate = cfs.estimatedKeysForRange(range);
      int minSamplesPerSplit = 4;
      int maxSplitCount = keys.size() / minSamplesPerSplit + 1;
      int splitCount = Math.max(1, Math.min(maxSplitCount, (int)(totalRowCountEstimate / (long)keysPerSplit)));
      List<Token> tokens = this.keysToTokens(range, keys);
      return this.getSplits(tokens, splitCount, cfs);
   }

   private List<Pair<Range<Token>, Long>> getSplits(List<Token> tokens, int splitCount, ColumnFamilyStore cfs) {
      double step = (double)(tokens.size() - 1) / (double)splitCount;
      Token prevToken = (Token)tokens.get(0);
      List<Pair<Range<Token>, Long>> splits = Lists.newArrayListWithExpectedSize(splitCount);

      for(int i = 1; i <= splitCount; ++i) {
         int index = (int)Math.round((double)i * step);
         Token token = (Token)tokens.get(index);
         Range<Token> range = new Range(prevToken, token);
         splits.add(Pair.create(range, Long.valueOf(Math.max((long)cfs.metadata().params.minIndexInterval, cfs.estimatedKeysForRange(range)))));
         prevToken = token;
      }

      return splits;
   }

   private List<Token> keysToTokens(Range<Token> range, List<DecoratedKey> keys) {
      List<Token> tokens = Lists.newArrayListWithExpectedSize(keys.size() + 2);
      tokens.add(range.left);
      Iterator var4 = keys.iterator();

      while(var4.hasNext()) {
         DecoratedKey key = (DecoratedKey)var4.next();
         tokens.add(key.getToken());
      }

      tokens.add(range.right);
      return tokens;
   }

   private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range) {
      List<DecoratedKey> keys = new ArrayList();
      Iterator var4 = cfses.iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
         Iterables.addAll(keys, cfs.keySamples(range));
      }

      FBUtilities.sortSampledKeys(keys, range);
      return keys;
   }

   private void startLeaving() {
      Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.leaving(this.getLocalTokensBlocking()));
      this.tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
      PendingRangeCalculatorService.instance.update();
   }

   public void decommission(boolean force) throws InterruptedException {
      TokenMetadata metadata = this.tokenMetadata.cloneAfterAllLeft();
      if(this.operationMode != StorageService.Mode.LEAVING) {
         if(!this.tokenMetadata.isMember(FBUtilities.getBroadcastAddress())) {
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
         }

         if(metadata.getAllEndpoints().size() < 2) {
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
         }

         if(this.operationMode != StorageService.Mode.NORMAL) {
            throw new UnsupportedOperationException("Node in " + this.operationMode + " state; wait for status to become normal or restart");
         }
      }

      if(!this.isDecommissioning.compareAndSet(false, true)) {
         throw new IllegalStateException("Node is still decommissioning. Check nodetool netstats.");
      } else {
         if(logger.isDebugEnabled()) {
            logger.debug("DECOMMISSIONING");
         }

         try {
            CompletableFuture<Boolean> nodeSyncStopFuture = this.nodeSyncService.disableAsync(false);
            waitForNodeSyncShutdown(nodeSyncStopFuture);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            String dc = DatabaseDescriptor.getLocalDataCenter();
            if(this.operationMode != StorageService.Mode.LEAVING) {
               Iterator var7 = Schema.instance.getPartitionedKeyspaces().iterator();

               while(var7.hasNext()) {
                  String keyspaceName = (String)var7.next();
                  if(!force) {
                     Keyspace keyspace = Keyspace.open(keyspaceName);
                     int rf;
                     int numNodes;
                     if(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
                        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
                        rf = strategy.getReplicationFactor(dc);
                        numNodes = metadata.getTopology().getDatacenterEndpoints().get(dc).size();
                     } else {
                        numNodes = metadata.getAllEndpoints().size();
                        rf = keyspace.getReplicationStrategy().getReplicationFactor();
                     }

                     if(numNodes <= rf) {
                        throw new UnsupportedOperationException("Not enough live nodes to maintain replication factor in keyspace " + keyspaceName + " (RF = " + rf + ", N = " + numNodes + "). Perform a forceful decommission to ignore.");
                     }
                  }

                  if(this.tokenMetadata.getPendingRanges(keyspaceName, FBUtilities.getBroadcastAddress()).size() > 0) {
                     throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
                  }
               }
            }

            this.startLeaving();
            long var10000 = (long)RING_DELAY;
            BatchlogManager var10001 = BatchlogManager.instance;
            long timeout = Math.max(var10000, BatchlogManager.getBatchlogTimeout());
            this.setMode(StorageService.Mode.LEAVING, "sleeping " + timeout + " ms for batch processing and pending range setup", true);
            Thread.sleep(timeout);
            Runnable finishLeaving = new Runnable() {
               public void run() {
                  StorageService.this.shutdownClientServers();
                  Gossiper.instance.stop();

                  try {
                     MessagingService.instance().shutdown();
                  } catch (IOError var2) {
                     StorageService.logger.info("failed to shutdown message service: {}", var2);
                  }

                  StageManager.shutdownNow();
                  StorageService.this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.DECOMMISSIONED);
                  StorageService.this.setMode(StorageService.Mode.DECOMMISSIONED, true);
               }
            };
            this.unbootstrap(finishLeaving);
         } catch (InterruptedException var15) {
            throw new RuntimeException("Node interrupted while decommissioning");
         } catch (ExecutionException var16) {
            logger.error("Error while decommissioning node ", var16.getCause());
            throw new RuntimeException("Error while decommissioning node: " + var16.getCause().getMessage());
         } finally {
            this.isDecommissioning.set(false);
         }

      }
   }

   private static void waitForNodeSyncShutdown(CompletableFuture<Boolean> nodeSyncStopFuture) throws InterruptedException {
      try {
         nodeSyncStopFuture.get(2L, TimeUnit.MINUTES);
      } catch (TimeoutException var2) {
         logger.warn("Wasn't able to stop NodeSync service within 2 minutes during drain. While this generally shouldn't happen (and should be reported if it happens constantly), it should be harmless.");
      } catch (ExecutionException var3) {
         logger.warn("Unexpected error stopping the NodeSync service. This shouldn't happen (and please report) but should be harmless.", var3.getCause());
      }

   }

   private static final Interceptor newGossiperInitGuard() {
      String interceptorName = "Gossiper initialization-guarding interceptor";
      return new AbstractInterceptor("Gossiper initialization-guarding interceptor", ImmutableSet.of(Verbs.SCHEMA.PUSH), Message.Type.all(), ImmutableSet.of(MessageDirection.RECEIVING), ImmutableSet.of(Message.Locality.REMOTE)) {
         protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context) {
            StorageService.logger.debug("Message {} intercepted and dropped by {}", message, "Gossiper initialization-guarding interceptor");
            context.drop(message);
         }
      };
   }

   private void leaveRing() {
      this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP);
      this.tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddress());
      PendingRangeCalculatorService.instance.update();
      Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.left(this.getLocalTokensBlocking(), Gossiper.computeExpireTime()));
      int delay = Math.max(RING_DELAY, 2000);
      logger.info("Announcing that I have left the ring for {}ms", Integer.valueOf(delay));
      Uninterruptibles.sleepUninterruptibly((long)delay, TimeUnit.MILLISECONDS);
   }

   private void unbootstrap(Runnable onFinish) throws ExecutionException, InterruptedException {
      Map<String, Multimap<Range<Token>, InetAddress>> rangesToStream = new HashMap();

      String keyspaceName;
      Multimap rangesMM;
      for(Iterator var3 = Schema.instance.getNonLocalStrategyKeyspaces().iterator(); var3.hasNext(); rangesToStream.put(keyspaceName, rangesMM)) {
         keyspaceName = (String)var3.next();
         rangesMM = this.getChangedRangesForLeaving(keyspaceName, FBUtilities.getBroadcastAddress());
         if(logger.isDebugEnabled()) {
            logger.debug("Ranges needing transfer are [{}]", StringUtils.join(rangesMM.keySet(), ","));
         }
      }

      this.setMode(StorageService.Mode.LEAVING, "replaying batch log and streaming data to other nodes", true);
      Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
      Future<StreamState> streamSuccess = this.streamRangesBlocking(rangesToStream);
      logger.debug("waiting for batch log processing.");
      batchlogReplay.get();
      this.setMode(StorageService.Mode.LEAVING, "streaming hints to other nodes", true);
      Future hintsSuccess = this.streamHints();
      logger.debug("waiting for stream acks.");
      streamSuccess.get();
      hintsSuccess.get();
      logger.debug("stream acks all received.");
      this.leaveRing();
      onFinish.run();
   }

   private Future streamHints() {
      return HintsService.instance.transferHints(this::getPreferredHintsStreamTarget);
   }

   private UUID getPreferredHintsStreamTarget() {
      List<InetAddress> candidates = new ArrayList(instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints());
      candidates.remove(FBUtilities.getBroadcastAddress());
      Iterator iter = candidates.iterator();

      while(iter.hasNext()) {
         InetAddress address = (InetAddress)iter.next();
         if(!FailureDetector.instance.isAlive(address)) {
            iter.remove();
         }
      }

      if(candidates.isEmpty()) {
         logger.warn("Unable to stream hints since no live endpoints seen");
         throw new RuntimeException("Unable to stream hints since no live endpoints seen");
      } else {
         DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), candidates);
         InetAddress hintsDestinationHost = (InetAddress)candidates.get(0);
         return this.tokenMetadata.getHostId(hintsDestinationHost);
      }
   }

   public void move(String newToken) throws IOException {
      try {
         this.getTokenFactory().validate(newToken);
      } catch (ConfigurationException var3) {
         throw new IOException(var3.getMessage());
      }

      this.move(this.getTokenFactory().fromString(newToken));
   }

   private void move(Token newToken) throws IOException {
      if(newToken == null) {
         throw new IOException("Can't move to the undefined (null) token.");
      } else if(this.tokenMetadata.sortedTokens().contains(newToken)) {
         throw new IOException("target token " + newToken + " is already owned by another node.");
      } else {
         InetAddress localAddress = FBUtilities.getBroadcastAddress();
         if(this.getTokenMetadata().getTokens(localAddress).size() > 1) {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
         } else {
            List<String> keyspacesToProcess = Schema.instance.getNonLocalStrategyKeyspaces();
            PendingRangeCalculatorService.instance.blockUntilFinished();
            Iterator var4 = keyspacesToProcess.iterator();

            String keyspaceName;
            do {
               if(!var4.hasNext()) {
                  Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.moving(newToken));
                  this.setMode(StorageService.Mode.MOVING, String.format("Moving %s from %s to %s.", new Object[]{localAddress, this.getLocalTokensBlocking().iterator().next(), newToken}), true);
                  this.setMode(StorageService.Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", new Object[]{Integer.valueOf(RING_DELAY)}), true);
                  Uninterruptibles.sleepUninterruptibly((long)RING_DELAY, TimeUnit.MILLISECONDS);
                  StorageService.RangeRelocator relocator = new StorageService.RangeRelocator(Collections.singleton(newToken), keyspacesToProcess, null);
                  if(relocator.streamsNeeded()) {
                     this.setMode(StorageService.Mode.MOVING, "fetching new ranges and streaming old ranges", true);

                     try {
                        relocator.stream().get();
                     } catch (InterruptedException | ExecutionException var6) {
                        throw new RuntimeException("Interrupted while waiting for stream/fetch ranges to finish: " + var6.getMessage());
                     }
                  } else {
                     this.setMode(StorageService.Mode.MOVING, "No ranges to fetch/stream", true);
                  }

                  this.setTokens(Collections.singleton(newToken));
                  if(logger.isDebugEnabled()) {
                     logger.debug("Successfully moved to new token {}", this.getLocalTokensBlocking().iterator().next());
                  }

                  return;
               }

               keyspaceName = (String)var4.next();
            } while(this.tokenMetadata.getPendingRanges(keyspaceName, localAddress).size() <= 0);

            throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
         }
      }
   }

   public String getRemovalStatus() {
      return this.removingNode == null?"No token removals in process.":String.format("Removing token (%s). Waiting for replication confirmation from [%s].", new Object[]{this.tokenMetadata.getToken(this.removingNode), StringUtils.join(this.replicatingNodes, ",")});
   }

   public void forceRemoveCompletion() {
      if(this.replicatingNodes.isEmpty() && this.tokenMetadata.getSizeOfLeavingEndpoints() <= 0) {
         logger.warn("No nodes to force removal on, call 'removenode' first");
      } else {
         logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));
         Iterator var1 = this.tokenMetadata.getLeavingEndpoints().iterator();

         while(var1.hasNext()) {
            InetAddress endpoint = (InetAddress)var1.next();
            UUID hostId = this.tokenMetadata.getHostId(endpoint);
            Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
            this.excise(this.tokenMetadata.getTokens(endpoint), endpoint);
         }

         this.replicatingNodes.clear();
         this.removingNode = null;
      }

   }

   public void removeNode(String hostIdString) {
      InetAddress myAddress = FBUtilities.getBroadcastAddress();
      UUID localHostId = this.tokenMetadata.getHostId(myAddress);
      UUID hostId = UUID.fromString(hostIdString);
      InetAddress endpoint = this.tokenMetadata.getEndpointForHostId(hostId);
      if(endpoint == null) {
         throw new UnsupportedOperationException("Host ID not found.");
      } else if(!this.tokenMetadata.isMember(endpoint)) {
         throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");
      } else if(endpoint.equals(myAddress)) {
         throw new UnsupportedOperationException("Cannot remove self");
      } else if(Gossiper.instance.getLiveMembers().contains(endpoint)) {
         throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");
      } else {
         if(this.tokenMetadata.isLeaving(endpoint)) {
            logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);
         }

         if(!this.replicatingNodes.isEmpty()) {
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");
         } else {
            Collection<Token> tokens = this.tokenMetadata.getTokens(endpoint);
            Iterator var7 = Schema.instance.getNonLocalStrategyKeyspaces().iterator();

            while(true) {
               String keyspaceName;
               do {
                  if(!var7.hasNext()) {
                     this.removingNode = endpoint;
                     this.tokenMetadata.addLeavingEndpoint(endpoint);
                     PendingRangeCalculatorService.instance.update();
                     Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);
                     this.restoreReplicaCount(endpoint, myAddress);

                     while(!this.replicatingNodes.isEmpty()) {
                        Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
                     }

                     this.excise(tokens, endpoint);
                     Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                     this.replicatingNodes.clear();
                     this.removingNode = null;
                     return;
                  }

                  keyspaceName = (String)var7.next();
               } while(Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor() == 1);

               Multimap<Range<Token>, InetAddress> changedRanges = this.getChangedRangesForLeaving(keyspaceName, endpoint);
               IFailureDetector failureDetector = FailureDetector.instance;
               Iterator var11 = changedRanges.values().iterator();

               while(var11.hasNext()) {
                  InetAddress ep = (InetAddress)var11.next();
                  if(failureDetector.isAlive(ep)) {
                     this.replicatingNodes.add(ep);
                  } else {
                     logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
                  }
               }
            }
         }
      }
   }

   public void confirmReplication(InetAddress node) {
      if(!this.replicatingNodes.isEmpty()) {
         this.replicatingNodes.remove(node);
      } else {
         logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
      }

   }

   public String getOperationMode() {
      return this.operationMode.toString();
   }

   public boolean isStarting() {
      return this.operationMode == StorageService.Mode.STARTING;
   }

   public boolean isMoving() {
      return this.operationMode == StorageService.Mode.MOVING;
   }

   public boolean isJoining() {
      return this.operationMode == StorageService.Mode.JOINING;
   }

   public boolean isDrained() {
      return this.operationMode == StorageService.Mode.DRAINED;
   }

   public boolean isDraining() {
      return this.operationMode == StorageService.Mode.DRAINING;
   }

   public String getDrainProgress() {
      return String.format("Drained %s/%s ColumnFamilies", new Object[]{Integer.valueOf(this.remainingCFs), Integer.valueOf(this.totalCFs)});
   }

   public synchronized void drain() throws IOException, InterruptedException, ExecutionException {
      this.drain(false);
   }

   protected synchronized void drain(boolean isFinalShutdown) throws IOException, InterruptedException, ExecutionException {
      if(this.isShutdown) {
         if(!isFinalShutdown) {
            logger.warn("Cannot drain node (did it already happen?)");
         }

      } else {
         this.isShutdown = true;
         Throwable preShutdownHookThrowable = Throwables.perform((Throwable)null, (Stream)this.preShutdownHooks.stream().map((h) -> {
            return h::run;
         }));
         if(preShutdownHookThrowable != null) {
            logger.error("Attempting to continue draining after pre-shutdown hooks returned exception", preShutdownHookThrowable);
         }

         boolean var14 = false;

         Throwable postShutdownHookThrowable;
         label276: {
            try {
               var14 = true;
               this.setMode(StorageService.Mode.DRAINING, "starting drain process", !isFinalShutdown);
               CompletableFuture<Boolean> nodeSyncStopFuture = this.nodeSyncService.disableAsync(false);
               waitForNodeSyncShutdown(nodeSyncStopFuture);
               BatchlogManager.instance.shutdown();
               HintsService.instance.pauseDispatch();
               if(this.daemon != null) {
                  this.shutdownClientServers();
               }

               ScheduledExecutors.optionalTasks.shutdown();
               Gossiper.instance.stop();
               if(!isFinalShutdown) {
                  this.setMode(StorageService.Mode.DRAINING, "shutting down MessageService", false);
               }

               MessagingService.instance().shutdown();
               if(!isFinalShutdown) {
                  this.setMode(StorageService.Mode.DRAINING, "flushing column families", false);
               }

               Iterator var4 = Keyspace.all().iterator();

               Keyspace keyspace;
               while(var4.hasNext()) {
                  keyspace = (Keyspace)var4.next();
                  Iterator var6 = keyspace.getColumnFamilyStores().iterator();

                  while(var6.hasNext()) {
                     ColumnFamilyStore cfs = (ColumnFamilyStore)var6.next();
                     cfs.disableAutoCompaction();
                  }
               }

               this.totalCFs = 0;

               for(var4 = Keyspace.nonSystem().iterator(); var4.hasNext(); this.totalCFs += keyspace.getColumnFamilyStores().size()) {
                  keyspace = (Keyspace)var4.next();
               }

               this.remainingCFs = this.totalCFs;

               try {
                  CompletableFutures.allOf(Streams.of(Keyspace.nonSystem()).flatMap((keyspace) -> {
                     return keyspace.getColumnFamilyStores().stream();
                  }).map((cfs) -> {
                     return cfs.forceFlush().whenComplete((cl, exc) -> {
                        --this.remainingCFs;
                     });
                  })).get(1L, TimeUnit.MINUTES);
               } catch (Throwable var16) {
                  JVMStabilityInspector.inspectThrowable(var16);
                  logger.error("Caught exception while waiting for memtable flushes during shutdown hook", var16);
               }

               CompactionManager.instance.forceShutdown();
               if(SSTableReader.readHotnessTrackerExecutor != null) {
                  SSTableReader.readHotnessTrackerExecutor.shutdown();
                  if(!SSTableReader.readHotnessTrackerExecutor.awaitTermination(1L, TimeUnit.MINUTES)) {
                     logger.warn("Wasn't able to stop the SSTable read hotness tracker with 1 minute.");
                  }
               }

               LifecycleTransaction.waitForDeletions();

               try {
                  CompletableFutures.allOf(Streams.of(Keyspace.system()).flatMap((keyspace) -> {
                     return keyspace.getColumnFamilyStores().stream();
                  }).map(ColumnFamilyStore::forceFlush)).get(1L, TimeUnit.MINUTES);
               } catch (Throwable var15) {
                  JVMStabilityInspector.inspectThrowable(var15);
                  logger.error("Caught exception while waiting for memtable flushes during shutdown hook", var15);
               }

               if(!isFinalShutdown) {
                  this.setMode(StorageService.Mode.DRAINING, "stopping mutations", false);
               }

               List<OpOrder.Barrier> barriers = (List)StreamSupport.stream(Keyspace.all().spliterator(), false).map((ks) -> {
                  return ks.stopMutations();
               }).collect(Collectors.toList());
               barriers.forEach(OpOrder.Barrier::await);
               if(!isFinalShutdown) {
                  this.setMode(StorageService.Mode.DRAINING, "clearing background IO stage", false);
               }

               StorageProxy.instance.waitForHintsInProgress(3600, TimeUnit.SECONDS);
               HintsService.instance.shutdownBlocking();
               CommitLog.instance.forceRecycleAllSegments();
               CommitLog.instance.shutdownBlocking();
               ScheduledExecutors.nonPeriodicTasks.shutdown();
               if(!ScheduledExecutors.nonPeriodicTasks.awaitTermination(1L, TimeUnit.MINUTES)) {
                  logger.warn("Failed to wait for non periodic tasks to shutdown");
               }

               ColumnFamilyStore.shutdownPostFlushExecutor();
               ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).awaitTermination(1L, TimeUnit.MINUTES);
               this.setMode(StorageService.Mode.DRAINED, !isFinalShutdown);
               var14 = false;
               break label276;
            } catch (Throwable var17) {
               logger.error("Caught an exception while draining ", var17);
               var14 = false;
            } finally {
               if(var14) {
                  Throwable postShutdownHookThrowable = Throwables.perform((Throwable)null, (Stream)this.postShutdownHooks.stream().map((h) -> {
                     return h::run;
                  }));
                  if(postShutdownHookThrowable != null) {
                     logger.error("Post-shutdown hooks returned exception", postShutdownHookThrowable);
                  }

               }
            }

            postShutdownHookThrowable = Throwables.perform((Throwable)null, (Stream)this.postShutdownHooks.stream().map((h) -> {
               return h::run;
            }));
            if(postShutdownHookThrowable != null) {
               logger.error("Post-shutdown hooks returned exception", postShutdownHookThrowable);
            }

            return;
         }

         postShutdownHookThrowable = Throwables.perform((Throwable)null, (Stream)this.postShutdownHooks.stream().map((h) -> {
            return h::run;
         }));
         if(postShutdownHookThrowable != null) {
            logger.error("Post-shutdown hooks returned exception", postShutdownHookThrowable);
         }

      }
   }

   public synchronized boolean addPreShutdownHook(Runnable hook) {
      return !this.isDraining() && !this.isDrained()?this.preShutdownHooks.add(hook):false;
   }

   public synchronized boolean removePreShutdownHook(Runnable hook) {
      return this.preShutdownHooks.remove(hook);
   }

   public synchronized boolean addPostShutdownHook(Runnable hook) {
      return !this.isDraining() && !this.isDrained()?this.postShutdownHooks.add(hook):false;
   }

   public synchronized boolean removePostShutdownHook(Runnable hook) {
      return this.postShutdownHooks.remove(hook);
   }

   synchronized void checkServiceAllowedToStart(String service) {
      if(this.isDraining()) {
         throw new IllegalStateException(String.format("Unable to start %s because the node is draining.", new Object[]{service}));
      } else if(this.isShutdown()) {
         throw new IllegalStateException(String.format("Unable to start %s because the node was drained.", new Object[]{service}));
      }
   }

   @VisibleForTesting
   public IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner) {
      IPartitioner oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(newPartitioner);
      this.tokenMetadata = this.tokenMetadata.cloneWithNewPartitioner(newPartitioner);
      this.valueFactory = new VersionedValue.VersionedValueFactory(newPartitioner);
      return oldPartitioner;
   }

   TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd) {
      TokenMetadata old = this.tokenMetadata;
      this.tokenMetadata = tmd;
      return old;
   }

   public void truncate(String keyspace, String table) throws TimeoutException, IOException {
      try {
         StorageProxy.truncateBlocking(keyspace, table);
      } catch (UnavailableException var4) {
         throw new IOException(var4.getMessage());
      }
   }

   public Map<InetAddress, Float> getOwnership() {
      List<Token> sortedTokens = this.tokenMetadata.sortedTokens();
      Map<Token, Float> tokenMap = new TreeMap(this.tokenMetadata.partitioner.describeOwnership(sortedTokens));
      Map<InetAddress, Float> nodeMap = new LinkedHashMap();
      Iterator var4 = tokenMap.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<Token, Float> entry = (Entry)var4.next();
         InetAddress endpoint = this.tokenMetadata.getEndpoint((Token)entry.getKey());
         Float tokenOwnership = (Float)entry.getValue();
         if(nodeMap.containsKey(endpoint)) {
            nodeMap.put(endpoint, Float.valueOf(((Float)nodeMap.get(endpoint)).floatValue() + tokenOwnership.floatValue()));
         } else {
            nodeMap.put(endpoint, tokenOwnership);
         }
      }

      return nodeMap;
   }

   public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException {
      AbstractReplicationStrategy strategy;
      if(keyspace != null) {
         Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
         if(keyspaceInstance == null) {
            throw new IllegalArgumentException("The keyspace " + keyspace + ", does not exist");
         }

         if(keyspaceInstance.getReplicationStrategy() instanceof LocalStrategy) {
            throw new IllegalStateException("Ownership values for keyspaces with LocalStrategy are meaningless");
         }

         strategy = keyspaceInstance.getReplicationStrategy();
      } else {
         List<String> userKeyspaces = Schema.instance.getUserKeyspaces();
         if(userKeyspaces.size() > 0) {
            keyspace = (String)userKeyspaces.get(0);
            AbstractReplicationStrategy replicationStrategy = Schema.instance.getKeyspaceInstance(keyspace).getReplicationStrategy();
            Iterator var5 = userKeyspaces.iterator();

            while(var5.hasNext()) {
               String keyspaceName = (String)var5.next();
               if(!Schema.instance.getKeyspaceInstance(keyspaceName).getReplicationStrategy().hasSameSettings(replicationStrategy)) {
                  throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
               }
            }
         } else {
            keyspace = "system_traces";
         }

         Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
         if(keyspaceInstance == null) {
            throw new IllegalArgumentException("The node does not have " + keyspace + " yet, probably still bootstrapping");
         }

         strategy = keyspaceInstance.getReplicationStrategy();
      }

      TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
      Collection<Collection<InetAddress>> endpointsGroupedByDc = new ArrayList();
      SortedMap<String, Collection<InetAddress>> sortedDcsToEndpoints = new TreeMap();
      sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());
      Iterator var21 = sortedDcsToEndpoints.values().iterator();

      while(var21.hasNext()) {
         Collection<InetAddress> endpoints = (Collection)var21.next();
         endpointsGroupedByDc.add(endpoints);
      }

      Map<Token, Float> tokenOwnership = this.tokenMetadata.partitioner.describeOwnership(this.tokenMetadata.sortedTokens());
      LinkedHashMap<InetAddress, Float> finalOwnership = Maps.newLinkedHashMap();
      Multimap<InetAddress, Range<Token>> endpointToRanges = strategy.getAddressRanges();
      Iterator var9 = endpointsGroupedByDc.iterator();

      while(var9.hasNext()) {
         Collection<InetAddress> endpoints = (Collection)var9.next();
         Iterator var11 = endpoints.iterator();

         while(var11.hasNext()) {
            InetAddress endpoint = (InetAddress)var11.next();
            float ownership = 0.0F;
            Iterator var14 = endpointToRanges.get(endpoint).iterator();

            while(var14.hasNext()) {
               Range<Token> range = (Range)var14.next();
               if(tokenOwnership.containsKey(range.right)) {
                  ownership += ((Float)tokenOwnership.get(range.right)).floatValue();
               }
            }

            finalOwnership.put(endpoint, Float.valueOf(ownership));
         }
      }

      return finalOwnership;
   }

   public List<String> getKeyspaces() {
      List<String> keyspaceNamesList = new ArrayList(Schema.instance.getKeyspaces());
      return Collections.unmodifiableList(keyspaceNamesList);
   }

   public List<String> getNonSystemKeyspaces() {
      List<String> nonKeyspaceNamesList = new ArrayList(Schema.instance.getNonSystemKeyspaces());
      return Collections.unmodifiableList(nonKeyspaceNamesList);
   }

   public List<String> getNonLocalStrategyKeyspaces() {
      return Collections.unmodifiableList(Schema.instance.getNonLocalStrategyKeyspaces());
   }

   public Map<String, Map<String, String>> getTableInfos(String keyspace, String... tables) {
      HashMap tableInfos = new HashMap();

      try {
         this.getValidColumnFamilies(false, false, keyspace, tables).forEach((cfs) -> {
            Map var10000 = (Map)tableInfos.put(cfs.name, cfs.getTableInfo().asMap());
         });
         return tableInfos;
      } catch (IOException var5) {
         throw new RuntimeException(String.format("Could not retrieve info for keyspace %s and table(s) %s.", new Object[]{keyspace, tables}), var5);
      }
   }

   public Map<String, List<String>> getKeyspacesAndViews() {
      Map<String, List<String>> map = new HashMap();
      Iterator var2 = Schema.instance.getKeyspaces().iterator();

      while(var2.hasNext()) {
         String ks = (String)var2.next();
         List<String> tables = new ArrayList();
         map.put(ks, tables);
         Iterator viewIter = Schema.instance.getKeyspaceMetadata(ks).views.iterator();

         while(viewIter.hasNext()) {
            tables.add(((ViewMetadata)viewIter.next()).name);
         }
      }

      return map;
   }

   public Map<String, String> getViewBuildStatuses(String keyspace, String view) {
      Map<UUID, String> coreViewStatus = (Map)TPCUtils.blockingGet(SystemDistributedKeyspace.viewStatus(keyspace, view));
      Map<InetAddress, UUID> hostIdToEndpoint = this.tokenMetadata.getEndpointToHostIdMapForReading();
      Map<String, String> result = new HashMap();
      Iterator var6 = hostIdToEndpoint.entrySet().iterator();

      while(var6.hasNext()) {
         Entry<InetAddress, UUID> entry = (Entry)var6.next();
         UUID hostId = (UUID)entry.getValue();
         InetAddress endpoint = (InetAddress)entry.getKey();
         result.put(endpoint.toString(), coreViewStatus.containsKey(hostId)?(String)coreViewStatus.get(hostId):"UNKNOWN");
      }

      return Collections.unmodifiableMap(result);
   }

   public void setDynamicUpdateInterval(int dynamicUpdateInterval) {
      if(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch) {
         try {
            this.updateSnitch((String)null, Boolean.valueOf(true), Integer.valueOf(dynamicUpdateInterval), (Integer)null, (Double)null);
         } catch (ClassNotFoundException var3) {
            throw new RuntimeException(var3);
         }
      }

   }

   public int getDynamicUpdateInterval() {
      return DatabaseDescriptor.getDynamicUpdateInterval();
   }

   public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException {
      if(dynamicUpdateInterval != null) {
         DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval.intValue());
      }

      if(dynamicResetInterval != null) {
         DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval.intValue());
      }

      if(dynamicBadnessThreshold != null) {
         DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold.doubleValue());
      }

      IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
      if(epSnitchClassName != null) {
         if(oldSnitch instanceof DynamicEndpointSnitch) {
            ((DynamicEndpointSnitch)oldSnitch).close();
         }

         IEndpointSnitch newSnitch;
         try {
            newSnitch = DatabaseDescriptor.createEndpointSnitch(dynamic != null && dynamic.booleanValue(), epSnitchClassName);
         } catch (ConfigurationException var10) {
            throw new ClassNotFoundException(var10.getMessage());
         }

         if(newSnitch instanceof DynamicEndpointSnitch) {
            logger.info("Created new dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}", new Object[]{((DynamicEndpointSnitch)newSnitch).subsnitch.getClass().getName(), Integer.valueOf(DatabaseDescriptor.getDynamicUpdateInterval()), Integer.valueOf(DatabaseDescriptor.getDynamicResetInterval()), Double.valueOf(DatabaseDescriptor.getDynamicBadnessThreshold())});
         } else {
            logger.info("Created new non-dynamic snitch {}", newSnitch.getClass().getName());
         }

         DatabaseDescriptor.setEndpointSnitch(newSnitch);

         String ks;
         for(Iterator var8 = Schema.instance.getKeyspaces().iterator(); var8.hasNext(); Keyspace.open(ks).getReplicationStrategy().snitch = newSnitch) {
            ks = (String)var8.next();
         }
      } else if(oldSnitch instanceof DynamicEndpointSnitch) {
         logger.info("Applying config change to dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}", new Object[]{((DynamicEndpointSnitch)oldSnitch).subsnitch.getClass().getName(), Integer.valueOf(DatabaseDescriptor.getDynamicUpdateInterval()), Integer.valueOf(DatabaseDescriptor.getDynamicResetInterval()), Double.valueOf(DatabaseDescriptor.getDynamicBadnessThreshold())});
         DynamicEndpointSnitch snitch = (DynamicEndpointSnitch)oldSnitch;
         snitch.applyConfigChanges();
      }

      this.updateTopology();
   }

   public String getBatchlogEndpointStrategy() {
      return DatabaseDescriptor.getBatchlogEndpointStrategy().name();
   }

   public void setBatchlogEndpointStrategy(String batchlogEndpointStrategy) {
      DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.valueOf(batchlogEndpointStrategy));
   }

   private Future<StreamState> streamRangesBlocking(Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByKeyspace) {
      Map<String, Map<InetAddress, List<Range<Token>>>> sessionsToStreamByKeyspace = new HashMap();
      Iterator var3 = rangesToStreamByKeyspace.entrySet().iterator();

      while(true) {
         String keyspace;
         Multimap rangesWithEndpoints;
         Map transferredRangePerKeyspace;
         InetAddress endpoint;
         do {
            if(!var3.hasNext()) {
               StreamPlan streamPlan = new StreamPlan(StreamOperation.DECOMMISSION, true, true);
               streamPlan.listeners(this.streamStateStore, new StreamEventHandler[0]);
               Iterator var16 = sessionsToStreamByKeyspace.entrySet().iterator();

               while(var16.hasNext()) {
                  Entry<String, Map<InetAddress, List<Range<Token>>>> entry = (Entry)var16.next();
                  String keyspaceName = (String)entry.getKey();
                  transferredRangePerKeyspace = (Map)entry.getValue();
                  Iterator var19 = transferredRangePerKeyspace.entrySet().iterator();

                  while(var19.hasNext()) {
                     Entry<InetAddress, List<Range<Token>>> rangesEntry = (Entry)var19.next();
                     List<Range<Token>> ranges = (List)rangesEntry.getValue();
                     InetAddress newEndpoint = (InetAddress)rangesEntry.getKey();
                     endpoint = SystemKeyspace.getPreferredIP(newEndpoint);
                     streamPlan.transferRanges(newEndpoint, (InetAddress)endpoint, (String)keyspaceName, (Collection)ranges);
                  }
               }

               return streamPlan.execute();
            }

            Entry<String, Multimap<Range<Token>, InetAddress>> entry = (Entry)var3.next();
            keyspace = (String)entry.getKey();
            rangesWithEndpoints = (Multimap)entry.getValue();
         } while(rangesWithEndpoints.isEmpty());

         transferredRangePerKeyspace = (Map)TPCUtils.blockingGet(SystemKeyspace.getTransferredRanges("Unbootstrap", keyspace, instance.getTokenMetadata().partitioner));
         Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = new HashMap();
         Iterator var9 = rangesWithEndpoints.entries().iterator();

         while(true) {
            while(var9.hasNext()) {
               Entry<Range<Token>, InetAddress> endPointEntry = (Entry)var9.next();
               Range<Token> range = (Range)endPointEntry.getKey();
               endpoint = (InetAddress)endPointEntry.getValue();
               Set<Range<Token>> transferredRanges = (Set)transferredRangePerKeyspace.get(endpoint);
               if(transferredRanges != null && transferredRanges.contains(range)) {
                  logger.debug("Skipping transferred range {} of keyspace {}, endpoint {}", new Object[]{range, keyspace, endpoint});
               } else {
                  List<Range<Token>> curRanges = (List)rangesPerEndpoint.get(endpoint);
                  if(curRanges == null) {
                     curRanges = new LinkedList();
                     rangesPerEndpoint.put(endpoint, curRanges);
                  }

                  ((List)curRanges).add(range);
               }
            }

            sessionsToStreamByKeyspace.put(keyspace, rangesPerEndpoint);
            break;
         }
      }
   }

   public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated) {
      Set<Range<Token>> toStream = new HashSet();
      Set<Range<Token>> toFetch = new HashSet();
      Iterator var5 = current.iterator();

      Range r2;
      boolean intersect;
      Iterator var8;
      Range r1;
      while(var5.hasNext()) {
         r2 = (Range)var5.next();
         intersect = false;
         var8 = updated.iterator();

         while(var8.hasNext()) {
            r1 = (Range)var8.next();
            if(r2.intersects(r1)) {
               toStream.addAll(r2.subtract(r1));
               intersect = true;
            }
         }

         if(!intersect) {
            toStream.add(r2);
         }
      }

      var5 = updated.iterator();

      while(var5.hasNext()) {
         r2 = (Range)var5.next();
         intersect = false;
         var8 = current.iterator();

         while(var8.hasNext()) {
            r1 = (Range)var8.next();
            if(r2.intersects(r1)) {
               toFetch.addAll(r2.subtract(r1));
               intersect = true;
            }
         }

         if(!intersect) {
            toFetch.add(r2);
         }
      }

      return Pair.create(toStream, toFetch);
   }

   public void bulkLoad(String directory) {
      try {
         this.bulkLoadInternal(directory).get();
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public String bulkLoadAsync(String directory) {
      return this.bulkLoadInternal(directory).planId.toString();
   }

   private StreamResultFuture bulkLoadInternal(String directory) {
      File dir = new File(directory);
      if(dir.exists() && dir.isDirectory()) {
         SSTableLoader.Client client = new SSTableLoader.Client() {
            private String keyspace;

            public void init(String keyspace) {
               this.keyspace = keyspace;

               try {
                  Iterator var2 = StorageService.instance.getRangeToAddressMap(keyspace).entrySet().iterator();

                  while(var2.hasNext()) {
                     Entry<Range<Token>, List<InetAddress>> entry = (Entry)var2.next();
                     Range<Token> range = (Range)entry.getKey();
                     Iterator var5 = ((List)entry.getValue()).iterator();

                     while(var5.hasNext()) {
                        InetAddress endpoint = (InetAddress)var5.next();
                        this.addRangeForEndpoint(range, endpoint);
                     }
                  }

               } catch (Exception var7) {
                  throw new RuntimeException(var7);
               }
            }

            public TableMetadataRef getTableMetadata(String tableName) {
               return Schema.instance.getTableMetadataRef(this.keyspace, tableName);
            }
         };
         return (new SSTableLoader(dir, client, new OutputHandler.LogOutput())).stream();
      } else {
         throw new IllegalArgumentException("Invalid directory " + directory);
      }
   }

   public void rescheduleFailedDeletions() {
      LifecycleTransaction.rescheduleFailedDeletions();
   }

   public void loadNewSSTables(String ksName, String cfName, boolean resetLevels) {
      if(!this.isInitialized()) {
         throw new RuntimeException("Not yet initialized, can't load new sstables");
      } else {
         ColumnFamilyStore.loadNewSSTables(ksName, cfName, resetLevels);
      }
   }

   public List<String> sampleKeyRange() {
      List<DecoratedKey> keys = new ArrayList();
      Iterator var2 = Keyspace.nonLocalStrategy().iterator();

      while(var2.hasNext()) {
         Keyspace keyspace = (Keyspace)var2.next();
         Iterator var4 = this.getPrimaryRangesForEndpoint(keyspace.getName(), FBUtilities.getBroadcastAddress()).iterator();

         while(var4.hasNext()) {
            Range<Token> range = (Range)var4.next();
            keys.addAll(this.keySamples(keyspace.getColumnFamilyStores(), range));
         }
      }

      List<String> sampledKeys = new ArrayList(keys.size());
      Iterator var7 = keys.iterator();

      while(var7.hasNext()) {
         DecoratedKey key = (DecoratedKey)var7.next();
         sampledKeys.add(key.getToken().toString());
      }

      return sampledKeys;
   }

   public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames) {
      String[] indices = (String[])((List)Arrays.asList(idxNames).stream().map((p) -> {
         return SecondaryIndexManager.isIndexColumnFamily(p)?SecondaryIndexManager.getIndexName(p):p;
      }).collect(Collectors.toList())).toArray(new String[0]);
      ColumnFamilyStore.rebuildSecondaryIndex(ksName, cfName, indices);
   }

   public void resetLocalSchema() throws IOException {
      MigrationManager.resetLocalSchema();
   }

   public void reloadLocalSchema() {
      Schema.instance.reloadSchemaAndAnnounceVersion();
   }

   public void setTraceProbability(double probability) {
      this.traceProbability = probability;
   }

   public double getTraceProbability() {
      return this.traceProbability;
   }

   public void disableAutoCompaction(String ks, String... tables) throws IOException {
      Iterator var3 = this.getValidColumnFamilies(true, true, ks, tables).iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
         cfs.disableAutoCompaction();
      }

   }

   public synchronized void enableAutoCompaction(String ks, String... tables) throws IOException {
      this.checkServiceAllowedToStart("auto compaction");
      Iterator var3 = this.getValidColumnFamilies(true, true, ks, tables).iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
         cfs.enableAutoCompaction();
      }

   }

   public Map<String, Boolean> getAutoCompactionStatus(String ks, String... tables) throws IOException {
      Map<String, Boolean> status = new HashMap();
      Iterator var4 = this.getValidColumnFamilies(true, true, ks, tables).iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
         status.put(cfs.getTableName(), Boolean.valueOf(cfs.isAutoCompactionDisabled()));
      }

      return status;
   }

   public String getClusterName() {
      return DatabaseDescriptor.getClusterName();
   }

   public String getPartitionerName() {
      return DatabaseDescriptor.getPartitionerName();
   }

   public int getTombstoneWarnThreshold() {
      return DatabaseDescriptor.getTombstoneWarnThreshold();
   }

   public void setTombstoneWarnThreshold(int threshold) {
      DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
   }

   public int getTombstoneFailureThreshold() {
      return DatabaseDescriptor.getTombstoneFailureThreshold();
   }

   public void setTombstoneFailureThreshold(int threshold) {
      DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
   }

   public int getBatchSizeFailureThreshold() {
      return DatabaseDescriptor.getBatchSizeFailThresholdInKB();
   }

   public void setBatchSizeFailureThreshold(int threshold) {
      DatabaseDescriptor.setBatchSizeFailThresholdInKB(threshold);
      logger.info("Updated batch_size_fail_threshold_in_kb to {}", Integer.valueOf(threshold));
   }

   public int getBatchSizeWarnThreshold() {
      return DatabaseDescriptor.getBatchSizeWarnThresholdInKB();
   }

   public void setBatchSizeWarnThreshold(int threshold) {
      DatabaseDescriptor.setBatchSizeWarnThresholdInKB(threshold);
      logger.info("Updated batch_size_warn_threshold_in_kb to {}", Integer.valueOf(threshold));
   }

   public void setHintedHandoffThrottleInKB(int throttleInKB) {
      DatabaseDescriptor.setHintedHandoffThrottleInKB(throttleInKB);
      logger.info("Updated hinted_handoff_throttle_in_kb to {}", Integer.valueOf(throttleInKB));
   }

   public long getPid() {
      return NativeLibrary.getProcessID();
   }

   public static List<Range<Token>> getStartupTokenRanges(Keyspace keyspace) {
      if(!DatabaseDescriptor.getPartitioner().splitter().isPresent()) {
         return null;
      } else {
         Object lr;
         if(instance.isBootstrapMode()) {
            lr = instance.getTokenMetadata().getPendingRanges(keyspace.getName(), FBUtilities.getBroadcastAddress());
         } else {
            TokenMetadata tmd = instance.getTokenMetadata().cloneAfterAllSettled();
            lr = keyspace.getReplicationStrategy().getAddressRanges(tmd).get(FBUtilities.getBroadcastAddress());
         }

         return lr != null && !((Collection)lr).isEmpty()?Range.sort((Collection)lr):null;
      }
   }

   public int forceMarkAllSSTablesAsUnrepaired(String keyspace, String... tables) throws IOException {
      int marked = 0;
      Iterator var4 = this.getValidColumnFamilies(false, false, keyspace, tables).iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();

         try {
            marked += cfs.forceMarkAllSSTablesAsUnrepaired();
         } catch (Throwable var7) {
            logger.error("Error while marking all SSTables from table {}.{} as unrepaired. Please trigger operation again or manually mark SSTables as unrepaired otherwise rows already purged on other replicas may be propagated to other replicas during incremental repair without their respectives tombstones.", new Object[]{keyspace, cfs.name, var7});
            throw new RuntimeException(var7);
         }
      }

      return marked;
   }

   public boolean shouldTraceRequest() {
      double traceProbability = this.getTraceProbability();
      return traceProbability != 0.0D && ThreadLocalRandom.current().nextDouble() < traceProbability;
   }

   private class RangeRelocator {
      private final StreamPlan streamPlan;

      private RangeRelocator(Collection<Token> var1, List<String> tokens) {
         this.streamPlan = new StreamPlan(StreamOperation.RELOCATION, true, true);
         this.calculateToFromStreams(tokens, keyspaceNames);
      }

      private void calculateToFromStreams(Collection<Token> newTokens, List<String> keyspaceNames) {
         InetAddress localAddress = FBUtilities.getBroadcastAddress();
         IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
         TokenMetadata tokenMetaCloneAllSettled = StorageService.this.tokenMetadata.cloneAfterAllSettled();
         TokenMetadata tokenMetaClone = StorageService.this.tokenMetadata.cloneOnlyTokenMap();
         Iterator var7 = keyspaceNames.iterator();

         while(var7.hasNext()) {
            String keyspace = (String)var7.next();
            AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
            Multimap<InetAddress, Range<Token>> endpointToRanges = strategy.getAddressRanges();
            StorageService.logger.debug("Calculating ranges to stream and request for keyspace {}", keyspace);
            Iterator var11 = newTokens.iterator();

            while(var11.hasNext()) {
               Token newToken = (Token)var11.next();
               Collection<Range<Token>> currentRanges = endpointToRanges.get(localAddress);
               Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);
               Multimap<Range<Token>, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);
               Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerKeyspace = StorageService.this.calculateStreamAndFetchRanges(currentRanges, updatedRanges);
               Multimap<Range<Token>, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
               Iterator var18 = ((Set)rangesPerKeyspace.right).iterator();

               Iterator var20;
               InetAddress sourceIp;
               label85:
               while(var18.hasNext()) {
                  Range<Token> toFetch = (Range)var18.next();
                  var20 = rangeAddresses.keySet().iterator();

                  while(true) {
                     Object endpoints;
                     while(true) {
                        Range range;
                        do {
                           if(!var20.hasNext()) {
                              Collection<InetAddress> addressList = rangesToFetchWithPreferredEndpoints.get(toFetch);
                              if(addressList != null && !addressList.isEmpty() && StorageService.useStrictConsistency) {
                                 if(addressList.size() > 1) {
                                    throw new IllegalStateException("Multiple strict sources found for " + toFetch);
                                 }

                                 sourceIp = (InetAddress)addressList.iterator().next();
                                 if(Gossiper.instance.isEnabled() && !Gossiper.instance.getEndpointStateForEndpoint(sourceIp).isAlive()) {
                                    throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + ").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
                                 }
                              }
                              continue label85;
                           }

                           range = (Range)var20.next();
                        } while(!range.contains((AbstractBounds)toFetch));

                        endpoints = null;
                        if(StorageService.useStrictConsistency) {
                           Set<InetAddress> oldEndpoints = Sets.newHashSet(rangeAddresses.get(range));
                           Set<InetAddress> newEndpoints = Sets.newHashSet(strategy.calculateNaturalEndpoints((Token)toFetch.right, tokenMetaCloneAllSettled));
                           if(oldEndpoints.size() == strategy.getReplicationFactor()) {
                              oldEndpoints.removeAll(newEndpoints);
                              if(oldEndpoints.isEmpty()) {
                                 continue;
                              }

                              assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                           }

                           endpoints = Lists.newArrayList(new InetAddress[]{(InetAddress)oldEndpoints.iterator().next()});
                           break;
                        }

                        endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                        break;
                     }

                     rangesToFetchWithPreferredEndpoints.putAll(toFetch, (Iterable)endpoints);
                  }
               }

               Multimap<InetAddress, Range<Token>> endpointRanges = HashMultimap.create();
               Iterator var26 = ((Set)rangesPerKeyspace.left).iterator();

               while(var26.hasNext()) {
                  Range<Token> toStream = (Range)var26.next();
                  Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)toStream.right, tokenMetaClone));
                  Set<InetAddress> newEndpointsx = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)toStream.right, tokenMetaCloneAllSettled));
                  StorageService.logger.debug("Range: {} Current endpoints: {} New endpoints: {}", new Object[]{toStream, currentEndpoints, newEndpointsx});
                  Iterator var34 = Sets.difference(newEndpointsx, currentEndpoints).iterator();

                  while(var34.hasNext()) {
                     InetAddress addressx = (InetAddress)var34.next();
                     StorageService.logger.debug("Range {} has new owner {}", toStream, addressx);
                     endpointRanges.put(addressx, toStream);
                  }
               }

               var26 = endpointRanges.keySet().iterator();

               while(var26.hasNext()) {
                  InetAddress address = (InetAddress)var26.next();
                  StorageService.logger.debug("Will stream range {} of keyspace {} to endpoint {}", new Object[]{endpointRanges.get(address), keyspace, address});
                  sourceIp = SystemKeyspace.getPreferredIP(address);
                  this.streamPlan.transferRanges(address, sourceIp, keyspace, endpointRanges.get(address));
               }

               Multimap<InetAddress, Range<Token>> workMap = RangeStreamer.getWorkMapForMove(rangesToFetchWithPreferredEndpoints, keyspace, FailureDetector.instance, StorageService.useStrictConsistency);
               var20 = workMap.keySet().iterator();

               while(var20.hasNext()) {
                  sourceIp = (InetAddress)var20.next();
                  StorageService.logger.debug("Will request range {} of keyspace {} from endpoint {}", new Object[]{workMap.get(sourceIp), keyspace, sourceIp});
                  InetAddress preferred = SystemKeyspace.getPreferredIP(sourceIp);
                  this.streamPlan.requestRanges(sourceIp, preferred, keyspace, workMap.get(sourceIp));
               }

               StorageService.logger.debug("Keyspace {}: work map {}.", keyspace, workMap);
            }
         }

      }

      public Future<StreamState> stream() {
         return this.streamPlan.execute();
      }

      public boolean streamsNeeded() {
         return !this.streamPlan.isEmpty();
      }
   }

   private static enum Mode {
      STARTING,
      NORMAL,
      JOINING,
      LEAVING,
      DECOMMISSIONED,
      MOVING,
      DRAINING,
      DRAINED;

      private Mode() {
      }
   }
}
