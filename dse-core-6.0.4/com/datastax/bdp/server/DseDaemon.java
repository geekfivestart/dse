package com.datastax.bdp.server;

import ch.qos.logback.classic.LoggerContext;
import com.datastax.bdp.cassandra.auth.CassandraDelegationTokenSecretManager;
import com.datastax.bdp.cassandra.auth.DseAuthenticator;
import com.datastax.bdp.cassandra.cache.CompressedCacheStreamFactory;
import com.datastax.bdp.cassandra.cql3.DseQueryHandler;
import com.datastax.bdp.cassandra.crypto.KmipKeyProviderFactory;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHosts;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfigurations;
import com.datastax.bdp.cassandra.db.tiered.TieredTableStats;
import com.datastax.bdp.cassandra.metrics.UserObjectLatencyPlugin;
import com.datastax.bdp.config.AlwaysOnSqlConfig;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.config.SystemTableEncryptionOptions;
import com.datastax.bdp.config.YamlLocation;
import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.gms.UpgradeCheck;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.plugin.PluginManager;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.transport.common.DseReloadableTrustManagerProvider;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.DseJavaSecurityManager;
import com.datastax.bdp.util.DseUtil;
import com.datastax.bdp.util.FileSystemUtil;
import com.datastax.bdp.util.MapBuilder;
import com.datastax.bdp.util.SchemaTool;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.crypto.Cipher;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogCompressorAccessor;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.HintsAccessor;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.compress.EncryptingLZ4Compressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.SuppressPropertiesBeanIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DseDaemon extends CassandraDaemon implements DseDaemonMXBean {
   public static final String SOLR_QUERY_KEY = "solr_query";
   public static final ByteBuffer SOLR_QUERY_KEY_BB = ByteBufferUtil.bytes("solr_query");
   public static final InetAddress UNKNOWN_SOURCE;
   private static final Logger logger = LoggerFactory.getLogger(DseDaemon.class);
   private static final CountDownLatch setupLatch = new CountDownLatch(1);
   private static final CountDownLatch startLatch = new CountDownLatch(1);
   private static final CountDownLatch stopLatch = new CountDownLatch(1);
   private static final AtomicBoolean stopped = new AtomicBoolean(false);
   private static final int RING_TIMEOUT = 10000;
   private static final Class<? extends ICompressor> systemEncryptorClass = EncryptingLZ4Compressor.class;
   @Inject
   static UserObjectLatencyPlugin objectLatencyPlugin;
   @Inject
   static Provider<CassandraDelegationTokenSecretManager> tokenSecretManager;
   @Inject
   static Injector injector;
   private final ImmutableSet<LifecycleAware> listeners;
   private static volatile boolean daemonMode = true;
   private static volatile boolean dseSystemSchemaSetup = false;
   public final PluginManager pluginManager;
   private final InternodeMessaging internodeMessaging;

   public static InetSocketAddress getClientAddress(ClientState state) {
      SocketAddress sockAddress = state.getRemoteAddress();
      return sockAddress instanceof InetSocketAddress?(InetSocketAddress)sockAddress:new InetSocketAddress(UNKNOWN_SOURCE, 0);
   }

   @Inject
   public DseDaemon(Set<LifecycleAware> listeners, PluginManager pluginManager, InternodeMessaging internodeMessaging) {
      this.pluginManager = pluginManager;
      this.internodeMessaging = internodeMessaging;
      this.listeners = (new Builder()).addAll(listeners).build();
   }

   @VisibleForTesting
   public DseDaemon(boolean runManaged, Set<LifecycleAware> listeners, PluginManager pluginManager, InternodeMessaging internodeMessaging) {
      super(runManaged);
      this.pluginManager = pluginManager;
      this.internodeMessaging = internodeMessaging;
      this.listeners = (new Builder()).addAll(listeners).build();
   }

   public static boolean isSetup() {
      return setupLatch.getCount() <= 0L;
   }

   public static String buildSolrIndexName(String keyspace, String table) {
      return keyspace + "_" + table + "_" + "solr_query" + "_index";
   }

   public static void waitForSetup(long timeout, TimeUnit unit) throws InterruptedException {
      if(!setupLatch.await(timeout, unit)) {
         logger.warn("Setup took longer than expected. Continuing...");
      }

   }

   public static void initDseToolMode() {
      DatabaseDescriptor.toolInitialization(false);
      (new DseDaemon(Collections.emptySet(), (PluginManager)null, (InternodeMessaging)null)).preSetup(false);
   }

   public static void initDseClientMode() {
      DatabaseDescriptor.clientInitialization(false);
      daemonMode = false;
   }

   public static boolean isStartupFinished() {
      return startLatch.getCount() <= 0L;
   }

   public boolean isKerberosEnabled() {
      return DseConfig.isKerberosEnabled();
   }

   public String getKerberosPrincipal() {
      if(DseConfig.isKerberosEnabled()) {
         String princ = DseConfig.getDseServicePrincipal().asLocal();
         return princ != null?princ:"Empty - should have a value here, check config files.";
      } else {
         return "N/A";
      }
   }

   public String getReleaseVersion() {
      return SystemInfo.getReleaseVersion("dse_version");
   }

   public boolean getStartupFinished() {
      return isStartupFinished();
   }

   private void setIndexesRemoved(String baseTable, Collection<String> indexNames) {
      Iterator var3 = indexNames.iterator();

      while(var3.hasNext()) {
         String name = (String)var3.next();

         assert name != null;

         TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(baseTable, name));
      }

   }

   public void rebuildSecondaryIndexes(String keyspace, String column_family, List<String> index_names) throws IOException {
      Collection<String> indexNames = index_names;
      if(!Schema.instance.getNonSystemKeyspaces().contains(keyspace)) {
         throw new IOException("Keyspace '" + keyspace + "' wasn't found.");
      } else if(Schema.instance.getTableMetadata(keyspace, column_family) == null) {
         throw new IOException("ColumnFamily '" + column_family + "' wasn't found in keyspace '" + keyspace + "'");
      } else {
         ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(column_family);
         logger.info(String.format("User Requested secondary index re-build for %s/%s of %s indexes.", new Object[]{keyspace, column_family, index_names.isEmpty()?"all":index_names}));
         Collection<Index> allIndexes = cfs.indexManager.listIndexes();
         if(index_names.isEmpty()) {
            Set<String> allIndexNames = new HashSet();
            allIndexes.stream().map((i) -> {
               return i.getIndexMetadata().name;
            }).forEach(allIndexNames::add);
            indexNames = allIndexNames;
         }

         Iterable<SSTableReader> sstables = cfs.getSSTables(SSTableSet.LIVE);
         Refs<SSTableReader> refs = Refs.ref(sstables);
         Throwable var9 = null;

         try {
            this.setIndexesRemoved(cfs.name, (Collection)indexNames);
            cfs.indexManager.rebuildIndexesBlocking(Sets.newHashSet((Iterable)indexNames));
         } catch (Throwable var18) {
            var9 = var18;
            throw var18;
         } finally {
            if(refs != null) {
               if(var9 != null) {
                  try {
                     refs.close();
                  } catch (Throwable var17) {
                     var9.addSuppressed(var17);
                  }
               } else {
                  refs.close();
               }
            }

         }

      }
   }

   private static Map<String, String> getCompressionOpts(SystemTableEncryptionOptions.Wrapper options) {
      return (Map)getCompressionOpts(options, true).right;
   }

   private static Pair<String, Map<String, String>> getCompressionOpts(SystemTableEncryptionOptions.Wrapper options, boolean forTableCompression) {
      Map<String, String> opts = new HashMap();
      if(forTableCompression) {
         opts.put("sstable_compression", systemEncryptorClass.getName());
         opts.put("chunk_length_kb", options.getChunkLengthInKB().toString());
      }

      opts.put("cipher_algorithm", options.getCipherAlgorithm());
      opts.put("secret_key_strength", options.getSecretKeyStrength().toString());
      String keyProvider = options.getKeyProvider();
      opts.put("key_provider", keyProvider);
      if(keyProvider.endsWith(KmipKeyProviderFactory.class.getSimpleName())) {
         opts.put("kmip_host", options.getKmipHost());
      } else {
         opts.put("secret_key_file", String.format("%s/system/%s", new Object[]{DseConfig.getSystemKeyDirectory().getPath(), options.getKeyName()}));
      }

      return Pair.create(systemEncryptorClass.getName(), opts);
   }

   void checkEncryptionOptions(TableMetadata cfmd) {
      CompressionParams params = cfmd.params.compression;
      if(params.isEnabled()) {
         ICompressor compressor = params.getSstableCompressor();

         assert !DatabaseDescriptor.getSystemTableEncryptionOptions().enabled || compressor instanceof EncryptingLZ4Compressor;

         ByteBuffer input = compressor.preferredBufferType().allocate(0);
         ByteBuffer output = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(0));

         try {
            compressor.compress(input, output);
         } catch (IOException var7) {
            throw new RuntimeException("Unable to set encryption settings", var7);
         }
      }

   }

   void maybeEncryptSavedCache(SystemTableEncryptionOptions.Wrapper options) {
      try {
         if(options.isEnabled()) {
            Map<String, String> opts = getCompressionOpts(options);
            CompressionParams parameters = CompressionParams.fromMap(opts);
            ICompressor compressor = parameters.getSstableCompressor();

            assert compressor instanceof EncryptingLZ4Compressor;

            ByteBuffer input = compressor.preferredBufferType().allocate(0);
            ByteBuffer output = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(0));
            compressor.compress(input, output);
            AutoSavingCache.setStreamFactory(new CompressedCacheStreamFactory(parameters));
            logger.info("Saved cache encryption configured");
         }
      } catch (Exception var7) {
         logger.error("Error setting encryption on saved cache", var7);
         throw new RuntimeException("Unable to set encryption settings", var7);
      }
   }

   void maybeEncryptCommitLog(SystemTableEncryptionOptions.Wrapper options) {
      try {
         if(options.isEnabled()) {
            Pair<String, Map<String, String>> opts = getCompressionOpts(options, false);
            ParameterizedClass cl = new ParameterizedClass((String)opts.left, (Map)opts.right);
            DatabaseDescriptor.setCommitLogCompression(cl);
            if(daemonMode && !CommitLogCompressorAccessor.getCommitLogCompressorClass().equals(systemEncryptorClass)) {
               throw new RuntimeException("Couldn't set commit log compressor class to " + systemEncryptorClass.getSimpleName());
            } else {
               logger.info("Commit log encryption configured");
            }
         }
      } catch (Exception var4) {
         logger.error("Error setting encryption on commit log", var4);
         throw new RuntimeException("Unable to set encryption settings", var4);
      }
   }

   void maybeEncryptHints(SystemTableEncryptionOptions.Wrapper options) {
      try {
         if(options.isEnabled()) {
            Pair<String, Map<String, String>> opts = getCompressionOpts(options, false);
            ParameterizedClass cl = new ParameterizedClass((String)opts.left, (Map)opts.right);
            DatabaseDescriptor.setHintsCompression(cl);
            if(daemonMode && !HintsAccessor.getCompressorClass().equals(systemEncryptorClass)) {
               throw new RuntimeException("Couldn't set hints compressor class to " + systemEncryptorClass.getSimpleName());
            } else {
               logger.info("Hints encryption configured");
            }
         }
      } catch (Exception var4) {
         logger.error("Error setting encryption on hints", var4);
         throw new RuntimeException("Unable to set encryption settings", var4);
      }
   }

   protected void setup() {
      logger.info("DSE version: " + SystemInfo.getReleaseVersion("dse_version"));
      logger.info("Solr version: " + SystemInfo.getReleaseVersion("solr_version"));
      logger.info("Appender version: " + SystemInfo.getReleaseVersion("appender_version"));
      logger.info("Spark version: " + SystemInfo.getReleaseVersion("spark_version"));
      logger.info("Spark Job Server version: " + SystemInfo.getReleaseVersion("spark_job_server_version"));
      logger.info("DSE Spark Connector version: " + SystemInfo.getReleaseVersion("dse_version"));

      try {
         logger.info("Maximum Key Length (AES): " + Cipher.getMaxAllowedKeyLength("AES"));
      } catch (NoSuchAlgorithmException var6) {
         logger.warn("Error discovering maximum allowed AES key length", var6);
      }

      try {
         this.preSetup(true);
         super.setup();
         this.postSetup();
      } finally {
         setupLatch.countDown();
      }

   }

   public void start() {
      try {
         waitForSetup(60L, TimeUnit.SECONDS);
         this.preStart();
         super.start();
         this.postStart();
      } catch (Throwable var5) {
         logger.error("Unable to start DSE server.", var5);
         System.err.println("Unable to start DSE server: " + var5.getMessage());
         var5.printStackTrace(System.err);
         throw new RuntimeException(var5);
      } finally {
         startLatch.countDown();
      }

   }

   private void preStart() {
      if(SystemInfo.isAnalyticsNode() && SystemInfo.isSearchNode()) {
         logger.warn("Both Search and Analytics services are enabled. Be advised that workloads will not be isolated within this node.");
      }

      SchemaTool.waitForRingToStabilize();
      UnmodifiableIterator var1 = this.listeners.iterator();

      while(var1.hasNext()) {
         LifecycleAware listener = (LifecycleAware)var1.next();
         listener.preStart();
      }

   }

   public void startNativeTransport() {
      super.startNativeTransport();
      this.listeners.forEach(LifecycleAware::postStartNativeTransport);
   }

   public void stopNativeTransport() {
      this.listeners.forEach(LifecycleAware::preStopNativeTransport);
      super.stopNativeTransport();
      logger.debug("Native server stopped");
   }

   public CompletableFuture stopNativeTransportAsync() {
      this.listeners.forEach(LifecycleAware::preStopNativeTransport);
      CompletableFuture future = super.stopNativeTransportAsync();
      logger.debug("Native server stopped");
      return future;
   }

   private void broadcastMyDC() {
      if(Gossiper.instance.isKnownEndpoint(Addresses.Internode.getBroadcastAddress())) {
         IEndpointSnitch endpointSnitch = DatabaseDescriptor.getEndpointSnitch();
         String myDC = endpointSnitch.getDatacenter(Addresses.Internode.getBroadcastAddress());
         Gossiper.instance.addLocalApplicationState(ApplicationState.DC, StorageService.instance.valueFactory.datacenter(myDC));
      }

   }

   public Set<LifecycleAware> getListeners() {
      return this.listeners;
   }

   private void preSetup(boolean daemonMode) {
      daemonMode = daemonMode;
      PropertyUtils.addBeanIntrospector(SuppressPropertiesBeanIntrospector.SUPPRESS_CLASS);
      DseConfig.init();
      logger.info("AlwaysOn SQL is {}enabled", AlwaysOnSqlConfig.isEnabled().booleanValue()?"":"not ");
      Addresses.init();
      if(daemonMode && DatabaseDescriptor.getAuthenticator().requireAuthentication() && DatabaseDescriptor.getAuthenticator().implementation().getClass() != DseAuthenticator.class) {
         logger.error("An unsupported Authenticator '{}' is in use. Many DSE security features will not work correctly unless DseAuthenticator is used.", DatabaseDescriptor.getAuthenticator().implementation().getClass().getSimpleName());
      }

      TieredStorageConfigurations.init();
      this.checkEncryptionOptions(SystemKeyspace.Paxos);
      this.checkEncryptionOptions(SystemKeyspace.Batches);
      this.maybeEncryptSavedCache(DseConfig.getSystemInfoEncryptionOptions());
      this.maybeEncryptCommitLog(DseConfig.getSystemInfoEncryptionOptions());
      this.maybeEncryptHints(DseConfig.getSystemInfoEncryptionOptions());
      if(daemonMode) {
         StorageService.instance.addPreShutdownHook(this::preShutdownHook);
         StorageService.instance.addPostShutdownHook(this::postShutdownHook);
      }

      FileSystemUtil.initJna();
      DseReloadableTrustManagerProvider.maybeInstall();
      if(daemonMode) {
         this.checkIfRequiredUpgradeIsSkippedAndSetDseVersion();
      }

      dseSystemSchemaSetup = true;
      if(daemonMode) {
         EndpointStateTracker.instance.setup();
         UnmodifiableIterator var2 = this.listeners.iterator();

         while(var2.hasNext()) {
            LifecycleAware listener = (LifecycleAware)var2.next();
            listener.preSetup();
         }

         this.internodeMessaging.activate();
      }

   }

   private void checkIfRequiredUpgradeIsSkippedAndSetDseVersion() {
      String releaseVersion = this.getReleaseVersion();
      CassandraVersion currentVersion = null == releaseVersion?null:new CassandraVersion(releaseVersion);
      CassandraVersion lastKnownDseVersion = this.getLastKnownDseVersion();
      UpgradeCheck.failIfRequiredUpgradeIsSkipped(currentVersion, lastKnownDseVersion);
      TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(String.format("UPDATE system.local SET %s='%s' WHERE key='local';", new Object[]{"dse_version", SystemInfo.getReleaseVersion("dse_version")}), new Object[0]));
   }

   private CassandraVersion getLastKnownDseVersion() {
      try {
         UntypedResultSet resultSet = (UntypedResultSet)TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(String.format("SELECT %s FROM system.local WHERE key='local';", new Object[]{"dse_version"}), new Object[0]));
         if(!resultSet.isEmpty()) {
            return new CassandraVersion(resultSet.one().getString("dse_version"));
         }
      } catch (Exception var2) {
         logger.warn("Could not retrieve last known DSE version from system.local.");
      }

      return null;
   }

   private void postSetup() {
      System.setSecurityManager(new DseJavaSecurityManager(System.getSecurityManager()));
      JMX.registerMBean(this, JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{"DseDaemon"}).build());
      RpcRegistry.register(EndpointStateTracker.instance);
      JMX.registerMBean(EndpointStateTracker.instance, JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{"EndpointStateTracker"}).build());
      JMX.registerMBean(KmipHosts.instance, JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{"KmipHosts"}).build());
      JMX.registerMBean(new YamlLocation(), JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{"YamlLocation"}).build());
      JMX.registerMBean(TieredTableStats.instance, JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{"TieredTableStats"}).build());
      DseState.instance.initialize();

      try {
         logger.info("Waiting for other nodes to become alive...");
         if(!EndpointStateTracker.instance.waitForAllNodesAlive(10000)) {
            logger.warn("The following nodes seems to be down: " + Gossiper.instance.getUnreachableMembers() + ". Some Cassandra operations may fail with UnavailableException.");
         }
      } catch (InterruptedException var3) {
         logger.info("Got interrupt when waiting for QUORUM of nodes...");
      }

      logger.info("Wait for nodes completed");
      Addresses.Internode.getBroadcastAddress();
      this.broadcastMyDC();
      UnmodifiableIterator var1 = this.listeners.iterator();

      while(var1.hasNext()) {
         LifecycleAware listener = (LifecycleAware)var1.next();
         listener.postSetup();
      }

   }

   private void postStart() {
      if(EndpointStateTracker.instance.getActiveStatus(Addresses.Internode.getBroadcastAddress())) {
         logger.warn("Node flagged as active before all plugins finished starting.  This is a bug, but a minor one as long as everything starts correctly.  If you also see PluginNotActiveExceptions cluttering the log, this is the root cause.");
      }

      DseState.instance.setActiveStatusAsync(true);
      UnmodifiableIterator var1 = this.listeners.iterator();

      while(var1.hasNext()) {
         LifecycleAware listener = (LifecycleAware)var1.next();
         listener.postStart();
      }

      logger.info("DSE startup complete.");
   }

   public List<List<String>> getSplits(String ks, String cf, int rangeSize, String startToken, String endToken) {
      List<List<String>> serializableSplits = new ArrayList();
      TableMetadata table = Schema.instance.getTableMetadata(ks, cf);
      TokenFactory tf = table.partitioner.getTokenFactory();
      Range<Token> tr = new Range(tf.fromString(startToken), tf.fromString(endToken));
      Iterator var10 = StorageService.instance.getSplits(ks, cf, tr, rangeSize).iterator();

      while(var10.hasNext()) {
         Pair<Range<Token>, Long> split = (Pair)var10.next();
         List<String> tuple = new ArrayList();
         tuple.add(((Token)((Range)split.left).left).toString());
         tuple.add(((Token)((Range)split.left).right).toString());
         tuple.add(((Long)split.right).toString());
         serializableSplits.add(tuple);
      }

      return serializableSplits;
   }

   protected void preShutdownHook() {
      try {
         if(stopped.compareAndSet(false, true)) {
            if(this.isNativeTransportRunning()) {
               this.stopNativeTransport();
            }

            DseState.instance.setActiveStatusSync(false);
            DseUtil.ignoreInterrupts(() -> {
               startLatch.await(10L, TimeUnit.SECONDS);
            });
            logger.info("DSE shutting down...");
            int cleanShutdownTimeout = Integer.getInteger("dse.clean.shutdown.timeout", -1).intValue();
            if(cleanShutdownTimeout == 0) {
               Runtime.getRuntime().halt(0);
            } else if(cleanShutdownTimeout > 0) {
               Executors.newSingleThreadExecutor().submit(() -> {
                  try {
                     stopLatch.await((long)cleanShutdownTimeout, TimeUnit.SECONDS);
                  } catch (InterruptedException var2) {
                     logger.warn("JVM did not shutdown within " + cleanShutdownTimeout + "s, forcing it now!");
                     Runtime.getRuntime().halt(0);
                  }

               });
            }

            DseReloadableTrustManagerProvider.maybeUninstall();
            UnmodifiableIterator var2 = this.listeners.asList().reverse().iterator();

            while(var2.hasNext()) {
               LifecycleAware listener = (LifecycleAware)var2.next();

               try {
                  listener.preStop();
               } catch (Exception var5) {
                  logger.warn("Continuing normal shutdown despite exception", var5);
               }
            }

            this.internodeMessaging.deactivate();
         }
      } catch (Exception var6) {
         logger.warn("Caught exception while shutting down; trying to continue . . .", var6);
      }

   }

   protected void postShutdownHook() {
      boolean var9 = false;

      LoggerContext loggerContext;
      label77: {
         try {
            var9 = true;
            UnmodifiableIterator var1 = this.listeners.asList().reverse().iterator();

            while(var1.hasNext()) {
               LifecycleAware listener = (LifecycleAware)var1.next();

               try {
                  logger.debug("Stopping listener {}", listener);
                  listener.postStop();
                  logger.debug("Stopped listener {}", listener);
               } catch (Exception var10) {
                  logger.warn("Continuing normal shutdown despite exception.", var10);
               }
            }

            var9 = false;
            break label77;
         } catch (Exception var11) {
            logger.warn("Caught exception while shutting down; ignoring", var11);
            var9 = false;
         } finally {
            if(var9) {
               stopLatch.countDown();
               logger.info("DSE shutdown complete.");
               LoggerContext var5 = (LoggerContext)LoggerFactory.getILoggerFactory();
               var5.stop();
            }
         }

         stopLatch.countDown();
         logger.info("DSE shutdown complete.");
         loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
         loggerContext.stop();
         return;
      }

      stopLatch.countDown();
      logger.info("DSE shutdown complete.");
      loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
      loggerContext.stop();
   }

   public static boolean isDaemonMode() {
      return daemonMode;
   }

   public static boolean isSystemSchemaSetup() {
      return dseSystemSchemaSetup;
   }

   public static boolean isStopped() {
      return stopped.get();
   }

   public static boolean canProcessQueries() {
      StorageService ss = StorageService.instance;
      return ss.isDaemonSetupCompleted() && !ss.isDraining() && !ss.isDrained();
   }

   static {
      try {
         UNKNOWN_SOURCE = InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
      } catch (UnknownHostException var1) {
         logger.error("Error creating default InetAddress for unknown event sources", var1);
         throw new RuntimeException("Unable to initialise constants for audit logging", var1);
      }

      String queryHandlerClass = System.getProperty("cassandra.custom_query_handler_class");
      if(null != queryHandlerClass && !Objects.equals(queryHandlerClass, DseQueryHandler.class.getName())) {
         logger.warn("Custom QueryHandler {} specified. This is not recommended and some DSE features may not be available", queryHandlerClass);
      } else {
         System.setProperty("cassandra.custom_query_handler_class", DseQueryHandler.class.getName());
      }

      System.setProperty("logbackDisableServletContainerInitializer", "true");
   }
}
