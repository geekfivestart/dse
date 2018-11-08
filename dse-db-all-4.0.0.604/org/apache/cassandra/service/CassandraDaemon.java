package org.apache.cassandra.service;

import com.addthis.metrics3.reporter.config.ReporterConfig;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.MetricRegistryListener.Base;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.nodesync.NodeSyncServiceProxy;
import com.datastax.bdp.db.upgrade.ClusterVersionBarrier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Mx4jTool;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.WindowsTimer;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDaemon {
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";
   private static final Logger logger;
   public static BiConsumer<Thread, Throwable> defaultExceptionHandler;
   static final CassandraDaemon instance;
   @VisibleForTesting
   public NativeTransportService nativeTransportService;
   private JMXConnectorServer jmxServer;
   private final boolean runManaged;
   protected final StartupChecks startupChecks;
   private boolean setupCompleted;
   private final CountDownLatch isRunning;

   private void maybeInitJmx() {
      System.setProperty("java.rmi.server.randomIDs", "true");
      DatabaseDescriptor.getJMXPort().ifPresent((port) -> {
         boolean localOnly = DatabaseDescriptor.isJMXLocalOnly();

         try {
            this.jmxServer = JMXServerUtils.createJMXServer(port.intValue(), localOnly);
         } catch (IOException var4) {
            this.exitOrFail(1, var4.getMessage(), var4.getCause());
         }

      });
   }

   public CassandraDaemon() {
      this(false);
   }

   public CassandraDaemon(boolean runManaged) {
      this.isRunning = new CountDownLatch(1);
      this.runManaged = runManaged;
      this.startupChecks = (new StartupChecks()).withDefaultTests();
      this.setupCompleted = false;
   }

   protected void setup() {
      if(FBUtilities.isWindows) {
         WindowsFailedSnapshotTracker.deleteOldSnapshots();
      }

      this.maybeInitJmx();
      Mx4jTool.maybeLoad();
      ThreadAwareSecurityManager.install();
      this.logSystemInfo();
      NativeLibrary.tryMlockall();

      try {
         this.startupChecks.verify();
      } catch (StartupException var19) {
         this.exitOrFail(var19.returnCode, var19.getMessage(), var19.getCause());
      }

      TPCUtils.blockingAwait(SystemKeyspace.snapshotOnVersionChange().whenComplete((v, e) -> {
         if(e != null) {
            if(e instanceof CompletionException && e.getCause() != null) {
               e = e.getCause();
            }

            if(e instanceof IOException) {
               this.exitOrFail(3, e.getMessage(), e.getCause());
            }
         }

      }));
      SystemKeyspace.beginStartupBlocking();
      StorageService.instance.populateTokenMetadata();

      try {
         Schema.instance.loadFromDisk();
      } catch (Exception var18) {
         logger.error("Error while loading schema: ", var18);
         throw var18;
      }

      Iterator var1 = Schema.instance.getKeyspaces().iterator();

      while(true) {
         String metricsReporterConfigFile;
         Iterator var3;
         Iterator var5;
         do {
            if(!var1.hasNext()) {
               Keyspace.setInitialized();
               var1 = Schema.instance.getKeyspaces().iterator();

               while(var1.hasNext()) {
                  metricsReporterConfigFile = (String)var1.next();
                  if(logger.isDebugEnabled()) {
                     logger.debug("opening keyspace {}", metricsReporterConfigFile);
                  }

                  var3 = Keyspace.open(metricsReporterConfigFile).getColumnFamilyStores().iterator();

                  while(var3.hasNext()) {
                     ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
                     var5 = cfs.concatWithIndexes().iterator();

                     while(var5.hasNext()) {
                        ColumnFamilyStore store = (ColumnFamilyStore)var5.next();
                        store.logStartupWarnings();
                        store.disableAutoCompaction();
                     }
                  }
               }

               try {
                  this.loadRowAndKeyCacheAsync().get();
               } catch (Throwable var15) {
                  JVMStabilityInspector.inspectThrowable(var15);
                  logger.warn("Error loading key or row cache", var15);
               }

               try {
                  GCInspector.register();
               } catch (Throwable var14) {
                  JVMStabilityInspector.inspectThrowable(var14);
                  logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
               }

               this.setDefaultTPCBoundaries();

               try {
                  CommitLog.instance.recoverSegmentsOnDisk();
               } catch (IOException var13) {
                  throw new RuntimeException(var13);
               }

               SystemKeyspace.finishStartupBlocking();
               Gossiper.instance.registerUpgradeBarrierListener();
               StorageService.instance.populateTokenMetadata();
               ActiveRepairService.instance.start();
               StorageService.instance.cleanupSizeEstimates();
               int sizeRecorderInterval = Integer.getInteger("cassandra.size_recorder_interval", 300).intValue();
               if(sizeRecorderInterval > 0) {
                  ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SizeEstimatesRecorder.instance, 30L, (long)sizeRecorderInterval, TimeUnit.SECONDS);
               }

               QueryProcessor.preloadPreparedStatementBlocking();
               metricsReporterConfigFile = System.getProperty("cassandra.metricsReporterConfigFile");
               if(metricsReporterConfigFile != null) {
                  logger.info("Trying to load metrics-reporter-config from file: {}", metricsReporterConfigFile);

                  try {
                     CassandraMetricsRegistry.Metrics.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
                     CassandraMetricsRegistry.Metrics.register("jvm.gc", new GarbageCollectorMetricSet());
                     CassandraMetricsRegistry.Metrics.register("jvm.memory", new MemoryUsageGaugeSet());
                     CassandraMetricsRegistry.Metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge());
                     URL resource = CassandraDaemon.class.getClassLoader().getResource(metricsReporterConfigFile);
                     if(resource == null) {
                        logger.warn("Failed to load metrics-reporter-config, file does not exist: {}", metricsReporterConfigFile);
                     } else {
                        String reportFileLocation = resource.getFile();
                        ReporterConfig.loadFromFile(reportFileLocation).enableAll(CassandraMetricsRegistry.Metrics);
                     }
                  } catch (Exception var12) {
                     logger.warn("Failed to load metrics-reporter-config, metric sinks will not be activated", var12);
                  }
               }

               StorageService.instance.registerDaemon(this);

               try {
                  StorageService.instance.initServer();
               } catch (ConfigurationException var11) {
                  System.err.println(var11.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
                  this.exitOrFail(1, "Fatal configuration error", var11);
               }

               Runnable viewRebuild = () -> {
                  Iterator var0 = Keyspace.all().iterator();

                  while(var0.hasNext()) {
                     Keyspace keyspace = (Keyspace)var0.next();
                     keyspace.viewManager.buildAllViews();
                  }

                  logger.debug("Completed submission of build tasks for any materialized views defined at startup");
               };
               ScheduledExecutors.optionalTasks.schedule(viewRebuild, (long)StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
               Iterator var25 = Keyspace.all().iterator();

               while(var25.hasNext()) {
                  Keyspace keyspace = (Keyspace)var25.next();
                  Iterator var28 = keyspace.getColumnFamilyStores().iterator();

                  while(var28.hasNext()) {
                     ColumnFamilyStore cfs = (ColumnFamilyStore)var28.next();
                     Iterator var8 = cfs.concatWithIndexes().iterator();

                     while(var8.hasNext()) {
                        ColumnFamilyStore store = (ColumnFamilyStore)var8.next();
                        store.reload();
                        if(store.getCompactionStrategyManager().shouldBeEnabled()) {
                           store.enableAutoCompaction();
                        }
                     }
                  }
               }

               ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(ColumnFamilyStore.getBackgroundCompactionTaskSubmitter(), 5L, 1L, TimeUnit.MINUTES);
               this.nativeTransportService = new NativeTransportService();
               this.completeSetup();
               StorageService.instance.getTokenMetadata().invalidateCachedRings();

               try {
                  FBUtilities.waitOnFutures(Iterables.concat(Iterables.transform(Keyspace.all(), Keyspace::flush)));
               } catch (Throwable var10) {
                  JVMStabilityInspector.inspectThrowable(var10);
                  logger.error("Error during initial flush after setup", var10);
               }

               return;
            }

            metricsReporterConfigFile = (String)var1.next();
         } while(metricsReporterConfigFile.equals("system"));

         var3 = Schema.instance.getTablesAndViews(metricsReporterConfigFile).iterator();

         while(var3.hasNext()) {
            TableMetadata cfm = (TableMetadata)var3.next();
            var5 = cfm.triggers.iterator();

            while(var5.hasNext()) {
               TriggerMetadata trigger = (TriggerMetadata)var5.next();

               try {
                  TriggerExecutor.instance.loadTriggerInstance(trigger.classOption);
               } catch (Throwable var17) {
                  this.exitOrFail(100, String.format("Could not load class '%s' from trigger '%s' from %s.%s. Cannot continue startup. Trigger load error stack trace: %s", new Object[]{trigger.classOption, trigger.name, cfm.keyspace, cfm.name, ExceptionUtils.getStackTrace(var17)}), var17.getCause());
               }
            }

            try {
               ColumnFamilyStore.scrubDataDirectories(cfm);
            } catch (StartupException var16) {
               this.exitOrFail(var16.returnCode, var16.getMessage(), var16.getCause());
            }
         }
      }
   }

   private void setDefaultTPCBoundaries() {
      Iterator var1 = Keyspace.nonSystem().iterator();

      while(var1.hasNext()) {
         Keyspace ks = (Keyspace)var1.next();
         List<Range<Token>> ranges = DatabaseDescriptor.getPartitioner().splitter().isPresent()?Range.merge((Collection)ks.getColumnFamilyStores().stream().flatMap((cfs) -> {
            return cfs.getLiveSSTables().stream();
         }).map((sstable) -> {
            return new Range(sstable.first.getToken(), sstable.last.getToken());
         }).collect(Collectors.toList())):Collections.emptyList();
         logger.info("Computing default TPC core assignments for {} based on ranges {}...", ks.getName(), ranges);
         ks.setDefaultTPCBoundaries(!ranges.isEmpty()?ranges:StorageService.getStartupTokenRanges(ks));
      }

      FBUtilities.waitOnFutures(Iterables.concat(Iterables.transform(Keyspace.all(), Keyspace::flush)));
   }

   private ListenableFuture<?> loadRowAndKeyCacheAsync() {
      ListenableFuture<Integer> keyCacheLoad = CacheService.instance.keyCache.loadSavedAsync();
      ListenableFuture<Integer> rowCacheLoad = CacheService.instance.rowCache.loadSavedAsync();
      ListenableFuture<List<Integer>> retval = Futures.successfulAsList(new ListenableFuture[]{keyCacheLoad, rowCacheLoad});
      return retval;
   }

   @VisibleForTesting
   public void completeSetup() {
      StorageService.instance.installDiskErrorHandler();
      this.setupCompleted = true;
   }

   public boolean setupCompleted() {
      return this.setupCompleted;
   }

   private void logSystemInfo() {
      if(logger.isInfoEnabled()) {
         try {
            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
         } catch (UnknownHostException var3) {
            logger.info("Could not resolve local host");
         }

         logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
         logger.info("Heap size: {}/{}", FBUtilities.prettyPrintMemory(Runtime.getRuntime().totalMemory()), FBUtilities.prettyPrintMemory(Runtime.getRuntime().maxMemory()));
         Iterator var1 = ManagementFactory.getMemoryPoolMXBeans().iterator();

         while(var1.hasNext()) {
            MemoryPoolMXBean pool = (MemoryPoolMXBean)var1.next();
            logger.info("{} {}: {}", new Object[]{pool.getName(), pool.getType(), pool.getPeakUsage()});
         }

         logger.info("Classpath: {}", System.getProperty("java.class.path"));
         logger.info("JVM Arguments: {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
      }

   }

   public void init(String[] arguments) throws IOException {
      this.setup();
   }

   public void start() {
      String nativeFlag = System.getProperty("cassandra.start_native_transport");
      if((nativeFlag == null || !Boolean.parseBoolean(nativeFlag)) && (nativeFlag != null || !DatabaseDescriptor.startNativeTransport())) {
         logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");
      } else {
         long nativeTransportStartupDelay = Long.getLong("cassandra.native_transport_startup_delay_seconds", 0L).longValue();
         Runnable startupNativeTransport = () -> {
            this.startNativeTransport();
            StorageService.instance.setNativeTransportReady(true);
         };
         if(nativeTransportStartupDelay > 0L) {
            logger.info("Delayed startup of native transport for {} seconds.", Long.valueOf(nativeTransportStartupDelay));
            ScheduledExecutors.nonPeriodicTasks.schedule(startupNativeTransport, nativeTransportStartupDelay, TimeUnit.SECONDS);
         } else {
            startupNativeTransport.run();
         }
      }

      if(DatabaseDescriptor.getNodeSyncConfig().isEnabled()) {
         StorageService.instance.nodeSyncService.enableAsync().whenComplete((s, e) -> {
            if(e != null) {
               if(e instanceof NodeSyncService.UpgradingClusterException) {
                  logger.warn(e.getMessage());
               } else {
                  logger.error("Unexpected error starting the NodeSync service. No tables will be validated by NodeSync.", e);
               }
            }

         });
      }

      NodeSyncServiceProxy.init();
   }

   public void stop() {
      logger.info("DSE shutting down...");
      if(this.nativeTransportService != null) {
         this.nativeTransportService.destroy();
      }

      StorageService.instance.setNativeTransportReady(false);
      if(FBUtilities.isWindows) {
         System.exit(0);
      }

      if(this.jmxServer != null) {
         try {
            this.jmxServer.stop();
         } catch (IOException var3) {
            logger.error("Error shutting down local JMX server: ", var3);
         }
      }

      try {
         StorageService.instance.nodeSyncService.disable(false, 2L, TimeUnit.MINUTES);
      } catch (TimeoutException var2) {
         logger.error("Timed-out (after 2 minutes) while waiting on the NodeSync service to stop");
      }

   }

   public void destroy() {
   }

   public void activate(boolean wait) {
      try {
         this.applyConfig();

         try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new StandardMBean(new CassandraDaemon.NativeAccess(), NativeAccessMBean.class), new ObjectName("org.apache.cassandra.db:type=NativeAccess"));
         } catch (Exception var4) {
            logger.error("error registering MBean {}", "org.apache.cassandra.db:type=NativeAccess", var4);
         }

         if(FBUtilities.isWindows) {
            WindowsTimer.startTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
         }

         this.setup();
         String pidFile = System.getProperty("cassandra-pidfile");
         if(pidFile != null) {
            (new File(pidFile)).deleteOnExit();
         }

         if(System.getProperty("cassandra-foreground") == null) {
            System.out.close();
            System.err.close();
         }

         Gossiper.instance.clusterVersionBarrier.register(new ClusterVersionBarrier.ClusterVersionListener() {
            private Boolean versionsEquals = null;

            public void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo) {
               Boolean newEquals = Boolean.valueOf(versionInfo.minOss.equals(versionInfo.maxOss));
               if(!Objects.equals(this.versionsEquals, newEquals)) {
                  this.versionsEquals = newEquals;
                  if(newEquals.booleanValue()) {
                     CassandraDaemon.logger.info("All nodes in this cluster are on version {}", versionInfo.minOss);
                  } else {
                     CassandraDaemon.logger.info("Nodes in this cluster are on version {} up to version {}", versionInfo.minOss, versionInfo.maxOss);
                  }
               }

            }
         });
         logger.debug("ClusterVersionBarrier starting");
         Gossiper.instance.clusterVersionBarrier.onLocalNodeReady();
         this.start();
         if(wait) {
            Uninterruptibles.awaitUninterruptibly(this.isRunning);
         }
      } catch (Throwable var5) {
         boolean logStackTrace = var5 instanceof ConfigurationException?((ConfigurationException)var5).logStackTrace:true;
         System.out.println("Exception (" + var5.getClass().getName() + ") encountered during startup: " + var5.getMessage());
         if(logStackTrace) {
            if(this.runManaged) {
               logger.error("Exception encountered during startup", var5);
            }

            var5.printStackTrace();
            this.exitOrFail(3, "Exception encountered during startup", var5);
         } else {
            if(this.runManaged) {
               logger.error("Exception encountered during startup: {}", var5.getMessage());
            }

            System.err.println(var5.getMessage());
            this.exitOrFail(3, "Exception encountered during startup: " + var5.getMessage());
         }
      }

   }

   public void applyConfig() {
      DatabaseDescriptor.daemonInitialization();
   }

   public void startNativeTransport() {
      if(this.nativeTransportService == null) {
         throw new IllegalStateException("setup() must be called first for CassandraDaemon");
      } else {
         this.nativeTransportService.start();
      }
   }

   public void stopNativeTransport() {
      if(this.nativeTransportService != null) {
         this.nativeTransportService.stop();
      }

   }

   public CompletableFuture stopNativeTransportAsync() {
      return this.nativeTransportService != null?this.nativeTransportService.stopAsync():CompletableFuture.completedFuture((Object)null);
   }

   public boolean isNativeTransportRunning() {
      return this.nativeTransportService != null?this.nativeTransportService.isRunning():false;
   }

   public void deactivate() {
      this.stop();
      this.destroy();
      this.isRunning.countDown();
      if(!this.runManaged) {
         System.exit(0);
      }

   }

   public static void stop(String[] args) {
      instance.deactivate();
   }

   public static void main(String[] args) {
      instance.activate(true);
   }

   public static void startForDseTesting() {
      instance.activate(false);
   }

   private void exitOrFail(int code, String message) {
      this.exitOrFail(code, message, (Throwable)null);
   }

   private void exitOrFail(int code, String message, Throwable cause) {
      if(this.runManaged) {
         RuntimeException t = cause != null?new RuntimeException(message, cause):new RuntimeException(message);
         throw t;
      } else {
         logger.error(message, cause);
         System.exit(code);
      }
   }

   static {
      LineNumberInference.init();
      SharedMetricRegistries.getOrCreate("logback-metrics").addListener(new Base() {
         public void onMeterAdded(String metricName, Meter meter) {
            int separator = metricName.lastIndexOf(46);
            String appenderName = metricName.substring(0, separator);
            String metric = metricName.substring(separator + 1);
            ObjectName name = DefaultNameFactory.createMetricName(appenderName, metric, (String)null).getMBeanName();
            CassandraMetricsRegistry.Metrics.registerMBean(meter, name);
         }
      });
      logger = LoggerFactory.getLogger(CassandraDaemon.class);
      Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
         defaultExceptionHandler.accept(t, e);
      });
      defaultExceptionHandler = new BiConsumer<Thread, Throwable>() {
         public void accept(Thread t, Throwable e) {
            if(!(e instanceof RequestExecutionException) && (e.getCause() == null || !(e.getCause() instanceof RequestExecutionException))) {
               StorageMetrics.uncaughtExceptions.inc();
               CassandraDaemon.logger.error("Exception in thread " + t, e);
               Tracing.trace("Exception in thread {}", t, e);
               JVMStabilityInspector.inspectThrowable(e);
            } else {
               CassandraDaemon.logger.debug("Got duplicated request execution exception: {}", e.getMessage());
            }
         }
      };
      instance = new CassandraDaemon();
   }

   public interface Server {
      void start();

      CompletableFuture stop();

      boolean isRunning();
   }

   static class NativeAccess implements NativeAccessMBean {
      NativeAccess() {
      }

      public boolean isAvailable() {
         return NativeLibrary.isAvailable();
      }

      public boolean isMemoryLockable() {
         return NativeLibrary.jnaMemoryLockable();
      }
   }
}
