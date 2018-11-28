package org.apache.cassandra.db;

import com.clearspring.analytics.stream.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.Runnables;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.cassandra.cache.CounterCacheKey;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.TableInfo;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.DefaultValue;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TopKSampler;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Refs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean {
   private static volatile Directories.DataDirectory[] initialDirectories;
   private static final Logger logger;
   private static final ExecutorService flushExecutor;
   private static final ExecutorService[] perDiskflushExecutors;
   private static final ExecutorService postFlushExecutor;
   private static final ExecutorService reclaimExecutor;
   private static final String[] COUNTER_NAMES;
   private static final String[] COUNTER_DESCS;
   private static final CompositeType COUNTER_COMPOSITE_TYPE;
   private static final TabularType COUNTER_TYPE;
   private static final String[] SAMPLER_NAMES;
   private static final String[] SAMPLER_DESCS;
   private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";
   private static final CompositeType SAMPLING_RESULT;
   public static final String SNAPSHOT_TRUNCATE_PREFIX = "truncated";
   public static final String SNAPSHOT_DROP_PREFIX = "dropped";
   public final Keyspace keyspace;
   public final String name;
   public final TableMetadataRef metadata;
   private final String mbeanName;
   /** @deprecated */
   @Deprecated
   private final String oldMBeanName;
   private volatile boolean valid = true;
   private final Tracker data;
   public final OpOrder readOrdering = TPC.newOpOrder(this);
   private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);
   public final SecondaryIndexManager indexManager;
   public final TableViews viewManager;
   private volatile DefaultValue<Integer> minCompactionThreshold;
   private volatile DefaultValue<Integer> maxCompactionThreshold;
   private volatile DefaultValue<Double> crcCheckChance;
   private final CompactionStrategyManager compactionStrategyManager;
   private volatile Directories directories;
   public final TableMetrics metric;
   public volatile long sampleLatencyNanos;
   private final ScheduledFuture<?> latencyCalculator;
   private volatile boolean compactionSpaceCheck = true;
   @VisibleForTesting
   final DiskBoundaryManager diskBoundaryManager = new DiskBoundaryManager();

   public static synchronized void addInitialDirectories(Directories.DataDirectory[] newDirectories) {
      assert newDirectories != null;

      Set<Directories.DataDirectory> existing = Sets.newHashSet(initialDirectories);
      List<Directories.DataDirectory> replacementList = Lists.newArrayList(initialDirectories);
      Directories.DataDirectory[] replacementArray = newDirectories;
      int var4 = newDirectories.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         Directories.DataDirectory directory = replacementArray[var5];
         if(!existing.contains(directory)) {
            replacementList.add(directory);
         }
      }

      replacementArray = new Directories.DataDirectory[replacementList.size()];
      replacementList.toArray(replacementArray);
      initialDirectories = replacementArray;
   }

   public static Directories.DataDirectory[] getInitialDirectories() {
      Directories.DataDirectory[] src = initialDirectories;
      return (Directories.DataDirectory[])Arrays.copyOf(src, src.length);
   }

   public static void shutdownPostFlushExecutor() throws InterruptedException {
      postFlushExecutor.shutdown();
      postFlushExecutor.awaitTermination(60L, TimeUnit.SECONDS);
   }

   public void reload() {
      Iterator var1;
      ColumnFamilyStore cfs;
      if(!this.minCompactionThreshold.isModified()) {
         for(var1 = this.concatWithIndexes().iterator(); var1.hasNext(); cfs.minCompactionThreshold = new DefaultValue(Integer.valueOf(this.metadata().params.compaction.minCompactionThreshold()))) {
            cfs = (ColumnFamilyStore)var1.next();
         }
      }

      if(!this.maxCompactionThreshold.isModified()) {
         for(var1 = this.concatWithIndexes().iterator(); var1.hasNext(); cfs.maxCompactionThreshold = new DefaultValue(Integer.valueOf(this.metadata().params.compaction.maxCompactionThreshold()))) {
            cfs = (ColumnFamilyStore)var1.next();
         }
      }

      if(!this.crcCheckChance.isModified()) {
         for(var1 = this.concatWithIndexes().iterator(); var1.hasNext(); cfs.crcCheckChance = new DefaultValue(Double.valueOf(this.metadata().params.crcCheckChance))) {
            cfs = (ColumnFamilyStore)var1.next();
         }
      }

      this.compactionStrategyManager.maybeReload(this.metadata());
      this.directories = this.compactionStrategyManager.getDirectories();
      this.scheduleFlush();
      this.indexManager.reload();
      if(this.data.getView().getCurrentMemtable().initialComparator != this.metadata().comparator) {
         this.switchMemtable();
      }

   }

   void scheduleFlush() {
      int period = this.metadata().params.memtableFlushPeriodInMs;
      if(period > 0) {
         logger.trace("scheduling flush in {} ms", Integer.valueOf(period));
         WrappedRunnable runnable = new WrappedRunnable() {
            protected void runMayThrow() {
               synchronized(ColumnFamilyStore.this.data) {
                  Memtable current = ColumnFamilyStore.this.data.getView().getCurrentMemtable();
                  if(current.isExpired()) {
                     if(current.isClean()) {
                        ColumnFamilyStore.this.scheduleFlush();
                     } else {
                        ColumnFamilyStore.this.forceFlush();
                     }
                  }

               }
            }
         };
         ScheduledExecutors.scheduledTasks.schedule(runnable, (long)period, TimeUnit.MILLISECONDS);
      }

   }

   public static Runnable getBackgroundCompactionTaskSubmitter() {
      return new Runnable() {
         public void run() {
            Iterator var1 = Keyspace.all().iterator();

            while(var1.hasNext()) {
               Keyspace keyspace = (Keyspace)var1.next();
               Iterator var3 = keyspace.getColumnFamilyStores().iterator();

               while(var3.hasNext()) {
                  ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
                  CompactionManager.instance.submitBackground(cfs);
               }
            }

         }
      };
   }

   public Map<String, String> getCompactionParameters() {
      return this.compactionStrategyManager.getCompactionParams().asMap();
   }

   public String getCompactionParametersJson() {
      return FBUtilities.json(this.getCompactionParameters());
   }

   public void setCompactionParameters(Map<String, String> options) {
      try {
         CompactionParams compactionParams = CompactionParams.fromMap(options);
         compactionParams.validate();
         this.compactionStrategyManager.setNewLocalCompactionStrategy(compactionParams);
      } catch (Throwable var3) {
         logger.error("Could not set new local compaction strategy", var3);
         throw new IllegalArgumentException("Could not set new local compaction strategy: " + var3.getMessage());
      }
   }

   public void setCompactionParametersJson(String options) {
      this.setCompactionParameters(FBUtilities.fromJsonMap(options));
   }

   public Map<String, String> getCompressionParameters() {
      return this.metadata().params.compression.asMap();
   }

   public String getCompressionParametersJson() {
      return FBUtilities.json(this.getCompressionParameters());
   }

   public void setCompressionParameters(Map<String, String> opts) {
      try {
         CompressionParams params = CompressionParams.fromMap(opts);
         params.validate();
         throw new UnsupportedOperationException();
      } catch (ConfigurationException var3) {
         throw new IllegalArgumentException(var3.getMessage());
      }
   }

   public void setCompressionParametersJson(String options) {
      this.setCompressionParameters(FBUtilities.fromJsonMap(options));
   }

   @VisibleForTesting
   public ColumnFamilyStore(Keyspace keyspace, String columnFamilyName, int generation, TableMetadataRef metadata, Directories directories, boolean loadSSTables, boolean registerBookeeping, boolean offline) {
      assert directories != null;

      assert metadata != null : "null metadata for " + keyspace + ":" + columnFamilyName;

      this.keyspace = keyspace;
      this.metadata = metadata;
      this.name = columnFamilyName;
      this.minCompactionThreshold = new DefaultValue(Integer.valueOf(metadata.get().params.compaction.minCompactionThreshold()));
      this.maxCompactionThreshold = new DefaultValue(Integer.valueOf(metadata.get().params.compaction.maxCompactionThreshold()));
      this.crcCheckChance = new DefaultValue(Double.valueOf(metadata.get().params.crcCheckChance));
      this.viewManager = keyspace.viewManager.forTable(metadata.id);
      this.fileIndexGenerator.set(generation);
      this.sampleLatencyNanos = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getReadRpcTimeout() / 2L);
      logger.info("Initializing {}.{}", keyspace.getName(), this.name);
      Memtable initialMemtable = null;
      if(DatabaseDescriptor.isDaemonInitialized()) {
         initialMemtable = new Memtable(new AtomicReference(CommitLog.instance.getCurrentPosition()), this);
      }

      this.data = new Tracker(initialMemtable, loadSSTables);
      this.metric = new TableMetrics(this);
      if(this.data.loadsstables) {
         Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
         Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata);
         this.data.addInitialSSTables(sstables);
      }

      if(offline) {
         this.directories = directories;
      } else {
         this.directories = new Directories(metadata.get(), Directories.dataDirectories);
      }

      this.compactionStrategyManager = new CompactionStrategyManager(this);
      this.directories = this.compactionStrategyManager.getDirectories();
      if(((Integer)this.maxCompactionThreshold.value()).intValue() <= 0 || ((Integer)this.minCompactionThreshold.value()).intValue() <= 0) {
         logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
         this.compactionStrategyManager.disable();
      }

      this.indexManager = new SecondaryIndexManager(this);
      this.indexManager.loadIndexesAsync(this.metadata(), true);
      if(registerBookeeping) {
         this.mbeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s", new Object[]{this.isIndex()?"IndexTables":"Tables", keyspace.getName(), this.name});
         this.oldMBeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,columnfamily=%s", new Object[]{this.isIndex()?"IndexColumnFamilies":"ColumnFamilies", keyspace.getName(), this.name});

         try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName[] objectNames = new ObjectName[]{new ObjectName(this.mbeanName), new ObjectName(this.oldMBeanName)};
            ObjectName[] var12 = objectNames;
            int var13 = objectNames.length;

            for(int var14 = 0; var14 < var13; ++var14) {
               ObjectName objectName = var12[var14];
               mbs.registerMBean(this, objectName);
            }
         } catch (Exception var16) {
            throw new RuntimeException(var16);
         }

         logger.trace("retryPolicy for {} is {}", this.name, this.metadata.get().params.speculativeRetry);
         this.latencyCalculator = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(new Runnable() {
            public void run() {
               SpeculativeRetryParam retryPolicy = ColumnFamilyStore.this.metadata.get().params.speculativeRetry;
               switch (retryPolicy.kind()) {
                  case PERCENTILE: {
                     ColumnFamilyStore.this.sampleLatencyNanos = (long)ColumnFamilyStore.this.metric.coordinatorReadLatency.getSnapshot().getValue(retryPolicy.threshold());
                     break;
                  }
                  case CUSTOM: {
                     ColumnFamilyStore.this.sampleLatencyNanos = (long)retryPolicy.threshold();
                     break;
                  }
                  default: {
                     ColumnFamilyStore.this.sampleLatencyNanos = Long.MAX_VALUE;
                  }
               }
            }
         }, DatabaseDescriptor.getReadRpcTimeout(), DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);
      } else {
         this.latencyCalculator = ScheduledExecutors.optionalTasks.schedule(Runnables.doNothing(), 0L, TimeUnit.NANOSECONDS);
         this.mbeanName = null;
         this.oldMBeanName = null;
      }

   }

   public TableMetadata metadata() {
      return this.metadata.get();
   }

   public Directories getDirectories() {
      return this.directories;
   }

   public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, LifecycleTransaction txn) {
      MetadataCollector collector = (new MetadataCollector(this.metadata().comparator)).sstableLevel(sstableLevel);
      return this.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, collector, header, txn);
   }

   public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector metadataCollector, SerializationHeader header, LifecycleTransaction txn) {
      return this.getCompactionStrategyManager().createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, metadataCollector, header, this.indexManager.listIndexes(), txn);
   }

   public boolean supportsEarlyOpen() {
      return this.compactionStrategyManager.supportsEarlyOpen();
   }

   public void invalidate() {
      this.invalidate(true);
   }

   public void invalidate(boolean expectMBean) {
      if(this.valid) {
         this.valid = false;

         try {
            this.unregisterMBean();
         } catch (Exception var3) {
            if(expectMBean) {
               JVMStabilityInspector.inspectThrowable(var3);
               logger.warn("Failed unregistering mbean: {}", this.mbeanName, var3);
            }
         }

         this.latencyCalculator.cancel(false);
         this.compactionStrategyManager.shutdown();
         TPCUtils.blockingAwait(SystemKeyspace.maybeRemoveTruncationRecord(this.metadata.id));
         this.data.dropSSTables();
         LifecycleTransaction.waitForDeletions();
         this.indexManager.dropAllIndexes();
         this.invalidateCaches();
      }
   }

   void maybeRemoveUnreadableSSTables(File directory) {
      this.data.removeUnreadableSSTables(directory);
   }

   void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName[] objectNames = new ObjectName[]{new ObjectName(this.mbeanName), new ObjectName(this.oldMBeanName)};
      ObjectName[] var3 = objectNames;
      int var4 = objectNames.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         ObjectName objectName = var3[var5];
         if(mbs.isRegistered(objectName)) {
            mbs.unregisterMBean(objectName);
         }
      }

      this.metric.release();
   }

   public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, TableMetadataRef metadata, boolean loadSSTables) {
      return createColumnFamilyStore(keyspace, metadata.name, metadata, loadSSTables);
   }

   public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, TableMetadataRef metadata, boolean loadSSTables) {
      Directories directories = new Directories(metadata.get(), initialDirectories);
      return createColumnFamilyStore(keyspace, columnFamily, metadata, directories, loadSSTables, true, false);
   }

   public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, TableMetadataRef metadata, Directories directories, boolean loadSSTables, boolean registerBookkeeping, boolean offline) {
      Directories.SSTableLister lister = directories.sstableLister(Directories.OnTxnErr.IGNORE).includeBackups(true);
      List<Integer> generations = new ArrayList();
      Iterator var9 = lister.list().entrySet().iterator();

      Descriptor desc;
      do {
         if(!var9.hasNext()) {
            Collections.sort(generations);
            int value = generations.size() > 0?((Integer)generations.get(generations.size() - 1)).intValue():0;
            return new ColumnFamilyStore(keyspace, columnFamily, value, metadata, directories, loadSSTables, registerBookkeeping, offline);
         }

         Entry<Descriptor, Set<Component>> entry = (Entry)var9.next();
         desc = (Descriptor)entry.getKey();
         generations.add(Integer.valueOf(desc.generation));
      } while(desc.isCompatible());

      throw new RuntimeException(String.format("Incompatible SSTable found. Current version %s is unable to read file: %s. Please run upgradesstables.", new Object[]{desc.getFormat().getLatestVersion(), desc}));
   }


   public static void scrubDataDirectories(TableMetadata metadata) throws StartupException {
      Directories directories = new Directories(metadata, initialDirectories);
      HashSet<File> cleanedDirectories = new HashSet<File>();
      ColumnFamilyStore.clearEphemeralSnapshots(directories);
      directories.removeTemporaryDirectories();
      logger.trace("Removing temporary or obsoleted files from unfinished operations for table {}", (Object)metadata.name);
      if (!LifecycleTransaction.removeUnfinishedLeftovers(metadata)) {
         throw new StartupException(3, String.format("Cannot remove temporary or obsoleted files for %s due to a problem with transaction log files. Please check records with problems in the log messages above and fix them. Refer to the 3.0 upgrading instructions in NEWS.txt for a description of transaction log files.", metadata.toString()));
      }
      logger.trace("Further extra check for orphan sstable files for {}", (Object)metadata.name);
      for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).list().entrySet()) {
         Descriptor desc = sstableFiles.getKey();
         File directory = desc.directory;
         Set<Component> components = sstableFiles.getValue();
         if (!cleanedDirectories.contains(directory)) {
            cleanedDirectories.add(directory);
            for (File tmpFile2 : desc.getTemporaryFiles()) {
               tmpFile2.delete();
            }
         }
         File dataFile = new File(desc.filenameFor(Component.DATA));
         if (components.contains(Component.DATA) && dataFile.length() > 0L) continue;
         logger.warn("Removing orphans for {}: {}", (Object)desc, components);
         for (Component component : components) {
            File file = new File(desc.filenameFor(component));
            if (!file.exists()) continue;
            FileUtils.deleteWithConfirm(desc.filenameFor(component));
         }
      }
      Pattern tmpCacheFilePattern = Pattern.compile(metadata.keyspace + "-" + metadata.name + "-(Key|Row)Cache.*\\.tmp$");
      File dir = new File(DatabaseDescriptor.getSavedCachesLocation());
      if (dir.exists()) {
         assert (dir.isDirectory());
         for (File file : dir.listFiles()) {
            if (!tmpCacheFilePattern.matcher(file.getName()).matches() || file.delete()) continue;
            logger.warn("could not delete {}", (Object)file.getAbsolutePath());
         }
      }
      for (IndexMetadata index : metadata.indexes) {
         if (index.isCustom()) continue;
         TableMetadata indexMetadata = CassandraIndex.indexCfsMetadata(metadata, index);
         ColumnFamilyStore.scrubDataDirectories(indexMetadata);
      }
   }

   public static void loadNewSSTables(String ksName, String cfName, boolean resetLevels) {
      Keyspace keyspace = Keyspace.open(ksName);
      keyspace.getColumnFamilyStore(cfName).loadNewSSTables(resetLevels);
   }

   public synchronized void loadNewSSTables() {
      this.loadNewSSTables(false);
   }

   public synchronized void loadNewSSTables(boolean resetLevels) {
      logger.info("Loading new SSTables for {}/{}...", this.keyspace.getName(), this.name);
      Set<Descriptor> currentDescriptors = new HashSet();
      Iterator var3 = this.getSSTables(SSTableSet.CANONICAL).iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         currentDescriptors.add(sstable.descriptor);
      }

      Set<SSTableReader> newSSTables = new HashSet();
      Directories.SSTableLister lister = this.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
      Iterator var5 = lister.list().entrySet().iterator();

      while(true) {
         Entry entry;
         Descriptor descriptor;
         while(true) {
            boolean anyNotExists;
            do {
               do {
                  if(!var5.hasNext()) {
                     if(newSSTables.isEmpty()) {
                        logger.info("No new SSTables were found for {}/{}", this.keyspace.getName(), this.name);
                        return;
                     }

                     logger.info("Loading new SSTables and building secondary indexes for {}/{}: {}", new Object[]{this.keyspace.getName(), this.name, newSSTables});
                     Refs<SSTableReader> refs = Refs.ref(newSSTables);
                     Throwable var30 = null;

                     try {
                        this.data.addSSTables(newSSTables);
                     } catch (Throwable var22) {
                        var30 = var22;
                        throw var22;
                     } finally {
                        if(refs != null) {
                           if(var30 != null) {
                              try {
                                 refs.close();
                              } catch (Throwable var21) {
                                 var30.addSuppressed(var21);
                              }
                           } else {
                              refs.close();
                           }
                        }

                     }

                     logger.info("Done loading load new SSTables for {}/{}", this.keyspace.getName(), this.name);
                     return;
                  }

                  entry = (Entry)var5.next();
                  descriptor = (Descriptor)entry.getKey();
               } while(currentDescriptors.contains(descriptor));

               if(!descriptor.isCompatible()) {
                  throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s", new Object[]{descriptor.getFormat().getLatestVersion(), descriptor}));
               }

               anyNotExists = false;
               Iterator var9 = ((Set)entry.getValue()).iterator();

               while(var9.hasNext()) {
                  Component component = (Component)var9.next();
                  if(!(new File(descriptor.filenameFor(component))).exists()) {
                     logger.debug("Component {} for {} not found - ignoring sstable to add", component, descriptor.filenameFor(component));
                     anyNotExists = true;
                  }
               }
            } while(anyNotExists);

            if(!resetLevels) {
               break;
            }

            try {
               if((new File(descriptor.filenameFor(Component.STATS))).exists()) {
                  descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
               }
               break;
            } catch (IOException var24) {
               FileUtils.handleCorruptSSTable(new CorruptSSTableException(var24, ((Descriptor)entry.getKey()).filenameFor(Component.STATS)));
               logger.error("Cannot read sstable {}; other IO error, skipping table", entry, var24);
            }
         }

         Descriptor newDescriptor;
         do {
            newDescriptor = new Descriptor(descriptor.version, descriptor.directory, descriptor.ksname, descriptor.cfname, this.fileIndexGenerator.incrementAndGet(), descriptor.formatType);
         } while((new File(newDescriptor.filenameFor(Component.DATA))).exists());

         logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
         SSTableWriter.rename(descriptor, newDescriptor, (Set)entry.getValue());

         SSTableReader reader;
         try {
            reader = SSTableReader.open(newDescriptor, (Set)entry.getValue(), this.metadata);
         } catch (CorruptSSTableException var25) {
            FileUtils.handleCorruptSSTable(var25);
            logger.error("Corrupt sstable {}; skipping table", entry, var25);
            continue;
         } catch (FSError var26) {
            FileUtils.handleFSError(var26);
            logger.error("Cannot read sstable {}; file system error, skipping table", entry, var26);
            continue;
         }

         newSSTables.add(reader);
      }
   }

   public void rebuildSecondaryIndex(String idxName) {
      rebuildSecondaryIndex(this.keyspace.getName(), this.metadata.name, new String[]{idxName});
   }

   public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames) {
      ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);
      logger.info("User Requested secondary index re-build for {}/{} indexes: {}", new Object[]{ksName, cfName, Joiner.on(',').join(idxNames)});
      cfs.indexManager.rebuildIndexesBlocking(Sets.newHashSet(Arrays.asList(idxNames)));
   }

   public AbstractCompactionStrategy createCompactionStrategyInstance(CompactionParams compactionParams) {
      try {
         Constructor<? extends AbstractCompactionStrategy> constructor = compactionParams.klass().getConstructor(new Class[]{ColumnFamilyStore.class, Map.class});
         return (AbstractCompactionStrategy)constructor.newInstance(new Object[]{this, compactionParams.options()});
      } catch (IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException var3) {
         throw Throwables.cleaned(var3);
      }
   }

   /** @deprecated */
   @Deprecated
   public String getColumnFamilyName() {
      return this.getTableName();
   }

   public String getTableName() {
      return this.name;
   }

   public Descriptor newSSTableDescriptor(File directory) {
      return this.newSSTableDescriptor(directory, SSTableFormat.Type.current().info.getLatestVersion(), SSTableFormat.Type.current());
   }

   public Descriptor newSSTableDescriptor(File directory, SSTableFormat.Type format) {
      return this.newSSTableDescriptor(directory, format.info.getLatestVersion(), format);
   }

   private Descriptor newSSTableDescriptor(File directory, Version version, SSTableFormat.Type format) {
      return new Descriptor(version, directory, this.keyspace.getName(), this.name, this.fileIndexGenerator.incrementAndGet(), format);
   }

   public CompletableFuture<CommitLogPosition> switchMemtableIfCurrent(Memtable memtable) {
      Tracker var2 = this.data;
      synchronized(this.data) {
         if(this.data.getView().getCurrentMemtable() == memtable) {
            return this.switchMemtable();
         }
      }

      return this.waitForFlushes();
   }

   public CompletableFuture<CommitLogPosition> switchMemtable() {
      Tracker var1 = this.data;
      synchronized(this.data) {
         this.logFlush();
         ColumnFamilyStore.Flush flush = new ColumnFamilyStore.Flush(false);
         flushExecutor.execute(flush);
         CompletableFuture<CommitLogPosition> future = new CompletableFuture();
         postFlushExecutor.execute(() -> {
            flush.postFlushTask.run();

            try {
               future.complete(flush.postFlushTask.get());
            } catch (ExecutionException | InterruptedException var3) {
               logger.error("Unexpected exception running post flush task", var3);
               JVMStabilityInspector.inspectThrowable(var3);
               future.completeExceptionally(var3);
            }

         });
         return future;
      }
   }

   private void logFlush() {
      Memtable.MemoryUsage usage = this.getCurrentMemoryUsageIncludingIndexes();
      logger.debug("Enqueuing flush of {}: {}", this.name, String.format("%s (%.0f%%) on-heap, %s (%.0f%%) off-heap", new Object[]{FBUtilities.prettyPrintMemory(usage.ownsOnHeap), Float.valueOf(usage.ownershipRatioOnHeap * 100.0F), FBUtilities.prettyPrintMemory(usage.ownsOffHeap), Float.valueOf(usage.ownershipRatioOffHeap * 100.0F)}));
   }

   public CompletableFuture<CommitLogPosition> forceFlush() {
      Tracker var1 = this.data;
      synchronized(this.data) {
         Memtable current = this.data.getView().getCurrentMemtable();
         Iterator var3 = this.concatWithIndexes().iterator();

         ColumnFamilyStore cfs;
         do {
            if(!var3.hasNext()) {
               return this.waitForFlushes();
            }

            cfs = (ColumnFamilyStore)var3.next();
         } while(cfs.data.getView().getCurrentMemtable().isClean());

         return this.switchMemtableIfCurrent(current);
      }
   }

   public CompletableFuture<CommitLogPosition> forceFlush(CommitLogPosition flushIfDirtyBefore) {
      Memtable current = this.data.getView().getCurrentMemtable();
      return current.mayContainDataBefore(flushIfDirtyBefore)?this.switchMemtableIfCurrent(current):this.waitForFlushes();
   }

   private CompletableFuture<CommitLogPosition> waitForFlushes() {
      Memtable current = this.data.getView().getCurrentMemtable();
      CompletableFuture<CommitLogPosition> future = new CompletableFuture();
      postFlushExecutor.execute(() -> {
         logger.debug("forceFlush requested but everything is clean in {}", this.name);
         CommitLogPosition pos = current.getCommitLogLowerBound();
         future.complete(pos == null?CommitLogPosition.NONE:pos);
      });
      return future;
   }

   public CommitLogPosition forceBlockingFlush() {
      return (CommitLogPosition)TPCUtils.blockingGet(this.forceFlush());
   }

   private static void setCommitLogUpperBound(AtomicReference<CommitLogPosition> commitLogUpperBound) {
      Memtable.LastCommitLogPosition lastReplayPosition;
      CommitLogPosition currentLast;
      do {
         lastReplayPosition = new Memtable.LastCommitLogPosition(CommitLog.instance.getCurrentPosition());
         currentLast = (CommitLogPosition)commitLogUpperBound.get();
      } while(currentLast != null && currentLast.compareTo((CommitLogPosition)lastReplayPosition) > 0 || !commitLogUpperBound.compareAndSet(currentLast, lastReplayPosition));

   }

   private static String ratio(float onHeap, float offHeap) {
      return String.format("%.2f/%.2f", new Object[]{Float.valueOf(onHeap), Float.valueOf(offHeap)});
   }

   public Completable apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, CommitLogPosition commitLogPosition) {
      long start = System.nanoTime();
      Memtable mt = this.data.getMemtableFor(opGroup, commitLogPosition);
      return mt.put(update, indexer, opGroup).map((timeDelta) -> {
         DecoratedKey key = update.partitionKey();
         this.invalidateCachedPartition(key);
         ((TopKSampler)this.metric.samplers.get(TableMetrics.Sampler.WRITES)).addSample(key.getKey(), (long)key.hashCode(), 1);
         StorageHook.instance.reportWrite(this.metadata.id, update);
         this.metric.writeLatency.addNano(System.nanoTime() - start);
         if(timeDelta.longValue() < 9223372036854775807L) {
            this.metric.colUpdateTimeDeltaHistogram.update(Math.min(18165375903306L, timeDelta.longValue()));
         }

         return Integer.valueOf(0);
      }).onErrorResumeNext((e) -> {
         RuntimeException exc = new RuntimeException(e.getMessage() + " for ks: " + this.keyspace.getName() + ", table: " + this.name, e);
         return Single.error(exc);
      }).toCompletable();
   }

   public Collection<SSTableReader> getOverlappingLiveSSTables(Iterable<SSTableReader> sstables) {
      logger.trace("Checking for sstables overlapping {}", sstables);
      if(!sstables.iterator().hasNext()) {
         return ImmutableSet.of();
      } else {
         View view = this.data.getView();
         List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
         Collections.sort(sortedByFirst, (o1, o2) -> {
            return o1.first.compareTo((PartitionPosition)o2.first);
         });
         List<AbstractBounds<PartitionPosition>> bounds = new ArrayList();
         DecoratedKey first = null;
         DecoratedKey last = null;
         Iterator var7 = sortedByFirst.iterator();

         while(var7.hasNext()) {
            SSTableReader sstable = (SSTableReader)var7.next();
            if(first == null) {
               first = sstable.first;
               last = sstable.last;
            } else if(sstable.first.compareTo((PartitionPosition)last) <= 0) {
               if(sstable.last.compareTo((PartitionPosition)last) > 0) {
                  last = sstable.last;
               }
            } else {
               bounds.add(AbstractBounds.bounds(first, true, last, true));
               first = sstable.first;
               last = sstable.last;
            }
         }

         bounds.add(AbstractBounds.bounds(first, true, last, true));
         Set<SSTableReader> results = new HashSet();
         Iterator var11 = bounds.iterator();

         while(var11.hasNext()) {
            AbstractBounds<PartitionPosition> bound = (AbstractBounds)var11.next();
            Iterables.addAll(results, view.liveSSTablesInBounds((PartitionPosition)bound.left, (PartitionPosition)bound.right));
         }

         return Sets.difference(results, ImmutableSet.copyOf(sstables));
      }
   }

   public Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables) {
      Refs refs;
      do {
         Iterable<SSTableReader> overlapped = this.getOverlappingLiveSSTables(sstables);
         refs = Refs.tryRef((Iterable)overlapped);
      } while(refs == null);

      return refs;
   }

   public void addSSTable(SSTableReader sstable) {
      assert sstable.getColumnFamilyName().equals(this.name);

      this.addSSTables(Collections.singletonList(sstable));
   }

   public void addSSTables(Collection<SSTableReader> sstables) {
      this.data.addSSTables(sstables);
      CompactionManager.instance.submitBackground(this);
   }

   public void addSSTablesFromStreaming(Collection<SSTableReader> sstables) {
      this.data.addSSTablesFromStreaming(sstables);
      CompactionManager.instance.submitBackground(this);
   }

   public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operation) {
      if(operation == OperationType.CLEANUP && !this.isIndex()) {
         long expectedFileSize = 0L;
         Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(this.keyspace.getName());
         Iterator var6 = sstables.iterator();

         while(var6.hasNext()) {
            SSTableReader sstable = (SSTableReader)var6.next();
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(ranges);

            Pair position;
            for(Iterator var9 = positions.iterator(); var9.hasNext(); expectedFileSize += ((Long)position.right).longValue() - ((Long)position.left).longValue()) {
               position = (Pair)var9.next();
            }
         }

         double compressionRatio = ((Double)this.metric.compressionRatio.getValue()).doubleValue();
         if(compressionRatio > 0.0D) {
            expectedFileSize = (long)((double)expectedFileSize * compressionRatio);
         }

         return expectedFileSize;
      } else {
         return SSTableReader.getTotalBytes(sstables);
      }
   }

   public SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables) {
      long maxSize = 0L;
      SSTableReader maxFile = null;
      Iterator var5 = sstables.iterator();

      while(var5.hasNext()) {
         SSTableReader sstable = (SSTableReader)var5.next();
         if(sstable.onDiskLength() > maxSize) {
            maxSize = sstable.onDiskLength();
            maxFile = sstable;
         }
      }

      return maxFile;
   }

   public CompactionManager.AllSSTableOpStatus forceCleanup(int jobs) throws ExecutionException, InterruptedException {
      return CompactionManager.instance.performCleanup(this, jobs);
   }

   public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs) throws ExecutionException, InterruptedException {
      return this.scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, false, checkData, jobs);
   }

   @VisibleForTesting
   public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean reinsertOverflowedTTL, boolean alwaysFail, boolean checkData, int jobs) throws ExecutionException, InterruptedException {
      if(!disableSnapshot) {
         this.snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());
      }

      try {
         return CompactionManager.instance.performScrub(this, skipCorrupted, checkData, reinsertOverflowedTTL, jobs);
      } catch (Throwable var8) {
         if(!this.rebuildOnFailedScrub(var8)) {
            throw var8;
         } else {
            return alwaysFail?CompactionManager.AllSSTableOpStatus.ABORTED:CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
         }
      }
   }

   public boolean rebuildOnFailedScrub(Throwable failure) {
      if(this.isIndex() && SecondaryIndexManager.isIndexColumnFamilyStore(this)) {
         this.truncateBlocking();
         logger.warn("Rebuilding index for {} because of <{}>", this.name, failure.getMessage());
         ColumnFamilyStore parentCfs = SecondaryIndexManager.getParentCfs(this);

         assert parentCfs.indexManager.getAllIndexColumnFamilyStores().contains(this);

         String indexName = SecondaryIndexManager.getIndexName(this);
         parentCfs.rebuildSecondaryIndex(indexName);
         return true;
      } else {
         return false;
      }
   }

   public CompactionManager.AllSSTableOpStatus verify(boolean extendedVerify) throws ExecutionException, InterruptedException {
      return CompactionManager.instance.performVerify(this, extendedVerify);
   }

   public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion, int jobs) throws ExecutionException, InterruptedException {
      return CompactionManager.instance.performSSTableRewrite(this, excludeCurrentVersion, jobs);
   }

   public CompactionManager.AllSSTableOpStatus relocateSSTables(int jobs) throws ExecutionException, InterruptedException {
      return CompactionManager.instance.relocateSSTables(this, jobs);
   }

   public CompactionManager.AllSSTableOpStatus garbageCollect(CompactionParams.TombstoneOption tombstoneOption, int jobs) throws ExecutionException, InterruptedException {
      return CompactionManager.instance.performGarbageCollection(this, tombstoneOption, jobs);
   }

   public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType) {
      assert !sstables.isEmpty();

      Throwables.maybeFail(this.data.dropSSTables(Predicates.in(sstables), compactionType, (Throwable)null));
   }

   void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables) {
      this.data.replaceFlushed(memtable, sstables);
      if(sstables != null && !sstables.isEmpty()) {
         CompactionManager.instance.submitBackground(this);
      }

   }

   public boolean isValid() {
      return this.valid;
   }

   public Tracker getTracker() {
      return this.data;
   }

   public Set<SSTableReader> getLiveSSTables() {
      return this.data.getView().liveSSTables();
   }

   public Iterable<SSTableReader> getSSTables(SSTableSet sstableSet) {
      return this.data.getView().select(sstableSet);
   }

   public Iterable<SSTableReader> getUncompactingSSTables() {
      return this.data.getUncompacting();
   }

   public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter, DataLimits limits, CachedPartition cached, int nowInSec, boolean enforceStrictLiveness) {
      return cached.cachedLiveRows() < this.metadata().params.caching.rowsPerPartitionToCache()?true:filter.isHeadFilter() && limits.hasEnoughLiveData(cached, nowInSec, filter.selectsAllPartition(), enforceStrictLiveness) || filter.isFullyCoveredBy(cached);
   }

   public int gcBefore(int nowInSec) {
      return nowInSec - this.metadata().params.gcGraceSeconds;
   }

   public ColumnFamilyStore.RefViewFragment selectAndReference(com.google.common.base.Function<View, Iterable<SSTableReader>> filter) {
      long failingSince = -1L;

      while(true) {
         while(true) {
            ColumnFamilyStore.ViewFragment view = this.select(filter);
            Refs<SSTableReader> refs = Refs.tryRef((Iterable)view.sstables);
            if(refs != null) {
               return new ColumnFamilyStore.RefViewFragment(view.sstables, view.memtables, refs);
            }

            if(failingSince <= 0L) {
               failingSince = System.nanoTime();
            } else if(System.nanoTime() - failingSince > TimeUnit.MILLISECONDS.toNanos(100L)) {
               List<SSTableReader> released = new ArrayList();
               Iterator var7 = view.sstables.iterator();

               while(var7.hasNext()) {
                  SSTableReader reader = (SSTableReader)var7.next();
                  if(reader.selfRef().globalCount() == 0) {
                     released.add(reader);
                  }
               }

               NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1L, TimeUnit.SECONDS, "Spinning trying to capture readers {}, released: {}, ", new Object[]{view.sstables, released});
               failingSince = System.nanoTime();
            }
         }
      }
   }

   public ColumnFamilyStore.ViewFragment select(com.google.common.base.Function<View, Iterable<SSTableReader>> filter) {
      View view = this.data.getView();
      List<SSTableReader> sstables = Lists.newArrayList((Iterable)filter.apply(view));
      return new ColumnFamilyStore.ViewFragment(sstables, view.getAllMemtables());
   }

   public List<String> getSSTablesForKey(String key) {
      return this.getSSTablesForKey(key, false);
   }

   public List<String> getSSTablesForKey(String key, boolean hexFormat) {
      ByteBuffer keyBuffer = hexFormat ? ByteBufferUtil.hexToBytes(key) : this.metadata().partitionKeyType.fromString(key);
      DecoratedKey dk = this.decorateKey(keyBuffer);
      try (OpOrder.Group op = this.readOrdering.start();){
         ArrayList<String> files = new ArrayList<String>();
         for (SSTableReader sstr : this.select(View.select((SSTableSet)SSTableSet.LIVE, (DecoratedKey)dk)).sstables) {
            if (!sstr.contains(dk, Rebufferer.ReaderConstraint.NONE)) continue;
            files.add(sstr.getFilename());
         }
         ArrayList<String> arrayList = files;
         return arrayList;
      }
   }

   public void beginLocalSampling(String sampler, int capacity) {
      ((TopKSampler)this.metric.samplers.get(TableMetrics.Sampler.valueOf(sampler))).beginSampling(capacity);
   }

   public CompositeData finishLocalSampling(String sampler, int count) throws OpenDataException {
      TopKSampler.SamplerResult<ByteBuffer> samplerResults = ((TopKSampler)this.metric.samplers.get(TableMetrics.Sampler.valueOf(sampler))).finishSampling(count);
      TabularDataSupport result = new TabularDataSupport(COUNTER_TYPE);
      Iterator var5 = samplerResults.topK.iterator();

      while(var5.hasNext()) {
         Counter<ByteBuffer> counter = (Counter)var5.next();
         ByteBuffer key = (ByteBuffer)counter.getItem();
         result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[]{ByteBufferUtil.bytesToHex(key), Long.valueOf(counter.getCount()), Long.valueOf(counter.getError()), this.metadata().partitionKeyType.getString(key)}));
      }

      return new CompositeDataSupport(SAMPLING_RESULT, SAMPLER_NAMES, new Object[]{Long.valueOf(samplerResults.cardinality), result});
   }

   public boolean isCompactionDiskSpaceCheckEnabled() {
      return this.compactionSpaceCheck;
   }

   public void compactionDiskSpaceCheck(boolean enable) {
      this.compactionSpaceCheck = enable;
   }

   public void cleanupCache() {
      Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(this.keyspace.getName());
      Iterator keyIter = CacheService.instance.rowCache.keyIterator();

      DecoratedKey dk;
      while(keyIter.hasNext()) {
         RowCacheKey key = (RowCacheKey)keyIter.next();
         dk = this.decorateKey(ByteBuffer.wrap(key.key));
         if(key.sameTable(this.metadata()) && !Range.isInRanges(dk.getToken(), ranges)) {
            this.invalidateCachedPartition(dk);
         }
      }

      if(this.metadata().isCounter()) {
         keyIter = CacheService.instance.counterCache.keyIterator();

         while(keyIter.hasNext()) {
            CounterCacheKey key = (CounterCacheKey)keyIter.next();
            dk = this.decorateKey(key.partitionKey());
            if(key.sameTable(this.metadata()) && !Range.isInRanges(dk.getToken(), ranges)) {
               CacheService.instance.counterCache.remove(key);
            }
         }
      }

   }

   public ClusteringComparator getComparator() {
      return this.metadata().comparator;
   }

   public void snapshotWithoutFlush(String snapshotName) {
      this.snapshotWithoutFlush(snapshotName, (Predicate)null, false, new HashSet());
   }

   public Set<SSTableReader> snapshotWithoutFlush(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, Set<SSTableReader> alreadySnapshotted) {
      assert alreadySnapshotted != null;
      final Set<SSTableReader> snapshottedSSTables = new HashSet<SSTableReader>();
      for (final ColumnFamilyStore cfs : this.concatWithIndexes()) {
         final JSONArray filesJSONArr = new JSONArray();
         try (final RefViewFragment currentView = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (Predicate<SSTableReader>)(x -> predicate == null || predicate.apply(x))))) {
            for (final SSTableReader ssTable : currentView.sstables) {
               if (alreadySnapshotted.contains(ssTable)) {
                  continue;
               }
               final File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
               ssTable.createLinks(snapshotDirectory.getPath());
               filesJSONArr.add((Object)ssTable.descriptor.relativeFilenameFor(Component.DATA));
               if (ColumnFamilyStore.logger.isTraceEnabled()) {
                  ColumnFamilyStore.logger.trace("Snapshot for {} keyspace data file {} created in {}", new Object[] { this.keyspace, ssTable.getFilename(), snapshotDirectory });
               }
               snapshottedSSTables.add(ssTable);
            }
            this.writeSnapshotManifest(filesJSONArr, snapshotName);
            if (!SchemaConstants.isLocalSystemKeyspace(this.metadata.keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(this.metadata.keyspace)) {
               this.writeSnapshotSchema(snapshotName);
            }
         }
      }
      if (ephemeral) {
         this.createEphemeralSnapshotMarkerFile(snapshotName);
      }
      return snapshottedSSTables;
   }

   private void writeSnapshotManifest(JSONArray filesJSONArr, String snapshotName) {
      File manifestFile = this.getDirectories().getSnapshotManifestFile(snapshotName);

      try {
         if(!manifestFile.getParentFile().exists()) {
            manifestFile.getParentFile().mkdirs();
         }

         PrintStream out = new PrintStream(manifestFile);
         Throwable var5 = null;

         try {
            JSONObject manifestJSON = new JSONObject();
            manifestJSON.put("files", filesJSONArr);
            out.println(manifestJSON.toJSONString());
         } catch (Throwable var15) {
            var5 = var15;
            throw var15;
         } finally {
            if(out != null) {
               if(var5 != null) {
                  try {
                     out.close();
                  } catch (Throwable var14) {
                     var5.addSuppressed(var14);
                  }
               } else {
                  out.close();
               }
            }

         }

      } catch (IOException var17) {
         throw new FSWriteError(var17, manifestFile);
      }
   }

   private void writeSnapshotSchema(String snapshotName) {
      final File schemaFile = this.getDirectories().getSnapshotSchemaFile(snapshotName);
      try {
         if (!schemaFile.getParentFile().exists()) {
            schemaFile.getParentFile().mkdirs();
         }
         try (final PrintStream out = new PrintStream(schemaFile)) {
            for (final String s : ColumnFamilyStoreCQLHelper.dumpReCreateStatements(this.metadata())) {
               out.println(s);
            }
         }
      }
      catch (IOException e) {
         throw new FSWriteError(e, schemaFile);
      }
   }

   private void createEphemeralSnapshotMarkerFile(String snapshot) {
      File ephemeralSnapshotMarker = this.getDirectories().getNewEphemeralSnapshotMarkerFile(snapshot);

      try {
         if(!ephemeralSnapshotMarker.getParentFile().exists()) {
            ephemeralSnapshotMarker.getParentFile().mkdirs();
         }

         Files.createFile(ephemeralSnapshotMarker.toPath(), new FileAttribute[0]);
         logger.trace("Created ephemeral snapshot marker file on {}.", ephemeralSnapshotMarker.getAbsolutePath());
      } catch (IOException var4) {
         logger.warn(String.format("Could not create marker file %s for ephemeral snapshot %s. In case there is a failure in the operation that created this snapshot, you may need to clean it manually afterwards.", new Object[]{ephemeralSnapshotMarker.getAbsolutePath(), snapshot}), var4);
      }

   }

   protected static void clearEphemeralSnapshots(Directories directories) {
      Iterator var1 = directories.listEphemeralSnapshots().iterator();

      while(var1.hasNext()) {
         String ephemeralSnapshot = (String)var1.next();
         logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot);
         Directories.clearSnapshot(ephemeralSnapshot, directories.getCFDirectories());
      }

   }

   public Refs<SSTableReader> getSnapshotSSTableReaders(String tag) throws IOException {
      Map<Integer, SSTableReader> active = new HashMap();
      Iterator var3 = this.getSSTables(SSTableSet.CANONICAL).iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         active.put(Integer.valueOf(sstable.descriptor.generation), sstable);
      }

      Map<Descriptor, Set<Component>> snapshots = this.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
      Refs refs = new Refs();

      try {
         Iterator var5 = snapshots.entrySet().iterator();

         while(true) {
            while(var5.hasNext()) {
               Entry<Descriptor, Set<Component>> entries = (Entry)var5.next();
               SSTableReader sstable = (SSTableReader)active.get(Integer.valueOf(((Descriptor)entries.getKey()).generation));
               if(sstable != null && refs.tryRef((RefCounted)sstable)) {
                  if(logger.isTraceEnabled()) {
                     logger.trace("using active sstable {}", entries.getKey());
                  }
               } else {
                  if(logger.isTraceEnabled()) {
                     logger.trace("using snapshot sstable {}", entries.getKey());
                  }

                  sstable = SSTableReader.open((Descriptor)entries.getKey(), (Set)entries.getValue(), this.metadata, true, false);
                  refs.tryRef((RefCounted)sstable);
                  sstable.selfRef().release();
               }
            }

            return refs;
         }
      } catch (RuntimeException | FSReadError var8) {
         refs.release();
         throw var8;
      }
   }

   public Set<SSTableReader> snapshot(String snapshotName) {
      return this.snapshot(snapshotName, (Predicate)null, false, false, new HashSet());
   }

   public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipFlush, Set<SSTableReader> alreadySnapshotted) {
      if(!skipFlush) {
         this.forceBlockingFlush();
      }

      return this.snapshotWithoutFlush(snapshotName, predicate, ephemeral, alreadySnapshotted);
   }

   public boolean snapshotExists(String snapshotName) {
      return this.getDirectories().snapshotExists(snapshotName);
   }

   public long getSnapshotCreationTime(String snapshotName) {
      return this.getDirectories().snapshotCreationTime(snapshotName);
   }

   public void clearSnapshot(String snapshotName) {
      List<File> snapshotDirs = this.getDirectories().getCFDirectories();
      Directories.clearSnapshot(snapshotName, snapshotDirs);
   }

   public Map<String, Pair<Long, Long>> getSnapshotDetails() {
      return this.getDirectories().getSnapshotDetails();
   }

   public CachedPartition getRawCachedPartition(DecoratedKey key) {
      if(!this.isRowCacheEnabled()) {
         return null;
      } else {
         IRowCacheEntry cached = (IRowCacheEntry)CacheService.instance.rowCache.getInternal(new RowCacheKey(this.metadata(), key));
         return cached != null && !(cached instanceof RowCacheSentinel)?(CachedPartition)cached:null;
      }
   }

   private void invalidateCaches() {
      CacheService.instance.invalidateKeyCacheForCf(this.metadata());
      CacheService.instance.invalidateRowCacheForCf(this.metadata());
      if(this.metadata().isCounter()) {
         CacheService.instance.invalidateCounterCacheForCf(this.metadata());
      }

   }

   public int invalidateRowCache(Collection<Bounds<Token>> boundsToInvalidate) {
      int invalidatedKeys = 0;
      Iterator keyIter = CacheService.instance.rowCache.keyIterator();

      while(keyIter.hasNext()) {
         RowCacheKey key = (RowCacheKey)keyIter.next();
         DecoratedKey dk = this.decorateKey(ByteBuffer.wrap(key.key));
         if(key.sameTable(this.metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate)) {
            this.invalidateCachedPartition(dk);
            ++invalidatedKeys;
         }
      }

      return invalidatedKeys;
   }

   public int invalidateCounterCache(Collection<Bounds<Token>> boundsToInvalidate) {
      int invalidatedKeys = 0;
      Iterator keyIter = CacheService.instance.counterCache.keyIterator();

      while(keyIter.hasNext()) {
         CounterCacheKey key = (CounterCacheKey)keyIter.next();
         DecoratedKey dk = this.decorateKey(key.partitionKey());
         if(key.sameTable(this.metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate)) {
            CacheService.instance.counterCache.remove(key);
            ++invalidatedKeys;
         }
      }

      return invalidatedKeys;
   }

   public boolean containsCachedParition(DecoratedKey key) {
      return CacheService.instance.rowCache.getCapacity() != 0L && CacheService.instance.rowCache.containsKey(new RowCacheKey(this.metadata(), key));
   }

   public void invalidateCachedPartition(RowCacheKey key) {
      CacheService.instance.rowCache.remove(key);
   }

   public void invalidateCachedPartition(DecoratedKey key) {
      if(this.isRowCacheEnabled()) {
         this.invalidateCachedPartition(new RowCacheKey(this.metadata(), key));
      }
   }

   public ClockAndCount getCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnMetadata column, CellPath path) {
      return CacheService.instance.counterCache.getCapacity() == 0L?null:(ClockAndCount)CacheService.instance.counterCache.get(CounterCacheKey.create(this.metadata(), partitionKey, clustering, column, path));
   }

   public void putCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnMetadata column, CellPath path, ClockAndCount clockAndCount) {
      if(CacheService.instance.counterCache.getCapacity() != 0L) {
         CacheService.instance.counterCache.put(CounterCacheKey.create(this.metadata(), partitionKey, clustering, column, path), clockAndCount);
      }
   }

   public void forceMajorCompaction() throws InterruptedException, ExecutionException {
      this.forceMajorCompaction(false);
   }

   public void forceMajorCompaction(boolean splitOutput) throws InterruptedException, ExecutionException {
      CompactionManager.instance.performMaximal(this, splitOutput);
   }

   public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException {
      CompactionManager.instance.forceCompactionForTokenRange(this, tokenRanges);
   }

   public static Iterable<ColumnFamilyStore> all() {
      List<Iterable<ColumnFamilyStore>> stores = new ArrayList(Schema.instance.getKeyspaces().size());
      Iterator var1 = Keyspace.all().iterator();

      while(var1.hasNext()) {
         Keyspace keyspace = (Keyspace)var1.next();
         stores.add(keyspace.getColumnFamilyStores());
      }

      return Iterables.concat(stores);
   }

   public Iterable<DecoratedKey> keySamples(Range<Token> range) {
      ColumnFamilyStore.RefViewFragment view = this.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
      Throwable var3 = null;

      try {
         Iterable<DecoratedKey>[] samples = new Iterable[view.sstables.size()];
         int i = 0;

         SSTableReader sstable;
         for(Iterator var6 = view.sstables.iterator(); var6.hasNext(); samples[i++] = sstable.getKeySamples(range)) {
            sstable = (SSTableReader)var6.next();
         }

         Iterable var17 = Iterables.concat(samples);
         return var17;
      } catch (Throwable var15) {
         var3 = var15;
         throw var15;
      } finally {
         if(view != null) {
            if(var3 != null) {
               try {
                  view.close();
               } catch (Throwable var14) {
                  var3.addSuppressed(var14);
               }
            } else {
               view.close();
            }
         }

      }
   }

   public long estimatedKeysForRange(Range<Token> range) {
      ColumnFamilyStore.RefViewFragment view = this.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
      Throwable var3 = null;

      try {
         long count = 0L;

         SSTableReader sstable;
         for(Iterator var6 = view.sstables.iterator(); var6.hasNext(); count += sstable.estimatedKeysForRanges(Collections.singleton(range))) {
            sstable = (SSTableReader)var6.next();
         }

         long var18 = count;
         return var18;
      } catch (Throwable var16) {
         var3 = var16;
         throw var16;
      } finally {
         if(view != null) {
            if(var3 != null) {
               try {
                  view.close();
               } catch (Throwable var15) {
                  var3.addSuppressed(var15);
               }
            } else {
               view.close();
            }
         }

      }
   }

   @VisibleForTesting
   public void clearUnsafe() {
      Iterator var1 = this.concatWithIndexes().iterator();

      while(var1.hasNext()) {
         final ColumnFamilyStore cfs = (ColumnFamilyStore)var1.next();
         cfs.runWithCompactionsDisabled(new Callable<Void>() {
            public Void call() {
               cfs.data.reset(new Memtable(new AtomicReference(CommitLogPosition.NONE), cfs));
               return null;
            }
         }, true, false);
      }

   }

   public void truncateBlocking() {
      this.truncateBlocking(DatabaseDescriptor.isAutoSnapshot());
   }

   public void truncateBlocking(final boolean snapshot) {
      ColumnFamilyStore.logger.info("Truncating {}.{}", (Object)this.keyspace.getName(), (Object)this.name);
      CommitLogPosition replayAfter;
      if (this.keyspace.getMetadata().params.durableWrites || snapshot) {
         replayAfter = this.forceBlockingFlush();
         this.viewManager.forceBlockingFlush();
      }
      else {
         this.viewManager.dumpMemtables();
         try {
            replayAfter = this.dumpMemtable().get();
         }
         catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      long now = System.currentTimeMillis();
      for (final ColumnFamilyStore cfs : this.concatWithIndexes()) {
         for (final SSTableReader sstable : cfs.getLiveSSTables()) {
            now = Math.max(now, sstable.maxDataAge);
         }
      }
      final long truncatedAt = now;
      final Runnable truncateRunnable = new Runnable() {
         @Override
         public void run() {
            ColumnFamilyStore.logger.debug("Discarding sstable data for truncated CF + indexes");
            ColumnFamilyStore.this.data.notifyTruncated(truncatedAt);
            if (snapshot) {
               ColumnFamilyStore.this.snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(ColumnFamilyStore.this.name, "truncated"));
            }
            ColumnFamilyStore.this.discardSSTables(truncatedAt);
            ColumnFamilyStore.this.indexManager.truncateAllIndexesBlocking(truncatedAt);
            ColumnFamilyStore.this.viewManager.truncateBlocking(replayAfter, truncatedAt);
            TPCUtils.blockingAwait(SystemKeyspace.saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter));
            ColumnFamilyStore.logger.trace("cleaning out row cache");
            ColumnFamilyStore.this.invalidateCaches();
         }
      };
      this.runWithCompactionsDisabled(Executors.callable(truncateRunnable), true);
      ColumnFamilyStore.logger.info("Truncate of {}.{} is complete", (Object)this.keyspace.getName(), (Object)this.name);
   }

   public Future<CommitLogPosition> dumpMemtable() {
      Tracker var1 = this.data;
      synchronized(this.data) {
         ColumnFamilyStore.Flush flush = new ColumnFamilyStore.Flush(true);
         flushExecutor.execute(flush);
         postFlushExecutor.execute(flush.postFlushTask);
         return flush.postFlushTask;
      }
   }

   /** @deprecated */
   @Deprecated
   public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews) {
      return this.runWithCompactionsDisabled(callable, interruptValidation?Predicates.alwaysTrue():OperationType.EXCEPT_VALIDATIONS, Predicates.alwaysTrue(), interruptViews);
   }

   public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptViews) {
      return this.runWithCompactionsDisabled(callable, Predicates.alwaysTrue(), Predicates.alwaysTrue(), interruptViews);
   }

   public <V> V runWithCompactionsDisabled(Callable<V> callable, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate, boolean interruptViews) {
      synchronized (this) {
         ColumnFamilyStore.logger.debug("Cancelling in-progress compactions for {}", (Object)this.metadata.name);
         final Iterable<ColumnFamilyStore> selfWithAuxiliaryCfs = interruptViews ? Iterables.concat((Iterable)this.concatWithIndexes(), (Iterable)this.viewManager.allViewsCfs()) : this.concatWithIndexes();
         for (final ColumnFamilyStore cfs : selfWithAuxiliaryCfs) {
            cfs.getCompactionStrategyManager().pause();
         }
         try {
            CompactionManager.instance.interruptCompactionForCFs(selfWithAuxiliaryCfs, opPredicate, readerPredicate);
            CompactionManager.instance.waitForCessation(selfWithAuxiliaryCfs, opPredicate, readerPredicate);
            for (final ColumnFamilyStore cfs : selfWithAuxiliaryCfs) {
               if (CompactionManager.instance.isCompacting((Iterable<ColumnFamilyStore>)ImmutableList.of(cfs), opPredicate, readerPredicate)) {
                  ColumnFamilyStore.logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", (Object)this.metadata.name);
                  return null;
               }
            }
            ColumnFamilyStore.logger.debug("Compactions successfully cancelled");
            try {
               return callable.call();
            }
            catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
         finally {
            for (final ColumnFamilyStore cfs2 : selfWithAuxiliaryCfs) {
               cfs2.getCompactionStrategyManager().resume();
            }
         }
      }
   }

   public LifecycleTransaction markAllCompacting(final OperationType operationType) {
      Callable<LifecycleTransaction> callable = new Callable<LifecycleTransaction>() {
         public LifecycleTransaction call() {
            assert ColumnFamilyStore.this.data.getCompacting().isEmpty() : ColumnFamilyStore.this.data.getCompacting();

            Iterable<SSTableReader> sstablesx = ColumnFamilyStore.this.getLiveSSTables();
            Iterable<SSTableReader> sstables = AbstractCompactionStrategy.filterSuspectSSTables(sstablesx);
            Iterable<SSTableReader> sstablesxx = ImmutableList.copyOf(sstables);
            LifecycleTransaction modifier = ColumnFamilyStore.this.data.tryModify((Iterable)sstablesxx, operationType);

            assert modifier != null : "something marked things compacting while compactions are disabled";

            return modifier;
         }
      };
      return (LifecycleTransaction)this.runWithCompactionsDisabled(callable, false, false);
   }

   public String toString() {
      return "CFS(Keyspace='" + this.keyspace.getName() + '\'' + ", ColumnFamily='" + this.name + '\'' + ')';
   }

   public void disableAutoCompaction() {
      this.compactionStrategyManager.disable();
   }

   public void enableAutoCompaction() {
      this.enableAutoCompaction(false);
   }

   @VisibleForTesting
   public void enableAutoCompaction(boolean waitForFutures) {
      this.compactionStrategyManager.enable();
      List<Future<?>> futures = CompactionManager.instance.submitBackground(this);
      if(waitForFutures) {
         FBUtilities.waitOnFutures(futures);
      }

   }

   public boolean isAutoCompactionDisabled() {
      return !this.compactionStrategyManager.isEnabled();
   }

   public CompactionStrategyManager getCompactionStrategyManager() {
      return this.compactionStrategyManager;
   }

   public void setCrcCheckChance(double crcCheckChance) {
      try {
         TableParams.builder().crcCheckChance(crcCheckChance).build().validate();
         Iterator var3 = this.concatWithIndexes().iterator();

         while(var3.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
            cfs.crcCheckChance.set(Double.valueOf(crcCheckChance));
            Iterator var5 = cfs.getSSTables(SSTableSet.LIVE).iterator();

            while(var5.hasNext()) {
               SSTableReader sstable = (SSTableReader)var5.next();
               sstable.setCrcCheckChance(crcCheckChance);
            }
         }

      } catch (ConfigurationException var7) {
         throw new IllegalArgumentException(var7.getMessage());
      }
   }

   public Double getCrcCheckChance() {
      return (Double)this.crcCheckChance.value();
   }

   public void setCompactionThresholds(int minThreshold, int maxThreshold) {
      this.validateCompactionThresholds(minThreshold, maxThreshold);
      this.minCompactionThreshold.set(Integer.valueOf(minThreshold));
      this.maxCompactionThreshold.set(Integer.valueOf(maxThreshold));
      CompactionManager.instance.submitBackground(this);
   }

   public int getMinimumCompactionThreshold() {
      return ((Integer)this.minCompactionThreshold.value()).intValue();
   }

   public void setMinimumCompactionThreshold(int minCompactionThreshold) {
      this.validateCompactionThresholds(minCompactionThreshold, ((Integer)this.maxCompactionThreshold.value()).intValue());
      this.minCompactionThreshold.set(Integer.valueOf(minCompactionThreshold));
   }

   public int getMaximumCompactionThreshold() {
      return ((Integer)this.maxCompactionThreshold.value()).intValue();
   }

   public void setMaximumCompactionThreshold(int maxCompactionThreshold) {
      this.validateCompactionThresholds(((Integer)this.minCompactionThreshold.value()).intValue(), maxCompactionThreshold);
      this.maxCompactionThreshold.set(Integer.valueOf(maxCompactionThreshold));
   }

   private void validateCompactionThresholds(int minThreshold, int maxThreshold) {
      if(minThreshold > maxThreshold) {
         throw new RuntimeException(String.format("The min_compaction_threshold cannot be larger than the max_compaction_threshold. Min is '%d', Max is '%d'.", new Object[]{Integer.valueOf(minThreshold), Integer.valueOf(maxThreshold)}));
      } else if(maxThreshold == 0 || minThreshold == 0) {
         throw new RuntimeException("Disabling compaction by setting min_compaction_threshold or max_compaction_threshold to 0 is deprecated, set the compaction strategy option 'enabled' to 'false' instead or use the nodetool command 'disableautocompaction'.");
      }
   }

   /** @deprecated */
   @Deprecated
   public int getMeanColumns() {
      return this.getMeanCells();
   }

   public int getMeanCells() {
      long sum = 0L;
      long count = 0L;

      long n;
      for(Iterator var5 = this.getSSTables(SSTableSet.CANONICAL).iterator(); var5.hasNext(); count += n) {
         SSTableReader sstable = (SSTableReader)var5.next();
         n = sstable.getEstimatedColumnCount().count();
         sum += sstable.getEstimatedColumnCount().mean() * n;
      }

      return count > 0L?(int)(sum / count):0;
   }

   public double getMeanPartitionSize() {
      long sum = 0L;
      long count = 0L;

      long n;
      for(Iterator var5 = this.getSSTables(SSTableSet.CANONICAL).iterator(); var5.hasNext(); count += n) {
         SSTableReader sstable = (SSTableReader)var5.next();
         n = sstable.getEstimatedPartitionSize().count();
         sum += sstable.getEstimatedPartitionSize().mean() * n;
      }

      return count > 0L?(double)sum * 1.0D / (double)count:0.0D;
   }

   public int getAverageRowSize() {
      long sum = 0L;
      long count = 0L;

      for(Iterator var5 = this.getSSTables(SSTableSet.CANONICAL).iterator(); var5.hasNext(); ++count) {
         SSTableReader sstable = (SSTableReader)var5.next();
         sum += sstable.uncompressedLength() / sstable.getTotalRows();
      }

      return (int)(count == 0L?0L:sum / count);
   }

   public long estimateKeys() {
      long n = 0L;

      SSTableReader sstable;
      for(Iterator var3 = this.getSSTables(SSTableSet.CANONICAL).iterator(); var3.hasNext(); n += sstable.estimatedKeys()) {
         sstable = (SSTableReader)var3.next();
      }

      return n;
   }

   public IPartitioner getPartitioner() {
      return this.metadata().partitioner;
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return this.getPartitioner().decorateKey(key);
   }

   public boolean isIndex() {
      return this.metadata().isIndex();
   }

   public Iterable<ColumnFamilyStore> concatWithIndexes() {
      return Iterables.concat(Collections.singleton(this), this.indexManager.getAllIndexColumnFamilyStores());
   }

   public Memtable.MemoryUsage getMemoryUsage() {
      Memtable.MemoryUsage usage = new Memtable.MemoryUsage();
      Iterator var2 = this.data.getView().getAllMemtables().iterator();

      while(var2.hasNext()) {
         Memtable m = (Memtable)var2.next();
         m.addMemoryUsage(usage);
      }

      return usage;
   }

   public Memtable.MemoryUsage getMemoryUsageIncludingIndexes() {
      Memtable.MemoryUsage usage = this.getMemoryUsage();
      Iterator var2 = this.indexManager.getAllIndexColumnFamilyStores().iterator();

      while(var2.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var2.next();
         Iterator var4 = cfs.data.getView().getAllMemtables().iterator();

         while(var4.hasNext()) {
            Memtable m = (Memtable)var4.next();
            m.addMemoryUsage(usage);
         }
      }

      return usage;
   }

   private Memtable.MemoryUsage getCurrentMemoryUsageIncludingIndexes() {
      Memtable.MemoryUsage usage = this.getTracker().getView().getCurrentMemtable().getMemoryUsage();
      Iterator var2 = this.indexManager.getAllIndexColumnFamilyStores().iterator();

      while(var2.hasNext()) {
         ColumnFamilyStore indexCfs = (ColumnFamilyStore)var2.next();
         indexCfs.getTracker().getView().getCurrentMemtable().addMemoryUsage(usage);
      }

      return usage;
   }

   public List<String> getBuiltIndexes() {
      return this.indexManager.getBuiltIndexNamesBlocking();
   }

   public int getUnleveledSSTables() {
      return this.compactionStrategyManager.getUnleveledSSTables();
   }

   public int[] getSSTableCountPerLevel() {
      return this.compactionStrategyManager.getSSTableCountPerLevel();
   }

   public int getLevelFanoutSize() {
      return this.compactionStrategyManager.getLevelFanoutSize();
   }

   public boolean isEmpty() {
      return this.data.getView().isEmpty();
   }

   public boolean isRowCacheEnabled() {
      boolean retval = this.metadata().params.caching.cacheRows() && CacheService.instance.rowCache.getCapacity() > 0L;

      assert !retval || !this.isIndex();

      return retval;
   }

   public boolean isCounterCacheEnabled() {
      return this.metadata().isCounter() && CacheService.instance.counterCache.getCapacity() > 0L;
   }

   public boolean isKeyCacheEnabled() {
      return this.metadata().params.caching.cacheKeys() && CacheService.instance.keyCache.getCapacity() > 0L;
   }

   public void discardSSTables(long truncatedAt) {
      assert this.data.getCompacting().isEmpty() : this.data.getCompacting();

      List<SSTableReader> truncatedSSTables = new ArrayList();
      Iterator var4 = this.getSSTables(SSTableSet.LIVE).iterator();

      while(var4.hasNext()) {
         SSTableReader sstable = (SSTableReader)var4.next();
         if(!sstable.newSince(truncatedAt)) {
            truncatedSSTables.add(sstable);
         }
      }

      if(!truncatedSSTables.isEmpty()) {
         this.markObsolete(truncatedSSTables, OperationType.UNKNOWN);
      }

   }

   public double getDroppableTombstoneRatio() {
      double allDroppable = 0.0D;
      long allColumns = 0L;
      int localTime = (int)(System.currentTimeMillis() / 1000L);

      SSTableReader sstable;
      for(Iterator var6 = this.getSSTables(SSTableSet.LIVE).iterator(); var6.hasNext(); allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count()) {
         sstable = (SSTableReader)var6.next();
         allDroppable += sstable.getDroppableTombstonesBefore(localTime - this.metadata().params.gcGraceSeconds);
      }

      return allColumns > 0L?allDroppable / (double)allColumns:0.0D;
   }

   public long trueSnapshotsSize() {
      return this.getDirectories().trueSnapshotsSize();
   }

   @VisibleForTesting
   void resetFileIndexGenerator() {
      this.fileIndexGenerator.set(0);
   }

   public static ColumnFamilyStore getIfExists(TableId id) {
      TableMetadata metadata = Schema.instance.getTableMetadata(id);
      if(metadata == null) {
         return null;
      } else {
         Keyspace keyspace = Keyspace.open(metadata.keyspace);
         return keyspace == null?null:(keyspace.hasColumnFamilyStore(id)?keyspace.getColumnFamilyStore(id):null);
      }
   }

   public static ColumnFamilyStore getIfExists(String ksName, String cfName) {
      if(ksName != null && cfName != null) {
         KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(ksName);
         if(keyspace == null) {
            return null;
         } else {
            TableMetadata table = Schema.instance.getTableMetadata(ksName, cfName);
            return table == null?null:Keyspace.open(ksName).getColumnFamilyStore(table.id);
         }
      } else {
         return null;
      }
   }

   public static TableMetrics metricsFor(TableId tableId) {
      return getIfExists(tableId).metric;
   }

   public long getMemtablesLiveSize() {
      return StreamSupport.stream(this.data.getView().getAllMemtables().spliterator(), false).mapToLong(Memtable::getLiveDataSize).sum();
   }

   public boolean hasViews() {
      return !this.viewManager.isEmpty();
   }

   public DiskBoundaries getDiskBoundaries() {
      return this.diskBoundaryManager.getDiskBoundaries(this, this.directories);
   }

   public DiskBoundaries getDiskBoundaries(Directories directories) {
      return this.diskBoundaryManager.getDiskBoundaries(this, directories);
   }

   public void invalidateDiskBoundaries() {
      this.diskBoundaryManager.invalidate();
   }

   public boolean isCdcEnabled() {
      return this.metadata.get().params.cdc;
   }

   public TableInfo getTableInfo() {
      return new TableInfo(this.hasViews(), this.metadata.get().isView(), this.getLiveSSTables().stream().anyMatch((s) -> {
         return s.isRepaired();
      }), this.isCdcEnabled());
   }

   public int forceMarkAllSSTablesAsUnrepaired() {
      return ((Integer)this.runWithCompactionsDisabled(() -> {
         Set<SSTableReader> repairedSSTables = (Set)this.getLiveSSTables().stream().filter(SSTableReader::isRepaired).collect(Collectors.toSet());
         int mutated = this.getCompactionStrategyManager().mutateRepaired(repairedSSTables, 0L, (UUID)null);
         logger.debug("Marked {} sstables from table {}.{} as unrepaired.", new Object[]{Integer.valueOf(mutated), this.keyspace.getName(), this.name});
         return Integer.valueOf(mutated);
      }, true)).intValue();
   }

   public void logStartupWarnings() {
      if(TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(this.metadata(), this.metadata().params)) {
         logger.warn(TimeWindowCompactionStrategy.getNodeSyncSplitDuringFlushWarning(this.keyspace.getName(), this.name));
      }

   }

   static {
      initialDirectories = Directories.dataDirectories;
      logger = LoggerFactory.getLogger(ColumnFamilyStore.class);
      flushExecutor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(), 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("MemtableFlushWriter"), "internal");
      perDiskflushExecutors = new ExecutorService[DatabaseDescriptor.getAllDataFileLocations().length];

      for(int i = 0; i < DatabaseDescriptor.getAllDataFileLocations().length; ++i) {
         perDiskflushExecutors[i] = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(), 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("PerDiskMemtableFlushWriter_" + i), "internal");
      }

      postFlushExecutor = new JMXEnabledThreadPoolExecutor(1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("MemtablePostFlush"), "internal");
      reclaimExecutor = new JMXEnabledThreadPoolExecutor(1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("MemtableReclaimMemory"), "internal");
      COUNTER_NAMES = new String[]{"raw", "count", "error", "string"};
      COUNTER_DESCS = new String[]{"partition key in raw hex bytes", "value of this partition for given sampler", "value is within the error bounds plus or minus of this", "the partition key turned into a human readable format"};
      SAMPLER_NAMES = new String[]{"cardinality", "partitions"};
      SAMPLER_DESCS = new String[]{"cardinality of partitions", "list of counter results"};

      try {
         OpenType<?>[] counterTypes = new OpenType[]{SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING};
         COUNTER_COMPOSITE_TYPE = new CompositeType("SAMPLING_RESULTS", "SAMPLING_RESULTS", COUNTER_NAMES, COUNTER_DESCS, counterTypes);
         COUNTER_TYPE = new TabularType("SAMPLING_RESULTS", "SAMPLING_RESULTS", COUNTER_COMPOSITE_TYPE, COUNTER_NAMES);
         OpenType<?>[] samplerTypes = new OpenType[]{SimpleType.LONG, COUNTER_TYPE};
         SAMPLING_RESULT = new CompositeType("SAMPLING_RESULTS", "SAMPLING_RESULTS", SAMPLER_NAMES, SAMPLER_DESCS, samplerTypes);
      } catch (OpenDataException var2) {
         throw com.google.common.base.Throwables.propagate(var2);
      }
   }

   public static class RefViewFragment extends ColumnFamilyStore.ViewFragment implements AutoCloseable {
      public final Refs<SSTableReader> refs;

      public RefViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables, Refs<SSTableReader> refs) {
         super(sstables, memtables);
         this.refs = refs;
      }

      public void release() {
         this.refs.release();
      }

      public void close() {
         this.refs.release();
      }
   }

   public static class ViewFragment {
      public final List<SSTableReader> sstables;
      public final Iterable<Memtable> memtables;

      public ViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables) {
         this.sstables = sstables;
         this.memtables = memtables;
      }
   }

   public static class FlushLargestColumnFamily implements Runnable {
      public FlushLargestColumnFamily() {
      }

      public void run() {
         float largestRatio = 0.0F;
         Memtable largest = null;
         float liveOnHeap = 0.0F;
         float liveOffHeap = 0.0F;

         Memtable.MemoryUsage usage;
         for(Iterator var5 = ColumnFamilyStore.all().iterator(); var5.hasNext(); liveOffHeap += usage.ownershipRatioOffHeap) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
            Memtable current = cfs.getTracker().getView().getCurrentMemtable();
            usage = cfs.getCurrentMemoryUsageIncludingIndexes();
            float ratio = Math.max(usage.ownershipRatioOnHeap, usage.ownershipRatioOffHeap);
            if(ratio > largestRatio) {
               largest = current;
               largestRatio = ratio;
            }

            liveOnHeap += usage.ownershipRatioOnHeap;
         }

         if(largest != null) {
            float usedOnHeap = Memtable.MEMORY_POOL.onHeap.usedRatio();
            float usedOffHeap = Memtable.MEMORY_POOL.offHeap.usedRatio();
            float flushingOnHeap = Memtable.MEMORY_POOL.onHeap.reclaimingRatio();
            float flushingOffHeap = Memtable.MEMORY_POOL.offHeap.reclaimingRatio();
            Memtable.MemoryUsage thisUsage = largest.getMemoryUsage();
            ColumnFamilyStore.logger.debug("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}", new Object[]{largest.cfs, ColumnFamilyStore.ratio(usedOnHeap, usedOffHeap), ColumnFamilyStore.ratio(liveOnHeap, liveOffHeap), ColumnFamilyStore.ratio(flushingOnHeap, flushingOffHeap), ColumnFamilyStore.ratio(thisUsage.ownershipRatioOnHeap, thisUsage.ownershipRatioOffHeap)});
            largest.cfs.switchMemtableIfCurrent(largest);
         }

      }
   }

   private final class Flush implements Runnable {
      final OpOrder.Barrier writeBarrier;
      final List<Memtable> memtables;
      final ListenableFutureTask<CommitLogPosition> postFlushTask;
      final ColumnFamilyStore.PostFlush postFlush;
      final boolean truncate;

      private Flush(boolean truncate) {
         this.memtables = new ArrayList();
         this.truncate = truncate;
         ColumnFamilyStore.this.metric.pendingFlushes.inc();
         Keyspace var10001 = ColumnFamilyStore.this.keyspace;
         this.writeBarrier = Keyspace.writeOrder.newBarrier();
         AtomicReference<CommitLogPosition> commitLogUpperBound = new AtomicReference();
         Iterator var4 = ColumnFamilyStore.this.concatWithIndexes().iterator();

         while(var4.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
            Memtable newMemtable = new Memtable(commitLogUpperBound, cfs);
            Memtable oldMemtable = cfs.data.switchMemtable(truncate, newMemtable);
            oldMemtable.setDiscarding(this.writeBarrier, commitLogUpperBound);
            this.memtables.add(oldMemtable);
         }

         ColumnFamilyStore.setCommitLogUpperBound(commitLogUpperBound);
         this.writeBarrier.issue();
         this.postFlush = ColumnFamilyStore.this.new PostFlush(this.memtables);
         this.postFlushTask = ListenableFutureTask.create(this.postFlush);
      }

      public void run() {
         this.writeBarrier.markBlocking();
         this.writeBarrier.await();
         Iterator var1 = this.memtables.iterator();

         while(var1.hasNext()) {
            Memtable memtable = (Memtable)var1.next();
            memtable.cfs.data.markFlushing(memtable);
         }

         ColumnFamilyStore.this.metric.memtableSwitchCount.inc();

         try {
            this.flushMemtable((Memtable)this.memtables.get(0), true);

            for(int i = 1; i < this.memtables.size(); ++i) {
               this.flushMemtable((Memtable)this.memtables.get(i), false);
            }
         } catch (Throwable var3) {
            JVMStabilityInspector.inspectThrowable(var3);
            this.postFlush.flushFailure = var3;
         }

         this.postFlush.latch.countDown();
      }

      public Collection<SSTableReader> flushMemtable(Memtable memtable, boolean flushNonCf2i) {
         long start = System.currentTimeMillis();
         if(!memtable.isEmpty() && !this.truncate) {
            List<Future<SSTableMultiWriter>> futures = new ArrayList();
            long totalBytesOnDisk = 0L;
            long maxBytesOnDisk = 0L;
            long minBytesOnDisk = 9223372036854775807L;
            List<SSTableReader> sstables = new ArrayList();
            LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH);
            Throwable var14 = null;

            try {
               List<Memtable.FlushRunnable> flushRunnables = null;
               ArrayList flushResults = null;

               Throwable accumulate;
               Iterator var18;
               try {
                  flushRunnables = memtable.createFlushRunnables(txn);

                  for(int i = 0; i < flushRunnables.size(); ++i) {
                     futures.add(ColumnFamilyStore.perDiskflushExecutors[i].submit((Callable)flushRunnables.get(i)));
                  }

                  if(flushNonCf2i) {
                     ColumnFamilyStore.this.indexManager.flushAllNonCFSBackedIndexesBlocking();
                  }

                  flushResults = Lists.newArrayList(FBUtilities.waitOnFutures(futures));
               } catch (Throwable var34) {
                  accumulate = var34;
                  ColumnFamilyStore.logger.error("Flushing {} failed with error", memtable.toString(), var34);
                  Memtable.FlushRunnable runnable;
                  if(flushRunnables != null) {
                     for(var18 = flushRunnables.iterator(); var18.hasNext(); accumulate = runnable.abort(accumulate)) {
                        runnable = (Memtable.FlushRunnable)var18.next();
                     }
                  }

                  accumulate = Throwables.perform(accumulate, () -> {
                     FBUtilities.waitOnFutures(futures);
                  });
                  accumulate = txn.abort(accumulate);
                  com.google.common.base.Throwables.propagate(accumulate);
               }

               SSTableMultiWriter writerx;
               try {
                  Iterator writerIterator = flushResults.iterator();

                  while(writerIterator.hasNext()) {
                     SSTableMultiWriter writer = (SSTableMultiWriter)writerIterator.next();
                     if(writer.getFilePointer() > 0L) {
                        writer.setOpenResult(true).prepareToCommit();
                     } else {
                        Throwables.maybeFail(writer.abort((Throwable)null));
                        writerIterator.remove();
                     }
                  }
               } catch (Throwable var35) {
                  accumulate = var35;

                  for(var18 = flushResults.iterator(); var18.hasNext(); accumulate = writerx.abort(accumulate)) {
                     writerx = (SSTableMultiWriter)var18.next();
                  }

                  accumulate = txn.abort(accumulate);
                  throw com.google.common.base.Throwables.propagate(accumulate);
               }

               txn.prepareToCommit();
               accumulate = null;

               for(var18 = flushResults.iterator(); var18.hasNext(); accumulate = writerx.commit(accumulate)) {
                  writerx = (SSTableMultiWriter)var18.next();
               }

               Throwables.maybeFail(txn.commit(accumulate));
               var18 = flushResults.iterator();

               while(var18.hasNext()) {
                  writerx = (SSTableMultiWriter)var18.next();
                  Collection<SSTableReader> flushedSSTables = writerx.finished();
                  Iterator var21 = flushedSSTables.iterator();

                  while(var21.hasNext()) {
                     SSTableReader sstable = (SSTableReader)var21.next();
                     if(sstable != null) {
                        sstables.add(sstable);
                        long size = sstable.bytesOnDisk();
                        totalBytesOnDisk += size;
                        maxBytesOnDisk = Math.max(maxBytesOnDisk, size);
                        minBytesOnDisk = Math.min(minBytesOnDisk, size);
                     }
                  }
               }
            } catch (Throwable var36) {
               var14 = var36;
               throw var36;
            } finally {
               if(txn != null) {
                  if(var14 != null) {
                     try {
                        txn.close();
                     } catch (Throwable var33) {
                        var14.addSuppressed(var33);
                     }
                  } else {
                     txn.close();
                  }
               }

            }

            memtable.cfs.replaceFlushed(memtable, sstables);
            this.reclaim(memtable);
            memtable.cfs.compactionStrategyManager.compactionLogger.flush(sstables);
            ColumnFamilyStore.logger.debug("Flushed to {} ({} sstables, {}), biggest {}, smallest {} ({}ms)", new Object[]{sstables, Integer.valueOf(sstables.size()), FBUtilities.prettyPrintMemory(totalBytesOnDisk), FBUtilities.prettyPrintMemory(maxBytesOnDisk), FBUtilities.prettyPrintMemory(minBytesOnDisk), Long.valueOf(System.currentTimeMillis() - start)});
            return sstables;
         } else {
            memtable.cfs.replaceFlushed(memtable, Collections.emptyList());
            this.reclaim(memtable);
            return Collections.emptyList();
         }
      }

      private void reclaim(final Memtable memtable) {
         final OpOrder.Barrier readBarrier = ColumnFamilyStore.this.readOrdering.newBarrier();
         readBarrier.issue();
         this.postFlushTask.addListener(new WrappedRunnable() {
            public void runMayThrow() {
               readBarrier.await();
               memtable.setDiscarded();
            }
         }, ColumnFamilyStore.reclaimExecutor);
      }
   }

   private final class PostFlush implements Callable<CommitLogPosition> {
      final CountDownLatch latch;
      final List<Memtable> memtables;
      volatile Throwable flushFailure;

      private PostFlush(List<Memtable> memtables) {
         this.latch = new CountDownLatch(1);
         this.flushFailure = null;
         this.memtables = memtables;
      }

      public CommitLogPosition call() {
         try {
            this.latch.await();
         } catch (InterruptedException var3) {
            throw new IllegalStateException();
         }

         CommitLogPosition commitLogUpperBound = CommitLogPosition.NONE;
         if(this.flushFailure == null && !this.memtables.isEmpty()) {
            Memtable memtable = (Memtable)this.memtables.get(0);
            commitLogUpperBound = memtable.getCommitLogUpperBound();
            CommitLog.instance.discardCompletedSegments(ColumnFamilyStore.this.metadata.id, memtable.getCommitLogLowerBound(), commitLogUpperBound);
         }

         ColumnFamilyStore.this.metric.pendingFlushes.dec();
         if(this.flushFailure != null) {
            throw com.google.common.base.Throwables.propagate(this.flushFailure);
         } else {
            return commitLogUpperBound;
         }
      }
   }
}
