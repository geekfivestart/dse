package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.reactivex.Completable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCBoundaries;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Keyspace {
   private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);
   private static final String TEST_FAIL_WRITES_KS = System.getProperty("cassandra.test.fail_writes_ks", "");
   private static final boolean TEST_FAIL_WRITES;
   public final KeyspaceMetrics metric;
   private volatile KeyspaceMetadata metadata;
   public static final OpOrder writeOrder;
   private volatile OpOrder.Barrier writeBarrier = null;
   private final ConcurrentMap<TableId, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap();
   private volatile AbstractReplicationStrategy replicationStrategy;
   public final ViewManager viewManager;
   private volatile ReplicationParams replicationParams;
   private volatile TPCBoundaries tpcBoundaries;
   private long boundariesForRingVersion = -1L;
   private static volatile boolean initialized;
   private volatile boolean replicationParamsChanged = false;

   public static void setInitialized() {
      initialized = true;
   }

   @VisibleForTesting
   public static boolean isInitialized() {
      return initialized;
   }

   public static Keyspace open(String keyspaceName) {
      if(!initialized && !SchemaConstants.isLocalSystemKeyspace(keyspaceName)) {
         throw new IllegalStateException(String.format("Cannot open non-system keyspace %s as server is not yet initialized.", new Object[]{keyspaceName}));
      } else {
         return open(keyspaceName, Schema.instance, true);
      }
   }

   public static Keyspace openWithoutSSTables(String keyspaceName) {
      return open(keyspaceName, Schema.instance, false);
   }

   private static Keyspace open(String keyspaceName, Schema schema, boolean loadSSTables) {
      Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
      if(keyspaceInstance == null) {
         Class var4 = Keyspace.class;
         synchronized(Keyspace.class) {
            keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
            if(keyspaceInstance == null) {
               keyspaceInstance = new Keyspace(keyspaceName, loadSSTables);
               schema.storeKeyspaceInstance(keyspaceInstance);
            }
         }
      }

      return keyspaceInstance;
   }

   public static Keyspace clear(String keyspaceName) {
      return clear(keyspaceName, Schema.instance);
   }

   public static Keyspace clear(String keyspaceName, Schema schema) {
      Class var2 = Keyspace.class;
      synchronized(Keyspace.class) {
         Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
         if(t != null) {
            Iterator var4 = t.getColumnFamilyStores().iterator();

            while(var4.hasNext()) {
               ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
               t.unloadCf(cfs);
            }

            t.metric.release();
         }

         return t;
      }
   }

   public static ColumnFamilyStore openAndGetStore(TableMetadataRef tableRef) {
      return open(tableRef.keyspace).getColumnFamilyStore(tableRef.id);
   }

   public static ColumnFamilyStore openAndGetStore(TableMetadata table) {
      return open(table.keyspace).getColumnFamilyStore(table.id);
   }

   public static void removeUnreadableSSTables(File directory) {
      Iterator var1 = all().iterator();

      while(var1.hasNext()) {
         Keyspace keyspace = (Keyspace)var1.next();
         Iterator var3 = keyspace.getColumnFamilyStores().iterator();

         while(var3.hasNext()) {
            ColumnFamilyStore baseCfs = (ColumnFamilyStore)var3.next();
            Iterator var5 = baseCfs.concatWithIndexes().iterator();

            while(var5.hasNext()) {
               ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
               cfs.maybeRemoveUnreadableSSTables(directory);
            }
         }
      }

   }

   public void setMetadata(KeyspaceMetadata metadata) {
      this.metadata = metadata;
      this.createReplicationStrategy(metadata);
   }

   public KeyspaceMetadata getMetadata() {
      return this.metadata;
   }

   public Collection<ColumnFamilyStore> getColumnFamilyStores() {
      return Collections.unmodifiableCollection(this.columnFamilyStores.values());
   }

   public ColumnFamilyStore getColumnFamilyStore(String cfName) {
      TableMetadata table = Schema.instance.getTableMetadata(this.getName(), cfName);
      if(table == null) {
         throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", new Object[]{this.getName(), cfName}));
      } else {
         return this.getColumnFamilyStore(table.id);
      }
   }

   public ColumnFamilyStore getColumnFamilyStore(TableId id) {
      ColumnFamilyStore cfs = (ColumnFamilyStore)this.columnFamilyStores.get(id);
      if(cfs == null) {
         throw new UnknownTableException("Cannot find table, it may have been dropped", id);
      } else {
         return cfs;
      }
   }

   public boolean hasColumnFamilyStore(TableId id) {
      return this.columnFamilyStores.containsKey(id);
   }

   public Set<SSTableReader> snapshot(String snapshotName, String columnFamilyName, boolean skipFlush, Set<SSTableReader> alreadySnapshotted) throws IOException {
      assert (snapshotName != null);
      assert (alreadySnapshotted != null);
      boolean tookSnapShot = false;
      HashSet<SSTableReader> snapshotSSTables = new HashSet<SSTableReader>();
      alreadySnapshotted = new HashSet<SSTableReader>(alreadySnapshotted);
      for (ColumnFamilyStore cfStore : this.columnFamilyStores.values()) {
         if (columnFamilyName != null && !cfStore.name.equals(columnFamilyName)) continue;
         tookSnapShot = true;
         Set<SSTableReader> newSnapshots = cfStore.snapshot(snapshotName, null, false, skipFlush, alreadySnapshotted);
         snapshotSSTables.addAll(newSnapshots);
         alreadySnapshotted.addAll(newSnapshots);
      }
      if (columnFamilyName != null && !tookSnapShot) {
         throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");
      }
      return snapshotSSTables;
   }

   public void snapshot(String snapshotName, String columnFamilyName) throws IOException {
      this.snapshot(snapshotName, columnFamilyName, false, new HashSet());
   }

   public static String getTimestampedSnapshotName(String clientSuppliedName) {
      String snapshotName = Long.toString(System.currentTimeMillis());
      if(clientSuppliedName != null && !clientSuppliedName.equals("")) {
         snapshotName = snapshotName + "-" + clientSuppliedName;
      }

      return snapshotName;
   }

   public static String getTimestampedSnapshotNameWithPrefix(String clientSuppliedName, String prefix) {
      return prefix + "-" + getTimestampedSnapshotName(clientSuppliedName);
   }

   public boolean snapshotExists(String snapshotName) {
      assert snapshotName != null;

      Iterator var2 = this.columnFamilyStores.values().iterator();

      ColumnFamilyStore cfStore;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         cfStore = (ColumnFamilyStore)var2.next();
      } while(!cfStore.snapshotExists(snapshotName));

      return true;
   }

   public static void clearSnapshot(String snapshotName, String keyspace) {
      List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace, ColumnFamilyStore.getInitialDirectories());
      Directories.clearSnapshot(snapshotName, snapshotDirs);
   }

   public List<SSTableReader> getAllSSTables(SSTableSet sstableSet) {
      List<SSTableReader> list = new ArrayList(this.columnFamilyStores.size());
      Iterator var3 = this.columnFamilyStores.values().iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore cfStore = (ColumnFamilyStore)var3.next();
         Iterables.addAll(list, cfStore.getSSTables(sstableSet));
      }

      return list;
   }

   private Keyspace(String keyspaceName, boolean loadSSTables) {
      this.metadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
      if(this.metadata == null) {
         throw new UnknownKeyspaceException(keyspaceName);
      } else {
         this.createReplicationStrategy(this.metadata);
         this.metric = new KeyspaceMetrics(this);
         this.viewManager = new ViewManager(this);
         Iterator var3 = this.metadata.tablesAndViews().iterator();

         while(var3.hasNext()) {
            TableMetadata cfm = (TableMetadata)var3.next();
            if(cfm == null) {
               throw new IllegalStateException("Unexpected null metadata for keyspace " + keyspaceName);
            }

            TableMetadataRef ref = Schema.instance.getTableMetadataRef(cfm.id);
            if(ref == null) {
               throw new UnknownTableException("Unexpected null metadata for table " + cfm, cfm.id);
            }

            logger.trace("Initializing {}.{}", this.getName(), cfm.name);
            this.initCf(ref, loadSSTables);
         }

         this.viewManager.reload(false);
      }
   }

   private Keyspace(KeyspaceMetadata metadata) {
      this.metadata = metadata;
      this.createReplicationStrategy(metadata);
      this.metric = new KeyspaceMetrics(this);
      this.viewManager = new ViewManager(this);
   }

   public static Keyspace mockKS(KeyspaceMetadata metadata) {
      return new Keyspace(metadata);
   }

   public void setDefaultTPCBoundaries(List<Range<Token>> ranges) {
      this.boundariesForRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
      this.tpcBoundaries = this.computeTPCBoundaries(ranges);
   }

   public TPCBoundaries getTPCBoundaries() {
      TPCBoundaries boundaries = this.tpcBoundaries;
      if(boundaries == null || this.boundariesForRingVersion < StorageService.instance.getTokenMetadata().getRingVersion() || this.replicationParamsChanged) {
         if(!StorageService.instance.isInitialized()) {
            return boundaries != null?boundaries:(SchemaConstants.isLocalSystemKeyspace(this.metadata.name)?TPCBoundaries.LOCAL:TPCBoundaries.NONE);
         }

         synchronized(this) {
            boundaries = this.tpcBoundaries;
            if(boundaries == null || this.boundariesForRingVersion < StorageService.instance.getTokenMetadata().getRingVersion() || this.replicationParamsChanged) {
               this.boundariesForRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
               this.tpcBoundaries = boundaries = this.computeTPCBoundaries();
               this.replicationParamsChanged = false;
            }
         }
      }

      return boundaries;
   }

   private TPCBoundaries computeTPCBoundaries() {
      if(SchemaConstants.isLocalSystemKeyspace(this.metadata.name)) {
         return TPCBoundaries.LOCAL;
      } else {
         List<Range<Token>> localRanges = StorageService.getStartupTokenRanges(this);
         return this.computeTPCBoundaries(localRanges);
      }
   }

   private TPCBoundaries computeTPCBoundaries(List<Range<Token>> ranges) {
      TPCBoundaries boundaries = ranges == null?TPCBoundaries.NONE:TPCBoundaries.compute(ranges, TPC.getNumCores());
      logger.debug("Computed TPC core assignments for {}: {}", this.getName(), boundaries);
      return boundaries;
   }

   private void createReplicationStrategy(KeyspaceMetadata ksm) {
      this.replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name, ksm.params.replication.klass, StorageService.instance.getTokenMetadata(), DatabaseDescriptor.getEndpointSnitch(), ksm.params.replication.options);
      if(!ksm.params.replication.equals(this.replicationParams)) {
         logger.debug("New replication settings for keyspace {} - invalidating disk boundary caches and TPC boundaries", ksm.name);
         this.columnFamilyStores.values().forEach(ColumnFamilyStore::invalidateDiskBoundaries);
         synchronized(this) {
            this.replicationParamsChanged = true;
         }
      }

      this.replicationParams = ksm.params.replication;
   }

   public void dropCf(TableId tableId) {
      assert this.columnFamilyStores.containsKey(tableId);

      ColumnFamilyStore cfs = (ColumnFamilyStore)this.columnFamilyStores.remove(tableId);
      if(cfs != null) {
         cfs.getCompactionStrategyManager().shutdown();
         CompactionManager.instance.interruptCompactionForCFs(cfs.concatWithIndexes());
         Keyspace var10000 = cfs.keyspace;
         writeOrder.awaitNewBarrier();
         cfs.readOrdering.awaitNewBarrier();
         this.unloadCf(cfs);
      }
   }

   private void unloadCf(ColumnFamilyStore cfs) {
      cfs.forceBlockingFlush();
      cfs.invalidate();
   }

   public void initCfCustom(ColumnFamilyStore newCfs) {
      ColumnFamilyStore cfs = (ColumnFamilyStore)this.columnFamilyStores.get(newCfs.metadata.id);
      if(cfs == null) {
         ColumnFamilyStore oldCfs = (ColumnFamilyStore)this.columnFamilyStores.putIfAbsent(newCfs.metadata.id, newCfs);
         if(oldCfs != null) {
            throw new IllegalStateException("added multiple mappings for cf id " + newCfs.metadata.id);
         }
      } else {
         throw new IllegalStateException("CFS is already initialized: " + cfs.name);
      }
   }

   public void initCf(TableMetadataRef metadata, boolean loadSSTables) {
      ColumnFamilyStore cfs = (ColumnFamilyStore)this.columnFamilyStores.get(metadata.id);
      if(cfs == null) {
         ColumnFamilyStore oldCfs = (ColumnFamilyStore)this.columnFamilyStores.putIfAbsent(metadata.id, ColumnFamilyStore.createColumnFamilyStore(this, metadata, loadSSTables));
         if(oldCfs != null) {
            throw new IllegalStateException("added multiple mappings for cf id " + metadata.id);
         }
      } else {
         assert cfs.name.equals(metadata.name);

         cfs.reload();
      }

   }

   public OpOrder.Barrier stopMutations() {
      assert this.writeBarrier == null : "Keyspace has already been closed to mutations";

      this.writeBarrier = writeOrder.newBarrier();
      this.writeBarrier.issue();
      return this.writeBarrier;
   }

   public Completable apply(Mutation mutation, boolean writeCommitLog) {
      return this.apply(mutation, writeCommitLog, true, true);
   }

   public Completable apply(Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean isDroppable) {
      if(TEST_FAIL_WRITES && this.metadata.name.equals(TEST_FAIL_WRITES_KS)) {
         return Completable.error(new InternalRequestExecutionException(RequestFailureReason.UNKNOWN, "Testing write failures"));
      } else if(this.writeBarrier != null) {
         return this.failDueToWriteBarrier(mutation);
      } else {
         boolean requiresViewUpdate = updateIndexes && this.viewManager.updatesAffectView(Collections.singleton(mutation), false);
         return requiresViewUpdate?this.applyWithViews(mutation, writeCommitLog, updateIndexes, isDroppable):this.applyNoViews(mutation, writeCommitLog, updateIndexes);
      }
   }

   private Completable applyWithViews(Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean isDroppable) {
      Supplier<CompletableFuture<Void>> update = () -> {
         OpOrder var10000 = writeOrder;
         writeOrder.getClass();
         return TPCUtils.toFuture(Completable.using(var10000::start, (opGroup) -> {
            return this.applyInternal(opGroup, mutation, writeCommitLog, updateIndexes, true);
         }, (opGroup) -> {
            opGroup.close();
         }));
      };
      return TPCUtils.toCompletable(this.viewManager.updateWithLocks(mutation, update, isDroppable));
   }

   private Completable applyNoViews(Mutation mutation, boolean writeCommitLog, boolean updateIndexes) {
      OpOrder var10000 = writeOrder;
      writeOrder.getClass();
      return Completable.using(var10000::start, (opGroup) -> {
         return this.applyInternal(opGroup, mutation, writeCommitLog, updateIndexes, false);
      }, OpOrder.Group::close);
   }

   private Completable applyInternal(OpOrder.Group opGroup, Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean requiresViewUpdate) {
      return this.writeBarrier != null && !this.writeBarrier.isAfter(opGroup)?this.failDueToWriteBarrier(mutation):(!writeCommitLog?this.postCommitLogApply(opGroup, mutation, CommitLogPosition.NONE, updateIndexes, requiresViewUpdate):CommitLog.instance.add(mutation).flatMapCompletable((position) -> {
         return this.postCommitLogApply(opGroup, mutation, position, updateIndexes, requiresViewUpdate);
      }));
   }

   private Completable postCommitLogApply(OpOrder.Group opGroup, Mutation mutation, CommitLogPosition commitLogPosition, boolean updateIndexes, boolean requiresViewUpdate) {
      if(logger.isTraceEnabled()) {
         logger.trace("Got CL position {} for mutation {} (view updates: {})", new Object[]{commitLogPosition, mutation, Boolean.valueOf(requiresViewUpdate)});
      }

      List<Completable> memtablePutCompletables = new ArrayList(mutation.getPartitionUpdates().size());
      Iterator var7 = mutation.getPartitionUpdates().iterator();

      while(var7.hasNext()) {
         PartitionUpdate upd = (PartitionUpdate)var7.next();
         ColumnFamilyStore cfs = (ColumnFamilyStore)this.columnFamilyStores.get(upd.metadata().id);
         if(cfs == null) {
            logger.error("Attempting to mutate non-existant table {} ({}.{})", new Object[]{upd.metadata().id, upd.metadata().keyspace, upd.metadata().name});
         } else {
            AtomicLong baseComplete = new AtomicLong(9223372036854775807L);
            Completable viewUpdateCompletable = null;
            if(requiresViewUpdate) {
               Tracing.trace("Creating materialized view mutations from base table replica");
               viewUpdateCompletable = this.viewManager.forTable(upd.metadata().id).pushViewReplicaUpdates(upd, commitLogPosition != CommitLogPosition.NONE, baseComplete).doOnError((exc) -> {
                  JVMStabilityInspector.inspectThrowable(exc);
                  logger.error(String.format("Unknown exception caught while attempting to update MaterializedView! %s.%s", new Object[]{upd.metadata().keyspace, upd.metadata().name}), exc);
               });
            }

            Tracing.trace("Adding to {} memtable", (Object)upd.metadata().name);
            UpdateTransaction indexTransaction = updateIndexes?cfs.indexManager.newUpdateTransaction(upd, opGroup, FBUtilities.nowInSeconds()):UpdateTransaction.NO_OP;
            CommitLogPosition pos = commitLogPosition == CommitLogPosition.NONE?null:commitLogPosition;
            Completable memtableCompletable;
            if(requiresViewUpdate) {
               memtableCompletable = viewUpdateCompletable.andThen(Completable.defer(() -> {
                  return cfs.apply(upd, indexTransaction, opGroup, pos);
               })).doOnComplete(() -> {
                  baseComplete.set(System.currentTimeMillis());
               });
            } else {
               memtableCompletable = cfs.apply(upd, indexTransaction, opGroup, pos);
            }

            memtablePutCompletables.add(memtableCompletable);
         }
      }

      if(memtablePutCompletables.size() == 1) {
         return (Completable)memtablePutCompletables.get(0);
      } else {
         return Completable.concat(memtablePutCompletables);
      }
   }

   private Completable failDueToWriteBarrier(Mutation mutation) {
      assert this.writeBarrier != null : "Expected non null write barrier";

      if(SchemaConstants.isLocalSystemKeyspace(mutation.getKeyspaceName())) {
         logger.warn("Attempted to apply system mutation {} during shutdown but keyspace was already closed to mutations", mutation);
         return Completable.complete();
      } else {
         logger.debug(FBUtilities.Debug.getStackTrace());
         logger.error("Attempted to apply user mutation {} during shutdown but keyspace was already closed to mutations", mutation);
         return Completable.error(new InternalRequestExecutionException(RequestFailureReason.UNKNOWN, "Keyspace closed to new mutations"));
      }
   }

   public AbstractReplicationStrategy getReplicationStrategy() {
      return this.replicationStrategy;
   }

   public List<CompletableFuture<CommitLogPosition>> flush() {
      List<CompletableFuture<CommitLogPosition>> futures = new ArrayList(this.columnFamilyStores.size());
      Iterator var2 = this.columnFamilyStores.values().iterator();

      while(var2.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var2.next();
         futures.add(cfs.forceFlush());
      }

      return futures;
   }

   public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String... cfNames) throws IOException {
      Set<ColumnFamilyStore> valid = new HashSet();
      if(cfNames.length == 0) {
         Iterator var13 = this.getColumnFamilyStores().iterator();

         while(var13.hasNext()) {
            ColumnFamilyStore cfStore = (ColumnFamilyStore)var13.next();
            valid.add(cfStore);
            if(autoAddIndexes) {
               valid.addAll(this.getIndexColumnFamilyStores(cfStore));
            }
         }

         return valid;
      } else {
         String[] var5 = cfNames;
         int var6 = cfNames.length;

         for(int var7 = 0; var7 < var6; ++var7) {
            String cfName = var5[var7];
            if(SecondaryIndexManager.isIndexColumnFamily(cfName)) {
               if(!allowIndexes) {
                  logger.warn("Operation not allowed on secondary Index table ({})", cfName);
               } else {
                  String baseName = SecondaryIndexManager.getParentCfsName(cfName);
                  String indexName = SecondaryIndexManager.getIndexName(cfName);
                  ColumnFamilyStore baseCfs = this.getColumnFamilyStore(baseName);
                  Index index = baseCfs.indexManager.getIndexByName(indexName);
                  if(index == null) {
                     throw new IllegalArgumentException(String.format("Invalid index specified: %s/%s.", new Object[]{baseCfs.metadata.name, indexName}));
                  }

                  if(index.getBackingTable().isPresent()) {
                     valid.add(index.getBackingTable().get());
                  }
               }
            } else {
               ColumnFamilyStore cfStore = this.getColumnFamilyStore(cfName);
               valid.add(cfStore);
               if(autoAddIndexes) {
                  valid.addAll(this.getIndexColumnFamilyStores(cfStore));
               }
            }
         }

         return valid;
      }
   }

   private Set<ColumnFamilyStore> getIndexColumnFamilyStores(ColumnFamilyStore baseCfs) {
      Set<ColumnFamilyStore> stores = new HashSet();
      Iterator var3 = baseCfs.indexManager.getAllIndexColumnFamilyStores().iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore indexCfs = (ColumnFamilyStore)var3.next();
         logger.info("adding secondary index table {} to operation", indexCfs.metadata.name);
         stores.add(indexCfs);
      }

      return stores;
   }

   public static Iterable<Keyspace> all() {
      return toKeyspaces(Schema.instance.getKeyspaces());
   }

   public static Iterable<Keyspace> nonSystem() {
      return toKeyspaces(Schema.instance.getNonSystemKeyspaces());
   }

   public static Iterable<Keyspace> nonLocalStrategy() {
      return toKeyspaces(Schema.instance.getNonLocalStrategyKeyspaces());
   }

   public static Iterable<Keyspace> system() {
      return toKeyspaces(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);
   }

   private static Iterable<Keyspace> toKeyspaces(Collection<String> ksNames) {
      return (Iterable)ksNames.stream().map((ksName) -> {
         try {
            return open(ksName);
         } catch (UnknownKeyspaceException var2) {
            logger.info("Could not open keyspace {}, it was probably dropped.", var2.keyspaceName);
            return null;
         } catch (UnknownTableException var3) {
            logger.info("Could not open keyspace {}: {}", ksName, var3.getMessage());
            return null;
         } catch (Throwable var4) {
            JVMStabilityInspector.inspectThrowable(var4);
            logger.error("Failed to open keyspace {} due to unexpected exception", ksName, var4);
            return null;
         }
      }).filter(Objects::nonNull).collect(Collectors.toList());
   }

   public String toString() {
      return this.getClass().getSimpleName() + "(name='" + this.getName() + "')";
   }

   public String getName() {
      return this.metadata.name;
   }

   static {
      TEST_FAIL_WRITES = !TEST_FAIL_WRITES_KS.isEmpty();
      if(DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized()) {
         DatabaseDescriptor.createAllDirectories();
      }

      writeOrder = TPC.newOpOrder(Keyspace.class);
      initialized = false;
   }
}
