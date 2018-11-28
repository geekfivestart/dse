package org.apache.cassandra.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.reactivex.Completable;
import io.reactivex.internal.operators.completable.CompletableEmpty;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowDiffListener;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.CleanupTransaction;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondaryIndexManager implements IndexRegistry, INotificationConsumer {
   private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);
   public static final int PAGE_SIZE_ROWS = 10000;
   public static final int PAGE_SIZE_BYTES = 33554432;
   private final Map<String, Index> indexes = Maps.newConcurrentMap();
   private final Set<String> toRebuildIndexes = Sets.newConcurrentHashSet();
   private final Set<String> queryableIndexes = Sets.newConcurrentHashSet();
   private final Map<String, AtomicInteger> inProgressBuilds = Maps.newConcurrentMap();
   private static final ListeningExecutorService asyncExecutor;
   private static final ListeningExecutorService blockingExecutor;
   public final ColumnFamilyStore baseCfs;

   public SecondaryIndexManager(ColumnFamilyStore baseCfs) {
      this.baseCfs = baseCfs;
      baseCfs.getTracker().subscribe(this);
   }

   public void reload() {
      Indexes tableIndexes = this.baseCfs.metadata().indexes;
      this.indexes.keySet().stream().filter((indexName) -> {
         return !tableIndexes.has(indexName);
      }).forEach(this::removeIndex);
      Iterator var2 = tableIndexes.iterator();

      while(var2.hasNext()) {
         IndexMetadata tableIndex = (IndexMetadata)var2.next();
         this.addIndex(tableIndex, false);
      }

   }

   private Future<?> reloadIndex(IndexMetadata indexDef) {
      Index index = (Index)this.indexes.get(indexDef.name);
      Callable<?> reloadTask = index.getMetadataReloadTask(indexDef);
      return reloadTask == null?Futures.immediateFuture((Object)null):blockingExecutor.submit(reloadTask);
   }

   private Index createIndex(IndexMetadata indexDef, boolean isNewCF) {
      Index index = this.createInstance(indexDef);
      index.register(this);
      this.markIndexesBuilding(ImmutableSet.of(index), true, isNewCF);
      return index;
   }

   private Future<?> buildIndex(final Index index) {
      Callable<?> initialBuildTask = null;
      if(this.indexes.containsKey(index.getIndexMetadata().name)) {
         try {
            initialBuildTask = index.getInitializationTask();
         } catch (Throwable var4) {
            this.logAndMarkIndexesFailed(Collections.singleton(index), var4);
            throw var4;
         }
      }

      if(initialBuildTask == null) {
         this.markIndexBuilt(index, true);
         return Futures.immediateFuture((Object)null);
      } else {
         final SettableFuture initialization = SettableFuture.create();
         Futures.addCallback(asyncExecutor.submit(initialBuildTask), new FutureCallback() {
            public void onFailure(Throwable t) {
               SecondaryIndexManager.this.logAndMarkIndexesFailed(Collections.singleton(index), t);
               initialization.setException(t);
            }

            public void onSuccess(Object o) {
               SecondaryIndexManager.this.markIndexBuilt(index, true);
               initialization.set(o);
            }
         }, MoreExecutors.directExecutor());
         return initialization;
      }
   }

   @VisibleForTesting
   public synchronized Future<?> addIndex(IndexMetadata indexDef, boolean isNewCF) {
      return this.indexes.containsKey(indexDef.name)?this.reloadIndex(indexDef):this.buildIndex(this.createIndex(indexDef, isNewCF));
   }

   public synchronized void loadIndexesAsync(TableMetadata metadata, boolean isNewCF) {
      List<Index> indexes = new ArrayList(metadata.indexes.size());
      Iterator var4 = metadata.indexes.iterator();

      while(var4.hasNext()) {
         IndexMetadata info = (IndexMetadata)var4.next();
         indexes.add(this.createIndex(info, isNewCF));
      }

      var4 = indexes.iterator();

      while(var4.hasNext()) {
         Index index = (Index)var4.next();
         this.buildIndex(index);
      }

   }

   public boolean isIndexQueryable(Index index) {
      return this.queryableIndexes.contains(index.getIndexMetadata().name);
   }

   @VisibleForTesting
   public synchronized boolean isIndexBuilding(String indexName) {
      AtomicInteger counter = (AtomicInteger)this.inProgressBuilds.get(indexName);
      return counter != null && counter.get() > 0;
   }

   public synchronized void removeIndex(String indexName) {
      Index index = this.unregisterIndex(indexName);
      if(null != index) {
         this.markIndexRemoved(indexName);
         this.executeBlocking(index.getInvalidateTask(), (FutureCallback)null);
      }

   }

   public Set<IndexMetadata> getDependentIndexes(ColumnMetadata column) {
      if(this.indexes.isEmpty()) {
         return Collections.emptySet();
      } else {
         Set<IndexMetadata> dependentIndexes = new HashSet();
         Iterator var3 = this.indexes.values().iterator();

         while(var3.hasNext()) {
            Index index = (Index)var3.next();
            if(index.dependsOn(column)) {
               dependentIndexes.add(index.getIndexMetadata());
            }
         }

         return dependentIndexes;
      }
   }

   public void markAllIndexesRemoved() {
      this.getBuiltIndexNamesBlocking().forEach(this::markIndexRemoved);
   }

   public void rebuildIndexesBlocking(Set<String> indexNames) {
      ColumnFamilyStore.RefViewFragment viewFragment = this.baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
      Throwable var3 = null;

      try {
         Refs<SSTableReader> allSSTables = viewFragment.refs;
         Throwable var5 = null;

         try {
            Set<Index> toRebuild = (Set)this.indexes.values().stream().filter((index) -> {
               return indexNames.contains(index.getIndexMetadata().name);
            }).filter(Index::shouldBuildBlocking).collect(Collectors.toSet());
            if(!toRebuild.isEmpty()) {
               this.buildIndexesBlocking(allSSTables, toRebuild, true);
               return;
            }

            logger.info("No defined indexes with the supplied names: {}", Joiner.on(',').join(indexNames));
         } catch (Throwable var32) {
            var5 = var32;
            throw var32;
         } finally {
            if(allSSTables != null) {
               if(var5 != null) {
                  try {
                     allSSTables.close();
                  } catch (Throwable var31) {
                     var5.addSuppressed(var31);
                  }
               } else {
                  allSSTables.close();
               }
            }

         }
      } catch (Throwable var34) {
         var3 = var34;
         throw var34;
      } finally {
         if(viewFragment != null) {
            if(var3 != null) {
               try {
                  viewFragment.close();
               } catch (Throwable var30) {
                  var3.addSuppressed(var30);
               }
            } else {
               viewFragment.close();
            }
         }

      }

   }

   public static boolean isIndexColumnFamilyStore(ColumnFamilyStore cfs) {
      return isIndexColumnFamily(cfs.name);
   }

   public static boolean isIndexColumnFamily(String cfName) {
      return cfName.contains(".");
   }

   public static ColumnFamilyStore getParentCfs(ColumnFamilyStore cfs) {
      String parentCfs = getParentCfsName(cfs.name);
      return cfs.keyspace.getColumnFamilyStore(parentCfs);
   }

   public static String getParentCfsName(String cfName) {
      assert isIndexColumnFamily(cfName);

      return StringUtils.substringBefore(cfName, ".");
   }

   public static String getIndexName(ColumnFamilyStore cfs) {
      return getIndexName(cfs.name);
   }

   public static String getIndexName(String cfName) {
      assert isIndexColumnFamily(cfName);

      return StringUtils.substringAfter(cfName, ".");
   }

   private void buildIndexesBlocking(Collection<SSTableReader> sstables, Set<Index> indexes, boolean isFullRebuild) {
      if(!indexes.isEmpty()) {
         this.markIndexesBuilding(indexes, isFullRebuild, false);
         final Set<Index> builtIndexes = new HashSet();
         Set<Index> unbuiltIndexes = new HashSet();
         Exception accumulatedFail = null;
         boolean var17 = false;

         try {
            var17 = true;
            logger.info("Submitting index build of {} for data in {}", indexes.stream().map((i) -> {
               return i.getIndexMetadata().name;
            }).collect(Collectors.joining(",")), sstables.stream().map(SSTable::toString).collect(Collectors.joining(",")));
            Map<Index.IndexBuildingSupport, Set<Index>> byType = new HashMap();
            Iterator var8 = indexes.iterator();

            while(true) {
               if(!var8.hasNext()) {
                  List<Future<?>> futures = new ArrayList(byType.size());
                  byType.forEach((buildingSupport, groupedIndexes) -> {
                     SecondaryIndexBuilder builder = buildingSupport.getIndexBuildTask(this.baseCfs, groupedIndexes, sstables);
                     final SettableFuture build = SettableFuture.create();
                     Futures.addCallback(CompactionManager.instance.submitIndexBuild(builder), new FutureCallback() {
                        public void onFailure(Throwable t) {
                           SecondaryIndexManager.this.logAndMarkIndexesFailed(groupedIndexes, t);
                           unbuiltIndexes.addAll(groupedIndexes);
                           build.setException(t);
                        }

                        public void onSuccess(Object o) {
                           groupedIndexes.forEach((i) -> {
                              SecondaryIndexManager.this.markIndexBuilt(i, isFullRebuild);
                           });
                           SecondaryIndexManager.logger.info("Index build of {} completed", SecondaryIndexManager.this.getIndexNames(groupedIndexes));
                           builtIndexes.addAll(groupedIndexes);
                           build.set(o);
                        }
                     });
                     futures.add(build);
                  });
                  FBUtilities.waitOnFutures(futures);
                  var17 = false;
                  break;
               }

               Index index = (Index)var8.next();
               Set<Index> stored = (Set)byType.computeIfAbsent(index.getBuildTaskSupport(), (i) -> {
                  return new HashSet();
               });
               stored.add(index);
            }
         } catch (Exception var20) {
            accumulatedFail = var20;
            throw var20;
         } finally {
            if(var17) {
               try {
                  Set<Index> failedIndexes = Sets.difference(indexes, Sets.union(builtIndexes, unbuiltIndexes));
                  if(!failedIndexes.isEmpty()) {
                     this.logAndMarkIndexesFailed(failedIndexes, accumulatedFail);
                  }

                  this.flushIndexesBlocking(builtIndexes, new FutureCallback() {
                     String indexNames = StringUtils.join((Iterable)builtIndexes.stream().map((i) -> {
                        return i.getIndexMetadata().name;
                     }).collect(Collectors.toList()), ',');

                     public void onFailure(Throwable ignored) {
                        SecondaryIndexManager.logger.info("Index flush of {} failed", this.indexNames);
                     }

                     public void onSuccess(Object ignored) {
                        SecondaryIndexManager.logger.info("Index flush of {} completed", this.indexNames);
                     }
                  });
               } catch (Exception var18) {
                  if(accumulatedFail == null) {
                     throw var18;
                  }

                  accumulatedFail.addSuppressed(var18);
               }

            }
         }

         try {
            Set<Index> failedIndexes = Sets.difference(indexes, Sets.union(builtIndexes, unbuiltIndexes));
            if(!failedIndexes.isEmpty()) {
               this.logAndMarkIndexesFailed(failedIndexes, accumulatedFail);
            }

            this.flushIndexesBlocking(builtIndexes, new FutureCallback() {
               String indexNames = StringUtils.join((Iterable)builtIndexes.stream().map((i) -> {
                  return i.getIndexMetadata().name;
               }).collect(Collectors.toList()), ',');

               public void onFailure(Throwable ignored) {
                  SecondaryIndexManager.logger.info("Index flush of {} failed", this.indexNames);
               }

               public void onSuccess(Object ignored) {
                  SecondaryIndexManager.logger.info("Index flush of {} completed", this.indexNames);
               }
            });
         } catch (Exception var19) {
            if(accumulatedFail == null) {
               throw var19;
            }

            accumulatedFail.addSuppressed(var19);
         }

      }
   }

   private String getIndexNames(Set<Index> indexes) {
      List<String> indexNames = (List)indexes.stream().map((i) -> {
         return i.getIndexMetadata().name;
      }).collect(Collectors.toList());
      return StringUtils.join(indexNames, ',');
   }

   private synchronized void markIndexesBuilding(Set<Index> indexes, boolean isFullRebuild, boolean isNewCF) {
      String keyspaceName = this.baseCfs.keyspace.getName();
      indexes.forEach((index) -> {
         String indexName = index.getIndexMetadata().name;
         AtomicInteger counter = (AtomicInteger)this.inProgressBuilds.computeIfAbsent(indexName, (ignored) -> {
            return new AtomicInteger(0);
         });
         if(counter.get() > 0 && isFullRebuild) {
            throw new IllegalStateException(String.format("Cannot rebuild index %s as another index build for the same index is currently in progress.", new Object[]{indexName}));
         }
      });
      indexes.forEach((index) -> {
         String indexName = index.getIndexMetadata().name;
         AtomicInteger counter = (AtomicInteger)this.inProgressBuilds.computeIfAbsent(indexName, (ignored) -> {
            return new AtomicInteger(0);
         });
         if(isFullRebuild) {
            this.toRebuildIndexes.remove(indexName);
         }

         if(counter.getAndIncrement() == 0 && DatabaseDescriptor.isDaemonInitialized() && !isNewCF) {
            TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(keyspaceName, indexName));
         }

      });
   }

   private synchronized void markIndexBuilt(Index index, boolean isFullRebuild) {
      String indexName = index.getIndexMetadata().name;
      if(isFullRebuild) {
         this.queryableIndexes.add(indexName);
      }

      AtomicInteger counter = (AtomicInteger)this.inProgressBuilds.get(indexName);
      if(counter != null) {
         assert counter.get() > 0;

         if(counter.decrementAndGet() == 0) {
            this.inProgressBuilds.remove(indexName);
            if(!this.toRebuildIndexes.contains(indexName) && DatabaseDescriptor.isDaemonInitialized()) {
               TPCUtils.blockingAwait(SystemKeyspace.setIndexBuilt(this.baseCfs.keyspace.getName(), indexName));
            }
         }
      }

   }

   private synchronized void markIndexFailed(Index index) {
      String indexName = index.getIndexMetadata().name;
      AtomicInteger counter = (AtomicInteger)this.inProgressBuilds.get(indexName);
      if(counter != null) {
         assert counter.get() > 0;

         counter.decrementAndGet();
         if(DatabaseDescriptor.isDaemonInitialized()) {
            TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(this.baseCfs.keyspace.getName(), indexName));
         }

         this.toRebuildIndexes.add(indexName);
      }

   }

   private void logAndMarkIndexesFailed(Set<Index> indexes, Throwable indexBuildFailure) {
      JVMStabilityInspector.inspectThrowable(indexBuildFailure);
      if(indexBuildFailure != null) {
         logger.warn("Index build of {} failed. Please run full index rebuild to fix it.", this.getIndexNames(indexes), indexBuildFailure);
      } else {
         logger.warn("Index build of {} failed. Please run full index rebuild to fix it.", this.getIndexNames(indexes));
      }

      indexes.forEach(this::markIndexFailed);
   }

   private synchronized void markIndexRemoved(String indexName) {
      TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(this.baseCfs.keyspace.getName(), indexName));
      this.queryableIndexes.remove(indexName);
      this.toRebuildIndexes.remove(indexName);
      this.inProgressBuilds.remove(indexName);
   }

   public Index getIndexByName(String indexName) {
      return (Index)this.indexes.get(indexName);
   }

   private Index createInstance(IndexMetadata indexDef) {
      Object newIndex;
      if(indexDef.isCustom()) {
         assert indexDef.options != null;

         String className = (String)indexDef.options.get("class_name");

         assert !Strings.isNullOrEmpty(className);

         try {
            Class<? extends Index> indexClass = FBUtilities.classForName(className, "Index");
            Constructor<? extends Index> ctor = indexClass.getConstructor(new Class[]{ColumnFamilyStore.class, IndexMetadata.class});
            newIndex = (Index)ctor.newInstance(new Object[]{this.baseCfs, indexDef});
         } catch (Exception var6) {
            throw new RuntimeException(var6);
         }
      } else {
         newIndex = CassandraIndex.newIndex(this.baseCfs, indexDef);
      }

      return (Index)newIndex;
   }

   public void truncateAllIndexesBlocking(long truncatedAt) {
      this.executeAllBlocking(this.indexes.values().stream(), (index) -> {
         return index.getTruncateTask(truncatedAt);
      }, (FutureCallback)null);
   }

   public void dropAllIndexes() {
      this.markAllIndexesRemoved();
      this.invalidateAllIndexesBlocking();
   }

   @VisibleForTesting
   public void invalidateAllIndexesBlocking() {
      this.executeAllBlocking(this.indexes.values().stream(), Index::getInvalidateTask, (FutureCallback)null);
   }

   public void flushAllIndexesBlocking() {
      this.flushIndexesBlocking(ImmutableSet.copyOf(this.indexes.values()));
   }

   public void flushIndexesBlocking(Set<Index> indexes) {
      this.flushIndexesBlocking(indexes, (FutureCallback)null);
   }

   public void flushAllNonCFSBackedIndexesBlocking() {
      this.executeAllBlocking(this.indexes.values().stream().filter((index) -> {
         return !index.getBackingTable().isPresent();
      }), Index::getBlockingFlushTask, (FutureCallback)null);
   }

   public void executePreJoinTasksBlocking(boolean hadBootstrap) {
      logger.info("Executing pre-join{} tasks for: {}", hadBootstrap?" post-bootstrap":"", this.baseCfs);
      this.executeAllBlocking(this.indexes.values().stream(), (index) -> {
         return index.getPreJoinTask(hadBootstrap);
      }, (FutureCallback)null);
   }

   private void flushIndexesBlocking(Set<Index> indexes, FutureCallback<Object> callback) {
      if(!indexes.isEmpty()) {
         List<CompletableFuture<CommitLogPosition>> futures = new ArrayList();
         List<Index> nonCfsIndexes = new ArrayList();
         synchronized(this.baseCfs.getTracker()) {
            Iterator var6 = indexes.iterator();

            while(true) {
               if(!var6.hasNext()) {
                  break;
               }

               Index index = (Index)var6.next();
               Optional<ColumnFamilyStore> backingTable = index.getBackingTable();
               if(backingTable.isPresent()) {
                  futures.add(((ColumnFamilyStore)backingTable.get()).forceFlush());
               } else {
                  nonCfsIndexes.add(index);
               }
            }
         }

         FBUtilities.waitOnFutures(futures);
         this.executeAllBlocking(nonCfsIndexes.stream(), Index::getBlockingFlushTask, callback);
      }
   }

   public List<String> getBuiltIndexNamesBlocking() {
      Set<String> allIndexNames = new HashSet();
      this.indexes.values().stream().map((i) -> {
         return i.getIndexMetadata().name;
      }).forEach(allIndexNames::add);
      return (List)TPCUtils.blockingGet(SystemKeyspace.getBuiltIndexes(this.baseCfs.keyspace.getName(), allIndexNames));
   }

   public Set<ColumnFamilyStore> getAllIndexColumnFamilyStores() {
      Set<ColumnFamilyStore> backingTables = new HashSet();
      this.indexes.values().forEach((index) -> {
         index.getBackingTable().ifPresent(backingTables::add);
      });
      return backingTables;
   }

   public boolean hasIndexes() {
      return !this.indexes.isEmpty();
   }

   public void indexPartition(DecoratedKey key, Set<Index> indexes, PageSize pageSize) {
      if(logger.isTraceEnabled()) {
         logger.trace("Indexing partition {}", this.baseCfs.metadata().partitionKeyType.getString(key.getKey()));
      }

      if(!indexes.isEmpty()) {
         SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(this.baseCfs.metadata(), FBUtilities.nowInSeconds(), key);
         int nowInSec = cmd.nowInSec();
         boolean readStatic = false;
         SinglePartitionPager pager = new SinglePartitionPager(cmd, (PagingState)null, ProtocolVersion.CURRENT);

         while(!pager.isExhausted()) {
            OpOrder.Group writeGroup = Keyspace.writeOrder.start();
            Throwable var9 = null;

            try {
               UnfilteredPartitionIterator page = FlowablePartitions.toPartitions(pager.fetchPageUnfiltered(pageSize), this.baseCfs.metadata());
               Throwable var11 = null;

               try {
                  if(!page.hasNext()) {
                     break;
                  }

                  UnfilteredRowIterator partition = (UnfilteredRowIterator)page.next();
                  Throwable var13 = null;

                  try {
                     Set<Index.Indexer> indexers = (Set)indexes.stream().map((index) -> {
                        return index.indexerFor(key, partition.columns(), nowInSec, writeGroup, IndexTransaction.Type.UPDATE);
                     }).filter(Objects::nonNull).collect(Collectors.toSet());
                     if(!readStatic && partition.isEmpty() && partition.staticRow().isEmpty()) {
                        break;
                     }

                     indexers.forEach(Index.Indexer::begin);
                     if(!readStatic) {
                        if(!partition.staticRow().isEmpty()) {
                           indexers.forEach((indexer) -> {
                              indexer.insertRow(partition.staticRow()).blockingAwait();
                           });
                        }

                        indexers.forEach((i) -> {
                           i.partitionDelete(partition.partitionLevelDeletion()).blockingAwait();
                        });
                        readStatic = true;
                     }

                     MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(partition.partitionLevelDeletion(), this.baseCfs.getComparator(), false);

                     while(partition.hasNext()) {
                        Unfiltered unfilteredRow = (Unfiltered)partition.next();
                        if(unfilteredRow.isRow()) {
                           Row row = (Row)unfilteredRow;
                           indexers.forEach((indexer) -> {
                              indexer.insertRow(row).blockingAwait();
                           });
                        } else {
                           assert unfilteredRow.isRangeTombstoneMarker();

                           RangeTombstoneMarker marker = (RangeTombstoneMarker)unfilteredRow;
                           deletionBuilder.add(marker);
                        }
                     }

                     MutableDeletionInfo deletionInfo = deletionBuilder.build();
                     if(deletionInfo.hasRanges()) {
                        Iterator iter = deletionInfo.rangeIterator(false);

                        while(iter.hasNext()) {
                           indexers.forEach((indexer) -> {
                              indexer.rangeTombstone((RangeTombstone)iter.next()).blockingAwait();
                           });
                        }
                     }

                     indexers.forEach((indexer) -> {
                        indexer.finish().blockingAwait();
                     });
                  } catch (Throwable var69) {
                     var13 = var69;
                     throw var69;
                  } finally {
                     if(partition != null) {
                        if(var13 != null) {
                           try {
                              partition.close();
                           } catch (Throwable var68) {
                              var13.addSuppressed(var68);
                           }
                        } else {
                           partition.close();
                        }
                     }

                  }
               } catch (Throwable var71) {
                  var11 = var71;
                  throw var71;
               } finally {
                  if(page != null) {
                     if(var11 != null) {
                        try {
                           page.close();
                        } catch (Throwable var67) {
                           var11.addSuppressed(var67);
                        }
                     } else {
                        page.close();
                     }
                  }

               }
            } catch (Throwable var73) {
               var9 = var73;
               throw var73;
            } finally {
               if(writeGroup != null) {
                  if(var9 != null) {
                     try {
                        writeGroup.close();
                     } catch (Throwable var66) {
                        var9.addSuppressed(var66);
                     }
                  } else {
                     writeGroup.close();
                  }
               }

            }
         }
      }

   }

   public PageSize calculateIndexingPageSize() {
      return Boolean.getBoolean("cassandra.force_default_indexing_page_size")?PageSize.rowsSize(10000):PageSize.bytesSize(33554432);
   }

   public void deletePartition(UnfilteredRowIterator partition, int nowInSec) {
      CleanupTransaction indexTransaction = this.newCleanupTransaction(partition.partitionKey(), partition.columns(), nowInSec);
      indexTransaction.start();
      indexTransaction.onPartitionDeletion(new DeletionTime(FBUtilities.timestampMicros(), nowInSec));
      indexTransaction.commit().blockingAwait();

      while(partition.hasNext()) {
         Unfiltered unfiltered = (Unfiltered)partition.next();
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            indexTransaction = this.newCleanupTransaction(partition.partitionKey(), partition.columns(), nowInSec);
            indexTransaction.start();
            indexTransaction.onRowDelete((Row)unfiltered);
            indexTransaction.commit().blockingAwait();
         }
      }

   }

   public Index getBestIndexFor(RowFilter rowFilter) {
      if(!this.indexes.isEmpty() && !rowFilter.isEmpty()) {
         Set<Index> searchableIndexes = new HashSet();
         Iterator var3 = rowFilter.iterator();

         while(var3.hasNext()) {
            RowFilter.Expression expression = (RowFilter.Expression)var3.next();
            if(expression.isCustom()) {
               RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression)expression;
               logger.trace("Command contains a custom index expression, using target index {}", customExpression.getTargetIndex().name);
               Tracing.trace("Command contains a custom index expression, using target index {}", (Object)customExpression.getTargetIndex().name);
               return (Index)this.indexes.get(customExpression.getTargetIndex().name);
            }

            if(!expression.isUserDefined()) {
               this.indexes.values().stream().filter((index) -> {
                  return index.supportsExpression(expression.column(), expression.operator());
               }).forEach(searchableIndexes::add);
            }
         }

         if(searchableIndexes.isEmpty()) {
            logger.trace("No applicable indexes found");
            Tracing.trace("No applicable indexes found");
            return null;
         } else {
            Index selected = searchableIndexes.size() == 1?(Index)Iterables.getOnlyElement(searchableIndexes):(Index)searchableIndexes.stream().min((a, b) -> {
               return Longs.compare(a.getEstimatedResultRows(), b.getEstimatedResultRows());
            }).orElseThrow(() -> {
               return new AssertionError("Could not select most selective index");
            });
            if(Tracing.isTracing()) {
               Tracing.trace("Index mean cardinalities are {}. Scanning with {}.", searchableIndexes.stream().map((i) -> {
                  return i.getIndexMetadata().name + ':' + i.getEstimatedResultRows();
               }).collect(Collectors.joining(",")), selected.getIndexMetadata().name);
            }

            return selected;
         }
      } else {
         return null;
      }
   }

   public Optional<Index> getBestIndexFor(RowFilter.Expression expression) {
      return this.indexes.values().stream().filter((i) -> {
         return i.supportsExpression(expression.column(), expression.operator());
      }).findFirst();
   }

   public void validate(PartitionUpdate update) throws InvalidRequestException {
      Iterator var2 = this.indexes.values().iterator();

      while(var2.hasNext()) {
         Index index = (Index)var2.next();
         index.validate(update);
      }

   }

   public void registerIndex(Index index) {
      String name = index.getIndexMetadata().name;
      this.indexes.put(name, index);
      logger.trace("Registered index {}", name);
   }

   public void unregisterIndex(Index index) {
      this.unregisterIndex(index.getIndexMetadata().name);
   }

   private Index unregisterIndex(String name) {
      Index removed = (Index)this.indexes.remove(name);
      logger.trace(removed == null?"Index {} was not registered":"Removed index {} from registry", name);
      return removed;
   }

   public Index getIndex(IndexMetadata metadata) {
      return (Index)this.indexes.get(metadata.name);
   }

   public Collection<Index> listIndexes() {
      return ImmutableSet.copyOf(this.indexes.values());
   }

   public UpdateTransaction newUpdateTransaction(PartitionUpdate update, OpOrder.Group opGroup, int nowInSec) {
      if(!this.hasIndexes()) {
         return UpdateTransaction.NO_OP;
      } else {
         Index.Indexer[] indexers = (Index.Indexer[])this.indexes.values().stream().map((i) -> {
            return i.indexerFor(update.partitionKey(), update.columns(), nowInSec, opGroup, IndexTransaction.Type.UPDATE);
         }).filter(Objects::nonNull).toArray((x$0) -> {
            return new Index.Indexer[x$0];
         });
         return (UpdateTransaction)(indexers.length == 0?UpdateTransaction.NO_OP:new SecondaryIndexManager.WriteTimeTransaction(indexers));
      }
   }

   public CompactionTransaction newCompactionTransaction(DecoratedKey key, RegularAndStaticColumns regularAndStaticColumns, int versions, int nowInSec) {
      return new SecondaryIndexManager.IndexGCTransaction(key, regularAndStaticColumns, versions, nowInSec, this.listIndexes());
   }

   public CleanupTransaction newCleanupTransaction(DecoratedKey key, RegularAndStaticColumns regularAndStaticColumns, int nowInSec) {
      return (CleanupTransaction)(!this.hasIndexes()?CleanupTransaction.NO_OP:new SecondaryIndexManager.CleanupGCTransaction(key, regularAndStaticColumns, nowInSec, this.listIndexes()));
   }

   private void executeBlocking(Callable<?> task, FutureCallback<Object> callback) {
      if(null != task) {
         ListenableFuture<?> f = blockingExecutor.submit(task);
         if(callback != null) {
            Futures.addCallback(f, callback);
         }

         FBUtilities.waitOnFuture(f);
      }

   }

   private void executeAllBlocking(Stream<Index> indexers, Function<Index, Callable<?>> function, FutureCallback<Object> callback) {
      if(function == null) {
         logger.error("failed to flush indexes: {} because flush task is missing.", indexers);
      } else {
         List<Future<?>> waitFor = new ArrayList();
         indexers.forEach((indexer) -> {
            Callable<?> task = (Callable)function.apply(indexer);
            if(null != task) {
               ListenableFuture<?> f = blockingExecutor.submit(task);
               if(callback != null) {
                  Futures.addCallback(f, callback);
               }

               waitFor.add(f);
            }

         });
         FBUtilities.waitOnFutures(waitFor);
      }
   }

   public void handleNotification(INotification notification, Object sender) {
      if(!this.indexes.isEmpty() && notification instanceof SSTableAddedNotification) {
         SSTableAddedNotification notice = (SSTableAddedNotification)notification;
         if(!notice.memtable().isPresent()) {
            this.buildIndexesBlocking(Lists.newArrayList(notice.added), (Set)this.indexes.values().stream().filter(Index::shouldBuildBlocking).collect(Collectors.toSet()), false);
         }
      }

   }

   static {
      asyncExecutor = MoreExecutors.listeningDecorator(new JMXEnabledThreadPoolExecutor(1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("SecondaryIndexManagement"), "internal"));
      blockingExecutor = MoreExecutors.newDirectExecutorService();
   }

   private static final class CleanupGCTransaction implements CleanupTransaction {
      private final DecoratedKey key;
      private final RegularAndStaticColumns columns;
      private final int nowInSec;
      private final Collection<Index> indexes;
      private Row row;
      private DeletionTime partitionDelete;

      private CleanupGCTransaction(DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, Collection<Index> indexes) {
         this.key = key;
         this.columns = columns;
         this.indexes = indexes;
         this.nowInSec = nowInSec;
      }

      public void start() {
      }

      public void onPartitionDeletion(DeletionTime deletionTime) {
         this.partitionDelete = deletionTime;
      }

      public void onRowDelete(Row row) {
         this.row = row;
      }

      public Completable commit() {
         return this.row == null && this.partitionDelete == null?Completable.complete():Completable.using(() -> {
            return Keyspace.writeOrder.start();
         }, (opOrder) -> {
            return Completable.defer(() -> {
               Completable r = Completable.complete();
               Iterator var3 = this.indexes.iterator();

               while(var3.hasNext()) {
                  Index index = (Index)var3.next();
                  Index.Indexer indexer = index.indexerFor(this.key, this.columns, this.nowInSec, opOrder, IndexTransaction.Type.CLEANUP);
                  if(indexer != null) {
                     indexer.begin();
                     if(this.partitionDelete != null) {
                        r = r.concatWith(indexer.partitionDelete(this.partitionDelete));
                     }

                     if(this.row != null) {
                        r = r.concatWith(indexer.removeRow(this.row));
                     }

                     r = r.concatWith(indexer.finish());
                  }
               }

               return r;
            });
         }, (opOrder) -> {
            opOrder.close();
         });
      }
   }

   private static final class IndexGCTransaction implements CompactionTransaction {
      private final DecoratedKey key;
      private final RegularAndStaticColumns columns;
      private final int versions;
      private final int nowInSec;
      private final Collection<Index> indexes;
      private Row[] rows;

      private IndexGCTransaction(DecoratedKey key, RegularAndStaticColumns columns, int versions, int nowInSec, Collection<Index> indexes) {
         this.key = key;
         this.columns = columns;
         this.versions = versions;
         this.indexes = indexes;
         this.nowInSec = nowInSec;
      }

      public void start() {
         if(this.versions > 0) {
            this.rows = new Row[this.versions];
         }

      }

      public void onRowMerge(Row merged, Row... versions) {
         final Row.Builder[] builders = new Row.Builder[versions.length];
         RowDiffListener diffListener = new RowDiffListener() {
            public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original) {
               if(original != null && (merged == null || !merged.isLive(IndexGCTransaction.this.nowInSec))) {
                  this.getBuilder(i, clustering).addPrimaryKeyLivenessInfo(original);
               }

            }

            public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original) {
            }

            public void onComplexDeletion(int i, Clustering clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original) {
            }

            public void onCell(int i, Clustering clustering, Cell merged, Cell original) {
               if(original != null && (merged == null || !merged.isLive(IndexGCTransaction.this.nowInSec))) {
                  this.getBuilder(i, clustering).addCell(original);
               }

            }

            private Row.Builder getBuilder(int index, Clustering clustering) {
               if(builders[index] == null) {
                  builders[index] = Row.Builder.sorted();
                  builders[index].newRow(clustering);
               }

               return builders[index];
            }
         };
         Rows.diff(diffListener, merged, versions);

         for(int i = 0; i < builders.length; ++i) {
            if(builders[i] != null) {
               this.rows[i] = builders[i].build();
            }
         }

      }

      public Completable commit() {
         return this.rows == null?Completable.complete():Completable.using(() -> {
            return Keyspace.writeOrder.start();
         }, (opOrder) -> {
            return Completable.defer(() -> {
               Completable r = Completable.complete();
               Iterator var3 = this.indexes.iterator();

               while(true) {
                  Index.Indexer indexer;
                  do {
                     if(!var3.hasNext()) {
                        return r;
                     }

                     Index index = (Index)var3.next();
                     indexer = index.indexerFor(this.key, this.columns, this.nowInSec, opOrder, IndexTransaction.Type.COMPACTION);
                  } while(indexer == null);

                  indexer.begin();
                  Row[] var6 = this.rows;
                  int var7 = var6.length;

                  for(int var8 = 0; var8 < var7; ++var8) {
                     Row row = var6[var8];
                     if(row != null) {
                        r = r.concatWith(indexer.removeRow(row));
                     }
                  }

                  r = r.concatWith(indexer.finish());
               }
            });
         }, (opOrder) -> {
            opOrder.close();
         });
      }
   }

   private static final class WriteTimeTransaction implements UpdateTransaction {
      private final Index.Indexer[] indexers;
      private final List<Completable> allCompletables;

      private WriteTimeTransaction(Index.Indexer... indexers) {
         Index.Indexer[] var2 = indexers;
         int var3 = indexers.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Index.Indexer indexer = var2[var4];

            assert indexer != null;
         }

         this.indexers = indexers;
         this.allCompletables = new ArrayList(indexers.length);
      }

      public void start() {
         Index.Indexer[] var1 = this.indexers;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            Index.Indexer indexer = var1[var3];
            indexer.begin();
         }

      }

      public void onPartitionDeletion(DeletionTime deletionTime) {
         Index.Indexer[] var2 = this.indexers;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Index.Indexer indexer = var2[var4];
            this.checkNotCompleteAndAdd(() -> {
               return indexer.partitionDelete(deletionTime);
            });
         }

      }

      public void onRangeTombstone(RangeTombstone tombstone) {
         Index.Indexer[] var2 = this.indexers;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Index.Indexer indexer = var2[var4];
            this.checkNotCompleteAndAdd(() -> {
               return indexer.rangeTombstone(tombstone);
            });
         }

      }

      public void onInserted(Row row) {
         Index.Indexer[] var2 = this.indexers;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Index.Indexer indexer = var2[var4];
            this.checkNotCompleteAndAdd(() -> {
               return indexer.insertRow(row);
            });
         }

      }

      public void onUpdated(Row existing, Row updated) {
         final Row.Builder toRemove = Row.Builder.sorted();
         toRemove.newRow(existing.clustering());
         toRemove.addPrimaryKeyLivenessInfo(existing.primaryKeyLivenessInfo());
         toRemove.addRowDeletion(existing.deletion());
         final Row.Builder toInsert = Row.Builder.sorted();
         toInsert.newRow(updated.clustering());
         toInsert.addPrimaryKeyLivenessInfo(updated.primaryKeyLivenessInfo());
         toInsert.addRowDeletion(updated.deletion());
         RowDiffListener diffListener = new RowDiffListener() {
            public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original) {
            }

            public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original) {
            }

            public void onComplexDeletion(int i, Clustering clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original) {
            }

            public void onCell(int i, Clustering clustering, Cell merged, Cell original) {
               if(merged != null && !merged.equals(original)) {
                  toInsert.addCell(merged);
               }

               if(merged == null || original != null && WriteTimeTransaction.this.shouldCleanupOldValue(original, merged)) {
                  toRemove.addCell(original);
               }

            }
         };
         Rows.diff(diffListener, updated, new Row[]{existing});
         Row oldRow = toRemove.build();
         Row newRow = toInsert.build();
         Index.Indexer[] var8 = this.indexers;
         int var9 = var8.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            Index.Indexer indexer = var8[var10];
            this.checkNotCompleteAndAdd(() -> {
               return indexer.updateRow(oldRow, newRow);
            });
         }

      }

      public Completable commit() {
         Index.Indexer[] var1 = this.indexers;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            Index.Indexer indexer = var1[var3];
            this.checkNotCompleteAndAdd(indexer::finish);
         }

         return Completable.concat(this.allCompletables);
      }

      private boolean shouldCleanupOldValue(Cell oldCell, Cell newCell) {
         return !oldCell.value().equals(newCell.value()) || oldCell.timestamp() != newCell.timestamp();
      }

      private void checkNotCompleteAndAdd(Supplier<Completable> s) {
         try {
            Completable c = (Completable)s.get();
            if(c != CompletableEmpty.INSTANCE) {
               this.allCompletables.add(c);
            }
         } catch (Exception var3) {
            this.allCompletables.add(Completable.error(var3));
         }

      }
   }
}
