package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.ViewBuilderTask;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionManager implements CompactionManagerMBean {
   public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
   private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
   public static final CompactionManager instance = new CompactionManager();
   public static final int NO_GC = -2147483648;
   public static final int GC_ALL = 2147483647;
   public static final FastThreadLocal<Boolean> isCompactionManager = new FastThreadLocal<Boolean>() {
      protected Boolean initialValue() {
         return Boolean.valueOf(false);
      }
   };
   private final CompactionManager.CompactionExecutor executor = new CompactionManager.CompactionExecutor();
   private final CompactionManager.CompactionExecutor validationExecutor = new CompactionManager.ValidationExecutor();
   private final CompactionManager.CompactionExecutor antiCompactionExecutor = new CompactionManager.CompactionExecutor(1, "AntiCompactionExecutor");
   private static final CompactionManager.CompactionExecutor cacheCleanupExecutor;
   private final CompactionManager.CompactionExecutor viewBuildExecutor = new CompactionManager.ViewBuildExecutor();
   private final CompactionMetrics metrics;
   @VisibleForTesting
   final Multiset<ColumnFamilyStore> compactingCF;
   private final RateLimiter compactionRateLimiter;

   public CompactionManager() {
      this.metrics = new CompactionMetrics(new ThreadPoolExecutor[]{this.executor, this.validationExecutor, this.viewBuildExecutor});
      this.compactingCF = ConcurrentHashMultiset.create();
      this.compactionRateLimiter = RateLimiter.create(1.7976931348623157E308D);
   }

   @VisibleForTesting
   CompactionMetrics getMetrics() {
      return this.metrics;
   }

   public RateLimiter getRateLimiter() {
      this.setRate((double)DatabaseDescriptor.getCompactionThroughputMbPerSec());
      return this.compactionRateLimiter;
   }

   public void setRate(double throughPutMbPerSec) {
      double throughput = throughPutMbPerSec * 1024.0D * 1024.0D;
      if(throughput == 0.0D || StorageService.instance.isBootstrapMode()) {
         throughput = 1.7976931348623157E308D;
      }

      if(this.compactionRateLimiter.getRate() != throughput) {
         this.compactionRateLimiter.setRate(throughput);
      }

   }

   public List<Future<?>> submitBackground(ColumnFamilyStore cfs) {
      if(cfs.isAutoCompactionDisabled()) {
         logger.debug("Autocompaction is disabled");
         return Collections.emptyList();
      } else {
         int count = this.compactingCF.count(cfs);
         if(count > 0 && this.executor.getActiveCount() >= this.executor.getMaximumPoolSize()) {
            logger.trace("Background compaction is still running for {}.{} ({} remaining). Skipping", new Object[]{cfs.keyspace.getName(), cfs.name, Integer.valueOf(count)});
            return Collections.emptyList();
         } else {
            logger.trace("Scheduling a background task check for {}.{} with {}", new Object[]{cfs.keyspace.getName(), cfs.name, cfs.getCompactionStrategyManager().getName()});
            List<Future<?>> futures = new ArrayList(1);
            Future<?> fut = this.executor.submitIfRunning((Runnable)(new CompactionManager.BackgroundCompactionCandidate(cfs)), "background task");
            if(!fut.isCancelled()) {
               futures.add(fut);
            } else {
               this.compactingCF.remove(cfs);
            }

            return futures;
         }
      }
   }

   public boolean isCompacting(Iterable<ColumnFamilyStore> cfses) {
      return this.isCompacting(cfses, Predicates.alwaysTrue(), Predicates.alwaysTrue());
   }

   public boolean isCompacting(Iterable<ColumnFamilyStore> cfses, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate) {
      Iterator var4 = cfses.iterator();

      ColumnFamilyStore cfs;
      do {
         if(!var4.hasNext()) {
            return false;
         }

         cfs = (ColumnFamilyStore)var4.next();
      } while(!cfs.getTracker().getTransactions().stream().anyMatch((txn) -> {
         return opPredicate.apply(txn.opType()) && txn.compactingSet().stream().anyMatch((s) -> {
            return readerPredicate.apply(s);
         });
      }));

      return true;
   }

   public void forceShutdown() {
      this.executor.shutdown();
      this.validationExecutor.shutdown();
      this.antiCompactionExecutor.shutdown();
      this.viewBuildExecutor.shutdown();
      Iterator var1 = CompactionMetrics.getCompactions().iterator();

      while(var1.hasNext()) {
         CompactionInfo.Holder compactionHolder = (CompactionInfo.Holder)var1.next();
         compactionHolder.stop();
      }

      var1 = Arrays.asList(new CompactionManager.CompactionExecutor[]{this.executor, this.validationExecutor, this.antiCompactionExecutor}).iterator();

      while(var1.hasNext()) {
         ExecutorService exec = (ExecutorService)var1.next();

         try {
            if(!exec.awaitTermination(1L, TimeUnit.MINUTES)) {
               logger.warn("Failed to wait for compaction executors shutdown");
            }
         } catch (InterruptedException var4) {
            logger.error("Interrupted while waiting for tasks to be terminated", var4);
         }
      }

   }

   public void finishCompactionsAndShutdown(long timeout, TimeUnit unit) throws InterruptedException {
      this.executor.shutdown();
      this.executor.awaitTermination(timeout, unit);
   }

   private CompactionManager.AllSSTableOpStatus parallelAllSSTableOperation(ColumnFamilyStore cfs, final CompactionManager.OneSSTableOperation operation, int jobs, OperationType operationType) throws ExecutionException, InterruptedException {
      ArrayList transactions = new ArrayList();
      boolean var29 = false;

      CompactionManager.AllSSTableOpStatus var42;
      label430: {
         CompactionManager.AllSSTableOpStatus var43;
         label431: {
            CompactionManager.AllSSTableOpStatus var48;
            label432: {
               CompactionManager.AllSSTableOpStatus var45;
               try {
                  var29 = true;
                  LifecycleTransaction compacting = cfs.markAllCompacting(operationType);
                  Throwable var7 = null;

                  label420: {
                     label419: {
                        label418: {
                           try {
                              if(compacting == null) {
                                 var42 = CompactionManager.AllSSTableOpStatus.UNABLE_TO_CANCEL;
                                 break label418;
                              }

                              Iterable<SSTableReader> sstables = Lists.newArrayList(operation.filterSSTables(compacting));
                              if(Iterables.isEmpty(sstables)) {
                                 logger.info("No sstables to {} for {}.{}", new Object[]{operationType.name(), cfs.keyspace.getName(), cfs.name});
                                 var43 = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
                                 break label419;
                              }

                              List<Future<?>> futures = new ArrayList();
                              Iterator var10 = sstables.iterator();

                              while(true) {
                                 if(!var10.hasNext()) {
                                    FBUtilities.waitOnFutures(futures);

                                    assert compacting.originals().isEmpty();

                                    var45 = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
                                    break label420;
                                 }

                                 SSTableReader sstable = (SSTableReader)var10.next();
                                 final LifecycleTransaction txn = compacting.split(Collections.singleton(sstable));
                                 transactions.add(txn);
                                 Callable<Object> callable = new Callable<Object>() {
                                    public Object call() throws Exception {
                                       operation.execute(txn);
                                       return this;
                                    }
                                 };
                                 Future<?> fut = this.executor.submitIfRunning(callable, "paralell sstable operation");
                                 if(fut.isCancelled()) {
                                    var48 = CompactionManager.AllSSTableOpStatus.ABORTED;
                                    break;
                                 }

                                 futures.add(fut);
                                 if(jobs > 0 && futures.size() == jobs) {
                                    Future<?> f = FBUtilities.waitOnFirstFuture(futures);
                                    futures.remove(f);
                                 }
                              }
                           } catch (Throwable var39) {
                              var7 = var39;
                              throw var39;
                           } finally {
                              if(compacting != null) {
                                 if(var7 != null) {
                                    try {
                                       compacting.close();
                                    } catch (Throwable var38) {
                                       var7.addSuppressed(var38);
                                    }
                                 } else {
                                    compacting.close();
                                 }
                              }

                           }

                           var29 = false;
                           break label432;
                        }

                        var29 = false;
                        break label430;
                     }

                     var29 = false;
                     break label431;
                  }

                  var29 = false;
               } finally {
                  if(var29) {
                     Throwable fail = Throwables.close((Throwable)null, transactions);
                     if(fail != null) {
                        logger.error("Failed to cleanup lifecycle transactions", fail);
                     }

                  }
               }

               Throwable fail = Throwables.close((Throwable)null, transactions);
               if(fail != null) {
                  logger.error("Failed to cleanup lifecycle transactions", fail);
               }

               return var45;
            }

            Throwable fail = Throwables.close((Throwable)null, transactions);
            if(fail != null) {
               logger.error("Failed to cleanup lifecycle transactions", fail);
            }

            return var48;
         }

         Throwable fail = Throwables.close((Throwable)null, transactions);
         if(fail != null) {
            logger.error("Failed to cleanup lifecycle transactions", fail);
         }

         return var43;
      }

      Throwable fail = Throwables.close((Throwable)null, transactions);
      if(fail != null) {
         logger.error("Failed to cleanup lifecycle transactions", fail);
      }

      return var42;
   }

   public CompactionManager.AllSSTableOpStatus performScrub(ColumnFamilyStore cfs, boolean skipCorrupted, boolean checkData, int jobs) throws InterruptedException, ExecutionException {
      return this.performScrub(cfs, skipCorrupted, checkData, false, jobs);
   }

   public CompactionManager.AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData, final boolean reinsertOverflowedTTL, int jobs) throws InterruptedException, ExecutionException {
      return this.parallelAllSSTableOperation(cfs, new CompactionManager.OneSSTableOperation() {
         public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input) {
            return input.originals();
         }

         public void execute(LifecycleTransaction input) {
            CompactionManager.this.scrubOne(cfs, input, skipCorrupted, checkData, reinsertOverflowedTTL);
         }
      }, jobs, OperationType.SCRUB);
   }

   public CompactionManager.AllSSTableOpStatus performVerify(final ColumnFamilyStore cfs, final boolean extendedVerify) throws InterruptedException, ExecutionException {
      assert !cfs.isIndex();

      return this.parallelAllSSTableOperation(cfs, new CompactionManager.OneSSTableOperation() {
         public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input) {
            return input.originals();
         }

         public void execute(LifecycleTransaction input) throws IOException {
            CompactionManager.this.verifyOne(cfs, input.onlyOne(), extendedVerify);
         }
      }, 0, OperationType.VERIFY);
   }

   public CompactionManager.AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion, int jobs) throws InterruptedException, ExecutionException {
      return this.parallelAllSSTableOperation(cfs, new CompactionManager.OneSSTableOperation() {
         public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction) {
            List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
            Collections.sort(sortedSSTables, SSTableReader.sizeComparator.reversed());
            Iterator iter = sortedSSTables.iterator();

            while(iter.hasNext()) {
               SSTableReader sstable = (SSTableReader)iter.next();
               if(excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion())) {
                  transaction.cancel(sstable);
                  iter.remove();
               }
            }

            return sortedSSTables;
         }

         public void execute(LifecycleTransaction txn) {
            AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, -2147483648, 9223372036854775807L);
            task.setUserDefined(true);
            task.setCompactionType(OperationType.UPGRADE_SSTABLES);
            task.execute(CompactionManager.this.metrics);
         }
      }, jobs, OperationType.UPGRADE_SSTABLES);
   }

   public CompactionManager.AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException {
      assert !cfStore.isIndex();

      Keyspace keyspace = cfStore.keyspace;
      if(!StorageService.instance.isJoined()) {
         logger.info("Cleanup cannot run before a node has joined the ring");
         return CompactionManager.AllSSTableOpStatus.ABORTED;
      } else {
         final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
         final boolean hasIndexes = cfStore.indexManager.hasIndexes();
         return this.parallelAllSSTableOperation(cfStore, new CompactionManager.OneSSTableOperation() {
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction) {
               List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
               Collections.sort(sortedSSTables, SSTableReader.sizeComparator);
               return sortedSSTables;
            }

            public void execute(LifecycleTransaction txn) throws IOException {
               CompactionManager.CleanupStrategy cleanupStrategy = CompactionManager.CleanupStrategy.get(cfStore, ranges, FBUtilities.nowInSeconds());
               CompactionManager.this.doCleanupOne(cfStore, txn, cleanupStrategy, ranges, hasIndexes);
            }
         }, jobs, OperationType.CLEANUP);
      }
   }

   public CompactionManager.AllSSTableOpStatus performGarbageCollection(final ColumnFamilyStore cfStore, final CompactionParams.TombstoneOption tombstoneOption, int jobs) throws InterruptedException, ExecutionException {
      assert !cfStore.isIndex();

      return this.parallelAllSSTableOperation(cfStore, new CompactionManager.OneSSTableOperation() {
         public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction) {
            Iterable<SSTableReader> originals = transaction.originals();
            if(cfStore.getCompactionStrategyManager().onlyPurgeRepairedTombstones()) {
               originals = Iterables.filter((Iterable)originals, SSTableReader::isRepaired);
            }

            List<SSTableReader> sortedSSTables = Lists.newArrayList((Iterable)originals);
            Collections.sort(sortedSSTables, SSTableReader.maxTimestampComparator);
            return sortedSSTables;
         }

         public void execute(LifecycleTransaction txn) throws IOException {
            CompactionManager.logger.debug("Garbage collecting {}", txn.originals());
            CompactionTask task = new CompactionTask(cfStore, txn, CompactionManager.getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds())) {
               protected CompactionController getCompactionController(Set<SSTableReader> toCompact) {
                  return new CompactionController(cfStore, toCompact, this.gcBefore, (RateLimiter)null, tombstoneOption);
               }
            };
            task.setUserDefined(true);
            task.setCompactionType(OperationType.GARBAGE_COLLECT);
            task.execute(CompactionManager.this.metrics);
         }
      }, jobs, OperationType.GARBAGE_COLLECT);
   }

   public CompactionManager.AllSSTableOpStatus relocateSSTables(final ColumnFamilyStore cfs, int jobs) throws ExecutionException, InterruptedException {
      if(!cfs.getPartitioner().splitter().isPresent()) {
         logger.info("Partitioner does not support splitting");
         return CompactionManager.AllSSTableOpStatus.ABORTED;
      } else {
         Collection<Range<Token>> r = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
         if(r.isEmpty()) {
            logger.info("Relocate cannot run before a node has joined the ring");
            return CompactionManager.AllSSTableOpStatus.ABORTED;
         } else {
            final DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();
            return this.parallelAllSSTableOperation(cfs, new CompactionManager.OneSSTableOperation() {
               public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction) {
                  Set<SSTableReader> originals = Sets.newHashSet(transaction.originals());
                  Set<SSTableReader> needsRelocation = (Set)originals.stream().filter((s) -> {
                     return !this.inCorrectLocation(s);
                  }).collect(Collectors.toSet());
                  transaction.cancel((Iterable)Sets.difference(originals, needsRelocation));
                  Map<Integer, List<SSTableReader>> groupedByDisk = this.groupByDiskIndex(needsRelocation);
                  int maxSize = 0;

                  List diskSSTables;
                  for(Iterator var6 = groupedByDisk.values().iterator(); var6.hasNext(); maxSize = Math.max(maxSize, diskSSTables.size())) {
                     diskSSTables = (List)var6.next();
                  }

                  List<SSTableReader> mixedSSTables = new ArrayList();

                  for(int i = 0; i < maxSize; ++i) {
                     Iterator var8 = groupedByDisk.values().iterator();

                     while(var8.hasNext()) {
                        List<SSTableReader> diskSSTablesx = (List)var8.next();
                        if(i < diskSSTablesx.size()) {
                           mixedSSTables.add(diskSSTablesx.get(i));
                        }
                     }
                  }

                  return mixedSSTables;
               }

               public Map<Integer, List<SSTableReader>> groupByDiskIndex(Set<SSTableReader> needsRelocation) {
                  return (Map)needsRelocation.stream().collect(Collectors.groupingBy((s) -> {
                     return Integer.valueOf(diskBoundaries.getDiskIndex(s));
                  }));
               }

               private boolean inCorrectLocation(SSTableReader sstable) {
                  if(!cfs.getPartitioner().splitter().isPresent()) {
                     return true;
                  } else {
                     int diskIndex = diskBoundaries.getDiskIndex(sstable);
                     File diskLocation = ((Directories.DataDirectory)diskBoundaries.directories.get(diskIndex)).location;
                     PartitionPosition diskLast = (PartitionPosition)diskBoundaries.positions.get(diskIndex);
                     return sstable.descriptor.directory.getAbsolutePath().startsWith(diskLocation.getAbsolutePath()) && sstable.last.compareTo(diskLast) <= 0;
                  }
               }

               public void execute(LifecycleTransaction txn) {
                  CompactionManager.logger.debug("Relocating {}", txn.originals());
                  AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, -2147483648, 9223372036854775807L);
                  task.setUserDefined(true);
                  task.setCompactionType(OperationType.RELOCATE);
                  task.execute(CompactionManager.this.metrics);
               }
            }, jobs, OperationType.RELOCATE);
         }
      }
   }

   public ListenableFuture<?> submitPendingAntiCompaction(final ColumnFamilyStore cfs, final Collection<Range<Token>> ranges, final Refs<SSTableReader> sstables, final LifecycleTransaction txn, final UUID sessionId) {
      Runnable runnable = new WrappedRunnable() {
         protected void runMayThrow() throws Exception {
            Timer.Context ctx = cfs.metric.anticompactionTime.timer();
            Throwable var2 = null;

            try {
               CompactionManager.this.performAnticompaction(cfs, ranges, sstables, txn, 0L, sessionId, sessionId);
            } catch (Throwable var11) {
               var2 = var11;
               throw var11;
            } finally {
               if(ctx != null) {
                  if(var2 != null) {
                     try {
                        ctx.close();
                     } catch (Throwable var10) {
                        var2.addSuppressed(var10);
                     }
                  } else {
                     ctx.close();
                  }
               }

            }

         }
      };
      ListenableFuture task = null;

      ListenableFuture var8;
      try {
         task = this.antiCompactionExecutor.submitIfRunning((Runnable)runnable, "pending anticompaction");
         var8 = task;
      } finally {
         if(task == null || task.isCancelled()) {
            sstables.release();
            txn.abort();
         }

      }

      return var8;
   }


   public void performAnticompaction(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, Refs<SSTableReader> validatedForRepair, LifecycleTransaction txn, long repairedAt, UUID pendingRepair, UUID parentRepairSession) throws InterruptedException, IOException {
      try {
         ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(parentRepairSession);
         Preconditions.checkArgument((boolean)(!prs.isPreview()), (Object)"Cannot anticompact for previews");
         logger.info("{} Starting anticompaction for {}.{} on {}/{} sstables", new Object[]{PreviewKind.NONE.logPrefix(parentRepairSession), cfs.keyspace.getName(), cfs.getTableName(), validatedForRepair.size(), cfs.getLiveSSTables().size()});
         logger.trace("{} Starting anticompaction for ranges {}", (Object)PreviewKind.NONE.logPrefix(parentRepairSession), ranges);
         HashSet<SSTableReader> sstables = new HashSet<SSTableReader>(validatedForRepair);
         HashSet<SSTableReader> sstablesToMutateRepairStatus = new HashSet<SSTableReader>();
         HashSet<SSTableReader> nonAnticompacting = new HashSet<SSTableReader>();
         Iterator<SSTableReader> sstableIterator = sstables.iterator();
         List<Range<Token>> normalizedRanges = Range.normalize(ranges);
         while (sstableIterator.hasNext()) {
            SSTableReader sstable = sstableIterator.next();
            Range<Token> sstableRange = new Range<Token>(sstable.first.getToken(), sstable.last.getToken());
            boolean shouldAnticompact = false;
            for (Range<Token> r : normalizedRanges) {
               if (r.contains(sstableRange)) {
                  logger.info("[repair #{}] SSTable {} fully contained in range {}, mutating repairedAt instead of anticompacting", new Object[]{parentRepairSession, sstable, r});
                  sstablesToMutateRepairStatus.add(sstable);
                  sstableIterator.remove();
                  shouldAnticompact = true;
                  break;
               }
               if (!sstableRange.intersects((Range<Token>)r)) continue;
               logger.info("{} SSTable {} ({}) will be anticompacted on range {}", new Object[]{PreviewKind.NONE.logPrefix(parentRepairSession), sstable, sstableRange, r});
               shouldAnticompact = true;
            }
            if (shouldAnticompact) continue;
            logger.info("{} SSTable {} ({}) does not intersect repaired ranges {}, not touching repairedAt.", new Object[]{PreviewKind.NONE.logPrefix(parentRepairSession), sstable, sstableRange, normalizedRanges});
            nonAnticompacting.add(sstable);
            sstableIterator.remove();
         }
         cfs.getCompactionStrategyManager().mutateRepaired(sstablesToMutateRepairStatus, repairedAt, pendingRepair);
         cfs.metric.bytesMutatedAnticompaction.inc(SSTableReader.getTotalBytes(sstablesToMutateRepairStatus));
         txn.cancel((Iterable<SSTableReader>)Sets.union(nonAnticompacting, sstablesToMutateRepairStatus));
         validatedForRepair.release((Collection<SSTableReader>)Sets.union(nonAnticompacting, sstablesToMutateRepairStatus));
         assert (txn.originals().equals(sstables));
         if (!sstables.isEmpty()) {
            this.doAntiCompaction(cfs, ranges, txn, repairedAt, pendingRepair);
         }
         txn.finish();
      }
      finally {
         validatedForRepair.release();
         txn.close();
      }
      logger.info("{} Completed anticompaction successfully", (Object)PreviewKind.NONE.logPrefix(parentRepairSession));
   }

   public void performMaximal(ColumnFamilyStore cfStore, boolean splitOutput) {
      FBUtilities.waitOnFutures(this.submitMaximal(cfStore, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()), splitOutput));
   }

   public List<Future<?>> submitMaximal(ColumnFamilyStore cfStore, int gcBefore, boolean splitOutput) {
      Collection<AbstractCompactionTask> tasks = cfStore.getCompactionStrategyManager().getMaximalTasks(gcBefore, splitOutput);
      if(tasks == null) {
         return Collections.emptyList();
      } else {
         List<Future<?>> futures = new ArrayList();
         int nonEmptyTasks = 0;
         Iterator var7 = tasks.iterator();

         while(var7.hasNext()) {
            final AbstractCompactionTask task = (AbstractCompactionTask)var7.next();
            if(task.transaction.originals().size() > 0) {
               ++nonEmptyTasks;
            }

            Runnable runnable = new WrappedRunnable() {
               protected void runMayThrow() {
                  task.execute(CompactionManager.this.metrics);
               }
            };
            Future<?> fut = this.executor.submitIfRunning((Runnable)runnable, "maximal task");
            if(!fut.isCancelled()) {
               futures.add(fut);
            }
         }

         if(nonEmptyTasks > 1) {
            logger.info("Major compaction will not result in a single sstable - repaired and unrepaired data is kept separate and compaction runs per data_file_directory.");
         }

         return futures;
      }
   }

   public void forceCompactionForTokenRange(ColumnFamilyStore cfStore, Collection<Range<Token>> ranges) {
      final Collection<AbstractCompactionTask> tasks = (Collection)cfStore.runWithCompactionsDisabled(() -> {
         Collection<SSTableReader> sstables = sstablesInBounds(cfStore, ranges);
         if(sstables != null && !sstables.isEmpty()) {
            return cfStore.getCompactionStrategyManager().getUserDefinedTasks(sstables, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()));
         } else {
            logger.debug("No sstables found for the provided token range");
            return null;
         }
      }, false, false);
      if(tasks != null) {
         Runnable runnable = new WrappedRunnable() {
            protected void runMayThrow() {
               Iterator var1 = tasks.iterator();

               while(var1.hasNext()) {
                  AbstractCompactionTask task = (AbstractCompactionTask)var1.next();
                  if(task != null) {
                     task.execute(CompactionManager.this.metrics);
                  }
               }

            }
         };
         if(this.executor.isShutdown()) {
            logger.info("Compaction executor has shut down, not submitting task");
         } else {
            FBUtilities.waitOnFuture(this.executor.submit(runnable));
         }
      }
   }

   private static Collection<SSTableReader> sstablesInBounds(ColumnFamilyStore cfs, Collection<Range<Token>> tokenRangeCollection) {
      Set<SSTableReader> sstables = new HashSet();
      Iterable<SSTableReader> liveTables = cfs.getTracker().getView().select(SSTableSet.LIVE);
      SSTableIntervalTree tree = SSTableIntervalTree.build(liveTables);
      Iterator var5 = tokenRangeCollection.iterator();

      while(var5.hasNext()) {
         Range<Token> tokenRange = (Range)var5.next();
         Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(((Token)tokenRange.left).minKeyBound(), ((Token)tokenRange.right).maxKeyBound(), tree);
         Iterables.addAll(sstables, ssTableReaders);
      }

      return sstables;
   }

   public void forceUserDefinedCompaction(String dataFiles) {
      String[] filenames = dataFiles.split(",");
      Multimap<ColumnFamilyStore, Descriptor> descriptors = ArrayListMultimap.create();
      String[] var4 = filenames;
      int nowInSec = filenames.length;

      for(int var6 = 0; var6 < nowInSec; ++var6) {
         String filename = var4[var6];
         Descriptor desc = Descriptor.fromFilename(filename.trim());
         if(Schema.instance.getTableMetadataRef(desc) == null) {
            logger.warn("Schema does not exist for file {}. Skipping.", filename);
         } else {
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            descriptors.put(cfs, cfs.getDirectories().find((new File(filename.trim())).getName()));
         }
      }

      List<Future<?>> futures = new ArrayList(descriptors.size());
      nowInSec = FBUtilities.nowInSeconds();
      Iterator var11 = descriptors.keySet().iterator();

      while(var11.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var11.next();
         futures.add(this.submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs, nowInSec)));
      }

      FBUtilities.waitOnFutures(futures);
   }

   public void forceUserDefinedCleanup(String dataFiles) {
      String[] filenames = dataFiles.split(",");
      HashMap<ColumnFamilyStore, Descriptor> descriptors = Maps.newHashMap();
      String[] var4 = filenames;
      int var5 = filenames.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String filename = var4[var6];
         Descriptor desc = Descriptor.fromFilename(filename.trim());
         if(Schema.instance.getTableMetadataRef(desc) == null) {
            logger.warn("Schema does not exist for file {}. Skipping.", filename);
         } else {
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            desc = cfs.getDirectories().find((new File(filename.trim())).getName());
            if(desc != null) {
               descriptors.put(cfs, desc);
            }
         }
      }

      if(!StorageService.instance.isJoined()) {
         logger.error("Cleanup cannot run before a node has joined the ring");
      } else {
         Iterator var26 = descriptors.entrySet().iterator();

         while(true) {
            while(var26.hasNext()) {
               Entry<ColumnFamilyStore, Descriptor> entry = (Entry)var26.next();
               ColumnFamilyStore cfs = (ColumnFamilyStore)entry.getKey();
               Keyspace keyspace = cfs.keyspace;
               Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
               boolean hasIndexes = cfs.indexManager.hasIndexes();
               SSTableReader sstable = this.lookupSSTable(cfs, (Descriptor)entry.getValue());
               if(sstable == null) {
                  logger.warn("Will not clean {}, it is not an active sstable", entry.getValue());
               } else {
                  CompactionManager.CleanupStrategy cleanupStrategy = CompactionManager.CleanupStrategy.get(cfs, ranges, FBUtilities.nowInSeconds());

                  try {
                     LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.CLEANUP);
                     Throwable var13 = null;

                     try {
                        this.doCleanupOne(cfs, txn, cleanupStrategy, ranges, hasIndexes);
                     } catch (Throwable var23) {
                        var13 = var23;
                        throw var23;
                     } finally {
                        if(txn != null) {
                           if(var13 != null) {
                              try {
                                 txn.close();
                              } catch (Throwable var22) {
                                 var13.addSuppressed(var22);
                              }
                           } else {
                              txn.close();
                           }
                        }

                     }
                  } catch (IOException var25) {
                     logger.error("forceUserDefinedCleanup failed: {}", var25.getLocalizedMessage());
                  }
               }
            }

            return;
         }
      }
   }

   public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore) {
      Runnable runnable = new WrappedRunnable() {
         protected void runMayThrow() {
            Collection<SSTableReader> sstables = new ArrayList(dataFiles.size());
            Iterator var2 = dataFiles.iterator();

            while(var2.hasNext()) {
               Descriptor desc = (Descriptor)var2.next();
               SSTableReader sstable = CompactionManager.this.lookupSSTable(cfs, desc);
               if(sstable == null) {
                  CompactionManager.logger.info("Will not compact {}: it is not an active sstable", desc);
               } else {
                  sstables.add(sstable);
               }
            }

            if(sstables.isEmpty()) {
               CompactionManager.logger.info("No files to compact for user defined compaction");
            } else {
               List<AbstractCompactionTask> tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstables, gcBefore);
               Iterator var6 = tasks.iterator();

               while(var6.hasNext()) {
                  AbstractCompactionTask task = (AbstractCompactionTask)var6.next();
                  if(task != null) {
                     task.execute(CompactionManager.this.metrics);
                  }
               }
            }

         }
      };
      return this.executor.submitIfRunning((Runnable)runnable, "user defined task");
   }

   private SSTableReader lookupSSTable(ColumnFamilyStore cfs, Descriptor descriptor) {
      Iterator var3 = cfs.getSSTables(SSTableSet.CANONICAL).iterator();

      SSTableReader sstable;
      do {
         if(!var3.hasNext()) {
            return null;
         }

         sstable = (SSTableReader)var3.next();
      } while(!sstable.descriptor.equals(descriptor));

      return sstable;
   }

   public Future<?> submitValidation(final ColumnFamilyStore cfStore, final Validator validator) {
      Callable<Object> callable = new Callable<Object>() {
         public Object call() throws IOException {
            try {
               Timer.Context c = cfStore.metric.validationTime.timer();
               Throwable var2 = null;

               try {
                  CompactionManager.this.doValidationCompaction(cfStore, validator);
               } catch (Throwable var12) {
                  var2 = var12;
                  throw var12;
               } finally {
                  if(c != null) {
                     if(var2 != null) {
                        try {
                           c.close();
                        } catch (Throwable var11) {
                           var2.addSuppressed(var11);
                        }
                     } else {
                        c.close();
                     }
                  }

               }

               return this;
            } catch (Throwable var14) {
               CompactionManager.logger.debug("Error during validation", var14);
               validator.fail();
               throw var14;
            }
         }
      };
      return this.validationExecutor.submitIfRunning(callable, "validation");
   }

   public void disableAutoCompaction() {
      UnmodifiableIterator var1 = Schema.instance.getNonSystemKeyspaces().iterator();

      while(var1.hasNext()) {
         String ksname = (String)var1.next();
         Iterator var3 = Keyspace.open(ksname).getColumnFamilyStores().iterator();

         while(var3.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
            cfs.disableAutoCompaction();
         }
      }

   }

   private void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL) {
      CompactionInfo.Holder scrubInfo = null;

      try {
         Scrubber scrubber = new Scrubber(cfs, modifier, skipCorrupted, checkData, reinsertOverflowedTTL);
         Throwable var8 = null;

         try {
            scrubInfo = scrubber.getScrubInfo();
            this.metrics.beginCompaction(scrubInfo);
            scrubber.scrub();
         } catch (Throwable var24) {
            var8 = var24;
            throw var24;
         } finally {
            if(scrubber != null) {
               if(var8 != null) {
                  try {
                     scrubber.close();
                  } catch (Throwable var23) {
                     var8.addSuppressed(var23);
                  }
               } else {
                  scrubber.close();
               }
            }

         }
      } finally {
         if(scrubInfo != null) {
            this.metrics.finishCompaction(scrubInfo);
         }

      }

   }

   private void verifyOne(ColumnFamilyStore cfs, SSTableReader sstable, boolean extendedVerify) throws IOException {
      CompactionInfo.Holder verifyInfo = null;

      try {
         Verifier verifier = new Verifier(cfs, sstable, false);
         Throwable var6 = null;

         try {
            verifyInfo = verifier.getVerifyInfo();
            this.metrics.beginCompaction(verifyInfo);
            verifier.verify(extendedVerify);
         } catch (Throwable var22) {
            var6 = var22;
            throw var22;
         } finally {
            if(verifier != null) {
               if(var6 != null) {
                  try {
                     verifier.close();
                  } catch (Throwable var21) {
                     var6.addSuppressed(var21);
                  }
               } else {
                  verifier.close();
               }
            }

         }
      } finally {
         if(verifyInfo != null) {
            this.metrics.finishCompaction(verifyInfo);
         }

      }

   }

   @VisibleForTesting
   public static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges) {
      if(ownedRanges.isEmpty()) {
         return true;
      } else {
         List<Range<Token>> sortedRanges = Range.normalize(ownedRanges);
         Range<Token> firstRange = (Range)sortedRanges.get(0);
         if(sstable.first.getToken().compareTo(firstRange.left) <= 0) {
            return true;
         } else {
            for(int i = 0; i < sortedRanges.size(); ++i) {
               Range<Token> range = (Range)sortedRanges.get(i);
               if(((Token)range.right).isMinimum()) {
                  return false;
               }

               DecoratedKey firstBeyondRange = sstable.firstKeyBeyond(((Token)range.right).maxKeyBound());
               if(firstBeyondRange == null) {
                  return false;
               }

               if(i == sortedRanges.size() - 1) {
                  return true;
               }

               Range<Token> nextRange = (Range)sortedRanges.get(i + 1);
               if(firstBeyondRange.getToken().compareTo(nextRange.left) <= 0) {
                  return true;
               }
            }

            return false;
         }
      }
   }

   private void doCleanupOne(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionManager.CleanupStrategy cleanupStrategy, Collection<Range<Token>> ranges, boolean hasIndexes) throws IOException {
      assert !cfs.isIndex();

      SSTableReader sstable = txn.onlyOne();
      if(!hasIndexes && !(new Bounds(sstable.first.getToken(), sstable.last.getToken())).intersects(ranges)) {
         txn.obsoleteOriginals();
         txn.finish();
      } else if(!needsCleanup(sstable, ranges)) {
         logger.trace("Skipping {} for cleanup; all rows should be kept", sstable);
      } else {
         long start = System.nanoTime();
         long totalkeysWritten = 0L;
         long expectedBloomFilterSize = Math.max((long)cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(txn.originals()));
         if(logger.isTraceEnabled()) {
            logger.trace("Expected bloom filter size : {}", Long.valueOf(expectedBloomFilterSize));
         }

         logger.info("Cleaning up {}", sstable);
         long writeSize = cfs.getExpectedCompactedFileSize(txn.originals(), OperationType.CLEANUP);
         File compactionFileLocation = cfs.getDirectories().getWriteableLocationAsFile(cfs, sstable, writeSize);
         if(compactionFileLocation == null) {
            throw new IOException("disk full");
         } else {
            RateLimiter limiter = this.getRateLimiter();
            double compressionRatio = sstable.getCompressionRatio();
            if(compressionRatio == -1.0D) {
               compressionRatio = 1.0D;
            }

            int nowInSec = FBUtilities.nowInSeconds();
            SSTableRewriter writer = SSTableRewriter.construct(cfs, txn, false, sstable.maxDataAge);
            Throwable var22 = null;

            List finished;
            try {
               ISSTableScanner scanner = cleanupStrategy.getScanner(sstable);
               Throwable var24 = null;

               try {
                  CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs, nowInSec));
                  Throwable var26 = null;

                  try {
                     Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable));
                     Throwable var28 = null;

                     try {
                        CompactionIterator ci = new CompactionIterator(OperationType.CLEANUP, Collections.singletonList(scanner), controller, nowInSec, UUIDGen.getTimeUUID(), this.metrics);
                        Throwable var30 = null;

                        try {
                           StatsMetadata metadata = sstable.getSSTableMetadata();
                           writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, sstable, txn));
                           long lastBytesScanned = 0L;

                           while(ci.hasNext()) {
                              if(ci.isStopRequested()) {
                                 throw new CompactionInterruptedException(ci.getCompactionInfo());
                              }

                              UnfilteredRowIterator partition = ci.next();
                              Throwable var35 = null;

                              try {
                                 UnfilteredRowIterator notCleaned = cleanupStrategy.cleanup(partition);
                                 Throwable var37 = null;

                                 try {
                                    if(notCleaned != null) {
                                       if(writer.append(notCleaned) != null) {
                                          ++totalkeysWritten;
                                       }

                                       long bytesScanned = scanner.getBytesScanned();
                                       compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);
                                       lastBytesScanned = bytesScanned;
                                    }
                                 } catch (Throwable var249) {
                                    var37 = var249;
                                    throw var249;
                                 } finally {
                                    if(notCleaned != null) {
                                       if(var37 != null) {
                                          try {
                                             notCleaned.close();
                                          } catch (Throwable var248) {
                                             var37.addSuppressed(var248);
                                          }
                                       } else {
                                          notCleaned.close();
                                       }
                                    }

                                 }
                              } catch (Throwable var251) {
                                 var35 = var251;
                                 throw var251;
                              } finally {
                                 if(partition != null) {
                                    if(var35 != null) {
                                       try {
                                          partition.close();
                                       } catch (Throwable var247) {
                                          var35.addSuppressed(var247);
                                       }
                                    } else {
                                       partition.close();
                                    }
                                 }

                              }
                           }

                           cfs.indexManager.flushAllIndexesBlocking();
                           finished = writer.finish();
                        } catch (Throwable var253) {
                           var30 = var253;
                           throw var253;
                        } finally {
                           if(ci != null) {
                              if(var30 != null) {
                                 try {
                                    ci.close();
                                 } catch (Throwable var246) {
                                    var30.addSuppressed(var246);
                                 }
                              } else {
                                 ci.close();
                              }
                           }

                        }
                     } catch (Throwable var255) {
                        var28 = var255;
                        throw var255;
                     } finally {
                        if(refs != null) {
                           if(var28 != null) {
                              try {
                                 refs.close();
                              } catch (Throwable var245) {
                                 var28.addSuppressed(var245);
                              }
                           } else {
                              refs.close();
                           }
                        }

                     }
                  } catch (Throwable var257) {
                     var26 = var257;
                     throw var257;
                  } finally {
                     if(controller != null) {
                        if(var26 != null) {
                           try {
                              controller.close();
                           } catch (Throwable var244) {
                              var26.addSuppressed(var244);
                           }
                        } else {
                           controller.close();
                        }
                     }

                  }
               } catch (Throwable var259) {
                  var24 = var259;
                  throw var259;
               } finally {
                  if(scanner != null) {
                     if(var24 != null) {
                        try {
                           scanner.close();
                        } catch (Throwable var243) {
                           var24.addSuppressed(var243);
                        }
                     } else {
                        scanner.close();
                     }
                  }

               }
            } catch (Throwable var261) {
               var22 = var261;
               throw var261;
            } finally {
               if(writer != null) {
                  if(var22 != null) {
                     try {
                        writer.close();
                     } catch (Throwable var242) {
                        var22.addSuppressed(var242);
                     }
                  } else {
                     writer.close();
                  }
               }

            }

            if(!finished.isEmpty()) {
               String format = "Cleaned up to %s.  %s to %s (~%d%% of original) for %,d keys.  Time: %,dms.";
               long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
               long startsize = sstable.onDiskLength();
               long endsize = 0L;

               SSTableReader newSstable;
               for(Iterator var267 = finished.iterator(); var267.hasNext(); endsize += newSstable.onDiskLength()) {
                  newSstable = (SSTableReader)var267.next();
               }

               double ratio = (double)endsize / (double)startsize;
               logger.info(String.format(format, new Object[]{((SSTableReader)finished.get(0)).getFilename(), FBUtilities.prettyPrintMemory(startsize), FBUtilities.prettyPrintMemory(endsize), Integer.valueOf((int)(ratio * 100.0D)), Long.valueOf(totalkeysWritten), Long.valueOf(dTime)}));
            }

         }
      }
   }

   static void compactionRateLimiterAcquire(RateLimiter limiter, long bytesScanned, long lastBytesScanned, double compressionRatio) {
      long lengthRead;
      for(lengthRead = (long)((double)(bytesScanned - lastBytesScanned) * compressionRatio) + 1L; lengthRead >= 2147483647L; lengthRead -= 2147483647L) {
         limiter.acquire(2147483647);
      }

      if(lengthRead > 0L) {
         limiter.acquire((int)lengthRead);
      }

   }

   public static SSTableWriter createWriter(ColumnFamilyStore cfs, File compactionFileLocation, long expectedBloomFilterSize, long repairedAt, UUID pendingRepair, SSTableReader sstable, LifecycleTransaction txn) {
      FileUtils.createDirectory(compactionFileLocation);
      return SSTableWriter.create(cfs.metadata, cfs.newSSTableDescriptor(compactionFileLocation), expectedBloomFilterSize, repairedAt, pendingRepair, sstable.getSSTableLevel(), sstable.header, cfs.indexManager.listIndexes(), txn);
   }

   public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs, File compactionFileLocation, int expectedBloomFilterSize, long repairedAt, UUID pendingRepair, Collection<SSTableReader> sstables, LifecycleTransaction txn) {
      FileUtils.createDirectory(compactionFileLocation);
      int minLevel = 2147483647;
      Iterator var9 = sstables.iterator();

      while(var9.hasNext()) {
         SSTableReader sstable = (SSTableReader)var9.next();
         if(minLevel == 2147483647) {
            minLevel = sstable.getSSTableLevel();
         }

         if(minLevel != sstable.getSSTableLevel()) {
            minLevel = 0;
            break;
         }
      }

      return SSTableWriter.create(cfs.newSSTableDescriptor(compactionFileLocation), Long.valueOf((long)expectedBloomFilterSize), Long.valueOf(repairedAt), pendingRepair, cfs.metadata, new MetadataCollector(sstables, cfs.metadata().comparator, minLevel), SerializationHeader.make(cfs.metadata(), sstables), cfs.indexManager.listIndexes(), txn);
   }

   private void doValidationCompaction(ColumnFamilyStore cfs, Validator validator) throws IOException {
      if(cfs.isValid()) {
         Refs sstables = null;

         try {
            UUID parentRepairSessionId = validator.desc.parentSessionId;
            boolean isGlobalSnapshotValidation = cfs.snapshotExists(parentRepairSessionId.toString());
            String snapshotName;
            if(isGlobalSnapshotValidation) {
               snapshotName = parentRepairSessionId.toString();
            } else {
               snapshotName = validator.desc.sessionId.toString();
            }

            boolean isSnapshotValidation = cfs.snapshotExists(snapshotName);
            if(isSnapshotValidation) {
               sstables = cfs.getSnapshotSSTableReaders(snapshotName);
            } else {
               if(!validator.isIncremental) {
                  StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), new String[]{cfs.name});
               }

               sstables = this.getSSTablesToValidate(cfs, validator);
               if(sstables == null) {
                  return;
               }
            }

            MerkleTrees tree = createMerkleTrees(sstables, validator.desc.ranges, cfs);
            long start = System.nanoTime();
            long partitionCount = 0L;

            try {
               AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables, validator.desc.ranges);
               Throwable var14 = null;

               try {
                  CompactionManager.ValidationCompactionController controller = new CompactionManager.ValidationCompactionController(cfs, getDefaultGcBefore(cfs, validator.nowInSec));
                  Throwable var16 = null;

                  try {
                     CompactionIterator ci = new CompactionManager.ValidationCompactionIterator(scanners.scanners, controller, validator.nowInSec, this.metrics);
                     Throwable var18 = null;

                     try {
                        validator.prepare(cfs, tree);

                        while(ci.hasNext()) {
                           if(ci.isStopRequested()) {
                              throw new CompactionInterruptedException(ci.getCompactionInfo());
                           }

                           UnfilteredRowIterator partition = ci.next();
                           Throwable var20 = null;

                           try {
                              validator.add(partition);
                              ++partitionCount;
                           } catch (Throwable var138) {
                              var20 = var138;
                              throw var138;
                           } finally {
                              if(partition != null) {
                                 if(var20 != null) {
                                    try {
                                       partition.close();
                                    } catch (Throwable var137) {
                                       var20.addSuppressed(var137);
                                    }
                                 } else {
                                    partition.close();
                                 }
                              }

                           }
                        }

                        validator.complete();
                     } catch (Throwable var140) {
                        var18 = var140;
                        throw var140;
                     } finally {
                        if(ci != null) {
                           if(var18 != null) {
                              try {
                                 ci.close();
                              } catch (Throwable var136) {
                                 var18.addSuppressed(var136);
                              }
                           } else {
                              ci.close();
                           }
                        }

                     }
                  } catch (Throwable var142) {
                     var16 = var142;
                     throw var142;
                  } finally {
                     if(controller != null) {
                        if(var16 != null) {
                           try {
                              controller.close();
                           } catch (Throwable var135) {
                              var16.addSuppressed(var135);
                           }
                        } else {
                           controller.close();
                        }
                     }

                  }
               } catch (Throwable var144) {
                  var14 = var144;
                  throw var144;
               } finally {
                  if(scanners != null) {
                     if(var14 != null) {
                        try {
                           scanners.close();
                        } catch (Throwable var134) {
                           var14.addSuppressed(var134);
                        }
                     } else {
                        scanners.close();
                     }
                  }

               }
            } finally {
               if(isSnapshotValidation && !isGlobalSnapshotValidation) {
                  cfs.clearSnapshot(snapshotName);
               }

               cfs.metric.partitionsValidated.update(partitionCount);
            }

            long estimatedTotalBytes = 0L;
            Iterator var149 = sstables.iterator();

            label1318:
            while(true) {
               if(var149.hasNext()) {
                  SSTableReader sstable = (SSTableReader)var149.next();
                  Iterator var152 = sstable.getPositionsForRanges(validator.desc.ranges).iterator();

                  while(true) {
                     if(!var152.hasNext()) {
                        continue label1318;
                     }

                     Pair<Long, Long> positionsForRanges = (Pair)var152.next();
                     estimatedTotalBytes += ((Long)positionsForRanges.right).longValue() - ((Long)positionsForRanges.left).longValue();
                  }
               }

               cfs.metric.bytesValidated.update(estimatedTotalBytes);
               if(logger.isDebugEnabled()) {
                  long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                  logger.debug("Validation of {} partitions (~{}) finished in {} msec, for {}", new Object[]{Long.valueOf(partitionCount), FBUtilities.prettyPrintMemory(estimatedTotalBytes), Long.valueOf(duration), validator.desc});
               }
               break;
            }
         } finally {
            if(sstables != null) {
               sstables.release();
            }

         }

      }
   }

   public static MerkleTrees createMerkleTrees(Iterable<SSTableReader> sstables, Collection<Range<Token>> ranges, ColumnFamilyStore cfs) {
      MerkleTrees tree = new MerkleTrees(cfs.getPartitioner());
      long allPartitions = 0L;
      Map<Range<Token>, Long> rangePartitionCounts = Maps.newHashMapWithExpectedSize(ranges.size());

      Iterator var7;
      Range range;
      long numPartitions;
      for(var7 = ranges.iterator(); var7.hasNext(); allPartitions += numPartitions) {
         range = (Range)var7.next();
         numPartitions = 0L;

         SSTableReader sstable;
         for(Iterator var11 = sstables.iterator(); var11.hasNext(); numPartitions += sstable.estimatedKeysForRanges(Collections.singleton(range))) {
            sstable = (SSTableReader)var11.next();
         }

         rangePartitionCounts.put(range, Long.valueOf(numPartitions));
      }

      int depth;
      for(var7 = ranges.iterator(); var7.hasNext(); tree.addMerkleTree((int)Math.pow(2.0D, (double)depth), range)) {
         range = (Range)var7.next();
         numPartitions = ((Long)rangePartitionCounts.get(range)).longValue();
         double rangeOwningRatio = allPartitions > 0L?(double)numPartitions / (double)allPartitions:0.0D;
         int maxDepth = rangeOwningRatio > 0.0D?(int)Math.floor(20.0D - Math.log(1.0D / rangeOwningRatio) / Math.log(2.0D)):0;
         depth = numPartitions > 0L?(int)Math.min(Math.ceil(Math.log((double)numPartitions) / Math.log(2.0D)), (double)maxDepth):0;
         if(depth > maxDepth) {
            logger.debug("Range {} with {} partitions require a merkle tree with depth {} but the maximum allowed depth for this range is {}.", new Object[]{range, Long.valueOf(numPartitions), Integer.valueOf(depth), Integer.valueOf(maxDepth)});
         }
      }

      if(logger.isDebugEnabled()) {
         logger.debug("Created {} merkle trees with merkle trees size {}, {} partitions, {} bytes", new Object[]{Integer.valueOf(tree.ranges().size()), Long.valueOf(tree.size()), Long.valueOf(allPartitions), Long.valueOf(((MerkleTrees.MerkleTreesSerializer)MerkleTrees.serializers.get(Version.last(RepairVerbs.RepairVersion.class))).serializedSize(tree))});
      }

      return tree;
   }

   @VisibleForTesting
   synchronized Refs<SSTableReader> getSSTablesToValidate(ColumnFamilyStore cfs, Validator validator) {
      Refs<SSTableReader> sstables;
      ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(validator.desc.parentSessionId);
      if (prs == null) {
         return null;
      }
      HashSet<SSTableReader> sstablesToValidate = new HashSet<SSTableReader>();
      Predicate<SSTableReader> predicate = prs.isPreview() ? prs.getPreviewPredicate() : (validator.isIncremental ? s -> validator.desc.parentSessionId.equals(s.getSSTableMetadata().pendingRepair) : s -> !prs.isIncremental || !s.isRepaired());
      try (ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, predicate));){
         for (SSTableReader sstable : sstableCandidates.sstables) {
            if (!new Bounds<Token>(sstable.first.getToken(), sstable.last.getToken()).intersects(validator.desc.ranges)) continue;
            sstablesToValidate.add(sstable);
         }
         sstables = Refs.tryRef(sstablesToValidate);
         if (sstables == null) {
            logger.error("Could not reference sstables");
            throw new RuntimeException("Could not reference sstables");
         }
      }
      return sstables;
   }

   private void doAntiCompaction(final ColumnFamilyStore cfs, final Collection<Range<Token>> ranges, final LifecycleTransaction repaired, final long repairedAt, final UUID pendingRepair) {
      CompactionManager.logger.info("Performing anticompaction on {} sstables", (Object)repaired.originals().size());
      final Set<SSTableReader> sstables = repaired.originals();
      final Set<SSTableReader> unrepairedSSTables = sstables.stream().filter(s -> !s.isRepaired()).collect(Collectors.toSet());
      cfs.metric.bytesAnticompacted.inc(SSTableReader.getTotalBytes(unrepairedSSTables));
      final Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(unrepairedSSTables);
      int antiCompactedSSTableCount = 0;
      for (final Collection<SSTableReader> sstableGroup : groupedSSTables) {
         try (final LifecycleTransaction txn = repaired.split(sstableGroup)) {
            final int antiCompacted = this.antiCompactGroup(cfs, ranges, txn, repairedAt, pendingRepair);
            antiCompactedSSTableCount += antiCompacted;
         }
      }
      final String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s).";
      CompactionManager.logger.info(format, (Object)repaired.originals().size(), (Object)antiCompactedSSTableCount);
   }

   private int antiCompactGroup(final ColumnFamilyStore cfs, final Collection<Range<Token>> ranges, final LifecycleTransaction anticompactionGroup, final long repairedAt, final UUID pendingRepair) {
      long groupMaxDataAge = -1L;
      for (final SSTableReader sstable : anticompactionGroup.originals()) {
         if (groupMaxDataAge < sstable.maxDataAge) {
            groupMaxDataAge = sstable.maxDataAge;
         }
      }
      if (anticompactionGroup.originals().size() == 0) {
         CompactionManager.logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
         return 0;
      }
      CompactionManager.logger.info("Anticompacting {}", (Object)anticompactionGroup);
      final Set<SSTableReader> sstableAsSet = anticompactionGroup.originals();
      final long writeSize = cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION);
      final File destination = cfs.getDirectories().getWriteableLocationAsFile(cfs, (SSTableReader)Iterables.get((Iterable)sstableAsSet, 0), writeSize);
      long repairedKeyCount = 0L;
      long unrepairedKeyCount = 0L;
      final int nowInSec = FBUtilities.nowInSeconds();
      final CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
      try (final SSTableRewriter repairedSSTableWriter = SSTableRewriter.constructWithoutEarlyOpening(anticompactionGroup, false, groupMaxDataAge);
           final SSTableRewriter unRepairedSSTableWriter = SSTableRewriter.constructWithoutEarlyOpening(anticompactionGroup, false, groupMaxDataAge);
           final AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(anticompactionGroup.originals());
           final CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
           final CompactionIterator ci = new CompactionIterator(OperationType.ANTICOMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID(), this.metrics)) {
         final int expectedBloomFilterSize = Math.max(cfs.metadata().params.minIndexInterval, (int)SSTableReader.getApproximateKeyCount(sstableAsSet));
         repairedSSTableWriter.switchWriter(createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, pendingRepair, sstableAsSet, anticompactionGroup));
         unRepairedSSTableWriter.switchWriter(createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, 0L, null, sstableAsSet, anticompactionGroup));
         final Range.OrderedRangeContainmentChecker containmentChecker = new Range.OrderedRangeContainmentChecker(ranges);
         while (ci.hasNext()) {
            try (final UnfilteredRowIterator partition = ci.next()) {
               if (containmentChecker.contains(partition.partitionKey().getToken())) {
                  repairedSSTableWriter.append(partition);
                  ++repairedKeyCount;
               }
               else {
                  unRepairedSSTableWriter.append(partition);
                  ++unrepairedKeyCount;
               }
            }
         }
         final List<SSTableReader> anticompactedSSTables = new ArrayList<SSTableReader>();
         anticompactionGroup.permitRedundantTransitions();
         repairedSSTableWriter.setRepairedAt(repairedAt).prepareToCommit();
         unRepairedSSTableWriter.prepareToCommit();
         anticompactedSSTables.addAll(repairedSSTableWriter.finished());
         anticompactedSSTables.addAll(unRepairedSSTableWriter.finished());
         repairedSSTableWriter.commit();
         unRepairedSSTableWriter.commit();
         CompactionManager.logger.trace("Repaired {} keys out of {} for {}/{} in {}", new Object[] { repairedKeyCount, repairedKeyCount + unrepairedKeyCount, cfs.keyspace.getName(), cfs.getTableName(), anticompactionGroup });
         return anticompactedSSTables.size();
      }
      catch (Throwable e) {
         JVMStabilityInspector.inspectThrowable(e);
         CompactionManager.logger.error("Error anticompacting " + anticompactionGroup, e);
         return 0;
      }
   }

   public ListenableFuture<?> submitIndexBuild(final SecondaryIndexBuilder builder) {
      Runnable runnable = new Runnable() {
         public void run() {
            CompactionManager.this.metrics.beginCompaction(builder);

            try {
               builder.build();
            } finally {
               CompactionManager.this.metrics.finishCompaction(builder);
            }

         }
      };
      return this.executor.submitIfRunning(runnable, "index build");
   }

   public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer) {
      Runnable runnable = new Runnable() {
         public void run() {
            if(!AutoSavingCache.flushInProgress.add(writer.cacheType())) {
               CompactionManager.logger.trace("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
            } else {
               try {
                  CompactionManager.this.metrics.beginCompaction(writer);

                  try {
                     writer.saveCache();
                  } finally {
                     CompactionManager.this.metrics.finishCompaction(writer);
                  }
               } finally {
                  AutoSavingCache.flushInProgress.remove(writer.cacheType());
               }

            }
         }
      };
      return this.executor.submitIfRunning(runnable, "cache write");
   }

   public static int getDefaultGcBefore(ColumnFamilyStore cfs, int nowInSec) {
      TableMetadata table = cfs.metadata();
      if(!cfs.isIndex() && table.params.gcGraceSeconds != 0) {
         int gcBefore = cfs.gcBefore(nowInSec);
         return StorageService.instance.nodeSyncService.canPurge(table, gcBefore)?gcBefore:-2147483648;
      } else {
         return nowInSec;
      }
   }

   public ListenableFuture<Long> submitViewBuilder(ViewBuilderTask task) {
      return this.viewBuildExecutor.submitIfRunning(() -> {
         this.metrics.beginCompaction(task);

         Long var2;
         try {
            var2 = task.call();
         } finally {
            this.metrics.finishCompaction(task);
         }

         return var2;
      }, "view build");
   }

   public int getActiveCompactions() {
      return CompactionMetrics.getCompactions().size();
   }

   public void incrementAborted() {
      this.metrics.compactionsAborted.inc();
   }

   public void incrementCompactionsReduced() {
      this.metrics.compactionsReduced.inc();
   }

   public void incrementSstablesDropppedFromCompactions(long num) {
      this.metrics.sstablesDropppedFromCompactions.inc(num);
   }

   public List<Map<String, String>> getCompactions() {
      List<CompactionInfo.Holder> compactionHolders = CompactionMetrics.getCompactions();
      List<Map<String, String>> out = new ArrayList(compactionHolders.size());
      Iterator var3 = compactionHolders.iterator();

      while(var3.hasNext()) {
         CompactionInfo.Holder ci = (CompactionInfo.Holder)var3.next();
         out.add(ci.getCompactionInfo().asMap());
      }

      return out;
   }

   public List<String> getCompactionSummary() {
      List<CompactionInfo.Holder> compactionHolders = CompactionMetrics.getCompactions();
      List<String> out = new ArrayList(compactionHolders.size());
      Iterator var3 = compactionHolders.iterator();

      while(var3.hasNext()) {
         CompactionInfo.Holder ci = (CompactionInfo.Holder)var3.next();
         out.add(ci.getCompactionInfo().toString());
      }

      return out;
   }

   public TabularData getCompactionHistory() {
      return (TabularData)TPCUtils.blockingGet(SystemKeyspace.getCompactionHistory());
   }

   public long getTotalBytesCompacted() {
      return this.metrics.bytesCompacted.getCount();
   }

   public long getTotalCompactionsCompleted() {
      return this.metrics.totalCompactionsCompleted.getCount();
   }

   public int getPendingTasks() {
      return ((Integer)this.metrics.pendingTasks.getValue()).intValue();
   }

   public long getCompletedTasks() {
      return ((Long)this.metrics.completedTasks.getValue()).longValue();
   }

   public void stopCompaction(String type) {
      OperationType operation = OperationType.valueOf(type);
      Iterator var3 = CompactionMetrics.getCompactions().iterator();

      while(var3.hasNext()) {
         CompactionInfo.Holder holder = (CompactionInfo.Holder)var3.next();
         if(holder.getCompactionInfo().getTaskType() == operation) {
            holder.stop();
         }
      }

   }

   public void stopCompactionById(String compactionId) {
      Iterator var2 = CompactionMetrics.getCompactions().iterator();

      while(var2.hasNext()) {
         CompactionInfo.Holder holder = (CompactionInfo.Holder)var2.next();
         UUID holderId = holder.getCompactionInfo().compactionId();
         if(holderId != null && holderId.equals(UUID.fromString(compactionId))) {
            holder.stop();
         }
      }

   }

   public void setConcurrentCompactors(int value) {
      if(value > this.executor.getCorePoolSize()) {
         this.executor.setMaximumPoolSize(value);
         this.executor.setCorePoolSize(value);
      } else if(value < this.executor.getCorePoolSize()) {
         this.executor.setCorePoolSize(value);
         this.executor.setMaximumPoolSize(value);
      }

   }

   public void setConcurrentValidations(int value) {
      value = value > 0?value:2147483647;
      this.validationExecutor.setMaximumPoolSize(value);
   }

   public void setConcurrentViewBuilders(int value) {
      if(value > this.viewBuildExecutor.getCorePoolSize()) {
         this.viewBuildExecutor.setMaximumPoolSize(value);
         this.viewBuildExecutor.setCorePoolSize(value);
      } else if(value < this.viewBuildExecutor.getCorePoolSize()) {
         this.viewBuildExecutor.setCorePoolSize(value);
         this.viewBuildExecutor.setMaximumPoolSize(value);
      }

   }

   public int getCoreCompactorThreads() {
      return this.executor.getCorePoolSize();
   }

   public void setCoreCompactorThreads(int number) {
      this.executor.setCorePoolSize(number);
   }

   public int getMaximumCompactorThreads() {
      return this.executor.getMaximumPoolSize();
   }

   public void setMaximumCompactorThreads(int number) {
      this.executor.setMaximumPoolSize(number);
   }

   public int getCoreValidationThreads() {
      return this.validationExecutor.getCorePoolSize();
   }

   public void setCoreValidationThreads(int number) {
      this.validationExecutor.setCorePoolSize(number);
   }

   public int getMaximumValidatorThreads() {
      return this.validationExecutor.getMaximumPoolSize();
   }

   public void setMaximumValidatorThreads(int number) {
      this.validationExecutor.setMaximumPoolSize(number);
   }

   public int getCoreViewBuildThreads() {
      return this.viewBuildExecutor.getCorePoolSize();
   }

   public void setCoreViewBuildThreads(int number) {
      this.viewBuildExecutor.setCorePoolSize(number);
   }

   public int getMaximumViewBuildThreads() {
      return this.viewBuildExecutor.getMaximumPoolSize();
   }

   public void setMaximumViewBuildThreads(int number) {
      this.viewBuildExecutor.setMaximumPoolSize(number);
   }

   /** @deprecated */
   @Deprecated
   public boolean interruptCompactionFor(Iterable<TableMetadata> tables, boolean interruptValidation) {
      return this.interruptCompactionFor(tables, interruptValidation?Predicates.alwaysTrue():OperationType.EXCEPT_VALIDATIONS, Predicates.alwaysTrue());
   }

   public boolean interruptCompactionFor(Iterable<TableMetadata> tables) {
      return this.interruptCompactionFor(tables, Predicates.alwaysTrue(), Predicates.alwaysTrue());
   }

   public boolean interruptCompactionFor(Iterable<TableMetadata> tables, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate) {
      assert tables != null;

      boolean interrupted = false;
      Iterator var5 = CompactionMetrics.getCompactions().iterator();

      while(var5.hasNext()) {
         CompactionInfo.Holder compactionHolder = (CompactionInfo.Holder)var5.next();
         CompactionInfo info = compactionHolder.getCompactionInfo();
         if(Iterables.contains(tables, info.getTableMetadata()) && opPredicate.apply(info.getTaskType())) {
            compactionHolder.stop(readerPredicate);
            interrupted = true;
         }
      }

      return interrupted;
   }

   /** @deprecated */
   @Deprecated
   public boolean interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss, boolean interruptValidation) {
      return this.interruptCompactionForCFs(cfss, interruptValidation?Predicates.alwaysTrue():OperationType.EXCEPT_VALIDATIONS, Predicates.alwaysTrue());
   }

   public boolean interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss) {
      return this.interruptCompactionForCFs(cfss, Predicates.alwaysTrue(), Predicates.alwaysTrue());
   }

   public boolean interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate) {
      List<TableMetadata> metadata = new ArrayList();
      Iterator var5 = cfss.iterator();

      while(var5.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
         metadata.add(cfs.metadata());
      }

      return this.interruptCompactionFor(metadata, opPredicate, readerPredicate);
   }

   public void waitForCessation(Iterable<ColumnFamilyStore> cfss) {
      this.waitForCessation(cfss, Predicates.alwaysTrue(), Predicates.alwaysTrue());
   }

   public void waitForCessation(Iterable<ColumnFamilyStore> cfss, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate) {
      long start = System.nanoTime();
      long delay = TimeUnit.MINUTES.toNanos(5L);

      while(System.nanoTime() - start < delay && instance.isCompacting(cfss, opPredicate, readerPredicate)) {
         Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
      }

   }

   static {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(instance, new ObjectName("org.apache.cassandra.db:type=CompactionManager"));
      } catch (Exception var2) {
         throw new RuntimeException(var2);
      }

      cacheCleanupExecutor = new CompactionManager.CacheCleanupExecutor();
   }

   public interface CompactionExecutorStatsCollector {
      void beginCompaction(CompactionInfo.Holder var1);

      void finishCompaction(CompactionInfo.Holder var1);
   }

   private static class CacheCleanupExecutor extends CompactionManager.CompactionExecutor {
      public CacheCleanupExecutor() {
         super(1, "CacheCleanupExecutor");
      }
   }

   private static class ViewBuildExecutor extends CompactionManager.CompactionExecutor {
      public ViewBuildExecutor() {
         super(DatabaseDescriptor.getConcurrentViewBuilders(), "ViewBuildExecutor");
      }
   }

   private static class ValidationExecutor extends CompactionManager.CompactionExecutor {
      public ValidationExecutor() {
         super(1, DatabaseDescriptor.getConcurrentValidations(), "ValidationExecutor", new SynchronousQueue());
      }
   }

   static class CompactionExecutor extends JMXEnabledThreadPoolExecutor {
      protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue) {
         super(minThreads, maxThreads, 60L, TimeUnit.SECONDS, queue, new NamedThreadFactory(name), "internal");
      }

      private CompactionExecutor(int threadCount, String name) {
         this(threadCount, threadCount, name, new LinkedBlockingQueue());
      }

      public CompactionExecutor() {
         this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
      }

      protected void beforeExecute(Thread t, Runnable r) {
         CompactionManager.isCompactionManager.set(Boolean.valueOf(true));
         super.beforeExecute(t, r);
      }

      public void afterExecute(Runnable r, Throwable t) {
         DebuggableThreadPoolExecutor.maybeResetTraceSessionWrapper(r);
         if(t == null) {
            t = DebuggableThreadPoolExecutor.extractThrowable(r);
         }

         if(t != null) {
            if(t instanceof CompactionInterruptedException) {
               logger.info(t.getMessage());
               if(t.getSuppressed() != null && t.getSuppressed().length > 0) {
                  logger.warn("Interruption of compaction encountered exceptions:", t);
               } else {
                  logger.trace("Full interruption stack trace:", t);
               }
            } else {
               DebuggableThreadPoolExecutor.handleOrLog(t);
            }
         }

         SnapshotDeletingTask.rescheduleFailedTasks();
      }

      @VisibleForTesting
      protected ListenableFuture<?> submitIfRunning(Runnable task, String name) {
         return this.submitIfRunning(Executors.callable(task, (Object)null), name);
      }

      public <T> ListenableFuture<T> submitIfRunning(Callable<T> task, String name) {
         if(this.isShutdown()) {
            logger.info("Executor has been shut down, not submitting {}", name);
            return Futures.immediateCancelledFuture();
         } else {
            try {
               ListenableFutureTask<T> ret = ListenableFutureTask.create(task);
               this.execute(ret);
               return ret;
            } catch (RejectedExecutionException var4) {
               if(this.isShutdown()) {
                  logger.info("Executor has shut down, could not submit {}", name);
               } else {
                  logger.error("Failed to submit {}", name, var4);
               }

               return Futures.immediateCancelledFuture();
            }
         }
      }
   }

   public static class ValidationCompactionController extends CompactionController {
      public ValidationCompactionController(ColumnFamilyStore cfs, int gcBefore) {
         super(cfs, gcBefore);
      }

      public LongPredicate getPurgeEvaluator(DecoratedKey key) {
         return (time) -> {
            return true;
         };
      }
   }

   public static class ValidationCompactionIterator extends CompactionIterator {
      public ValidationCompactionIterator(List<ISSTableScanner> scanners, CompactionManager.ValidationCompactionController controller, int nowInSec, CompactionMetrics metrics) {
         super(OperationType.VALIDATION, scanners, controller, nowInSec, UUIDGen.getTimeUUID(), metrics);
      }
   }

   private abstract static class CleanupStrategy {
      protected final Collection<Range<Token>> ranges;
      protected final int nowInSec;

      protected CleanupStrategy(Collection<Range<Token>> ranges, int nowInSec) {
         this.ranges = ranges;
         this.nowInSec = nowInSec;
      }

      public static CompactionManager.CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec) {
         return (CompactionManager.CleanupStrategy)(cfs.indexManager.hasIndexes()?new CompactionManager.CleanupStrategy.Full(cfs, ranges, nowInSec):new CompactionManager.CleanupStrategy.Bounded(cfs, ranges, nowInSec));
      }

      public abstract ISSTableScanner getScanner(SSTableReader var1);

      public abstract UnfilteredRowIterator cleanup(UnfilteredRowIterator var1);

      private static final class Full extends CompactionManager.CleanupStrategy {
         private final ColumnFamilyStore cfs;

         public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec) {
            super(ranges, nowInSec);
            this.cfs = cfs;
         }

         public ISSTableScanner getScanner(SSTableReader sstable) {
            return sstable.getScanner();
         }

         public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition) {
            if(Range.isInRanges(partition.partitionKey().getToken(), this.ranges)) {
               return partition;
            } else {
               this.cfs.invalidateCachedPartition(partition.partitionKey());
               this.cfs.indexManager.deletePartition(partition, this.nowInSec);
               return null;
            }
         }
      }

      private static final class Bounded extends CompactionManager.CleanupStrategy {
         public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec) {
            super(ranges, nowInSec);
            CompactionManager.cacheCleanupExecutor.submit(new Runnable() {
               public void run() {
                  cfs.cleanupCache();
               }
            });
         }

         public ISSTableScanner getScanner(SSTableReader sstable) {
            return sstable.getScanner(this.ranges);
         }

         public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition) {
            return partition;
         }
      }
   }

   public static enum AllSSTableOpStatus {
      SUCCESSFUL(0),
      ABORTED(1),
      UNABLE_TO_CANCEL(2);

      public final int statusCode;

      private AllSSTableOpStatus(int statusCode) {
         this.statusCode = statusCode;
      }
   }

   private interface OneSSTableOperation {
      Iterable<SSTableReader> filterSSTables(LifecycleTransaction var1);

      void execute(LifecycleTransaction var1) throws IOException;
   }

   class BackgroundCompactionCandidate implements Runnable {
      private final ColumnFamilyStore cfs;

      BackgroundCompactionCandidate(ColumnFamilyStore cfs) {
         CompactionManager.this.compactingCF.add(cfs);
         this.cfs = cfs;
      }

      public void run() {
         label60: {
            try {
               CompactionManager.logger.trace("Checking {}.{}", this.cfs.keyspace.getName(), this.cfs.name);
               if(!this.cfs.isValid()) {
                  CompactionManager.logger.trace("Aborting compaction for dropped CF");
                  return;
               }

               CompactionStrategyManager strategy = this.cfs.getCompactionStrategyManager();
               AbstractCompactionTask task = strategy.getNextBackgroundTask(CompactionManager.getDefaultGcBefore(this.cfs, FBUtilities.nowInSeconds()));
               if(task != null) {
                  task.execute(CompactionManager.this.metrics);
                  break label60;
               }

               CompactionManager.logger.trace("No tasks available");
            } finally {
               CompactionManager.this.compactingCF.remove(this.cfs);
            }

            return;
         }

         CompactionManager.this.submitBackground(this.cfs);
      }
   }
}
