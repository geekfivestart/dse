package org.apache.cassandra.db.compaction;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionTask extends AbstractCompactionTask {
   protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
   protected final int gcBefore;
   protected final boolean keepOriginals;
   protected final boolean ignoreOverlaps;
   protected static long totalBytesCompacted = 0L;
   private CompactionManager.CompactionExecutorStatsCollector collector;

   public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore) {
      this(cfs, txn, gcBefore, false);
   }

   public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean ignoreOverlaps) {
      this(cfs, txn, gcBefore, false, ignoreOverlaps);
   }

   private CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean keepOriginals, boolean ignoreOverlaps) {
      super(cfs, txn);
      this.gcBefore = gcBefore;
      this.keepOriginals = keepOriginals;
      this.ignoreOverlaps = ignoreOverlaps;
   }

   public static synchronized long addToTotalBytesCompacted(long bytesCompacted) {
      return totalBytesCompacted += bytesCompacted;
   }

   protected int executeInternal(CompactionManager.CompactionExecutorStatsCollector collector) {
      this.collector = collector;
      this.run();
      return this.transaction.originals().size();
   }

   public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize) {
      if(this.partialCompactionsAcceptable() && this.transaction.originals().size() > 1) {
         logger.warn("Insufficient space to compact all requested files. {}MB required, {}", Float.valueOf((float)expectedSize / 1024.0F / 1024.0F), StringUtils.join(this.transaction.originals(), ", "));
         SSTableReader removedSSTable = this.cfs.getMaxSizeFile(nonExpiredSSTables);
         this.transaction.cancel(removedSSTable);
         return true;
      } else {
         return false;
      }
   }

   protected void runMayThrow() throws Exception {
      assert this.transaction != null;

      if(!this.transaction.originals().isEmpty()) {
         CompactionStrategyManager strategy = this.cfs.getCompactionStrategyManager();
         if(DatabaseDescriptor.isSnapshotBeforeCompaction()) {
            this.cfs.snapshotWithoutFlush(System.currentTimeMillis() + "-compact-" + this.cfs.name);
         }

         CompactionController controller = this.getCompactionController(this.transaction.originals());
         Throwable var3 = null;

         try {
            Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();
            this.buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);

            assert !Iterables.any(this.transaction.originals(), new Predicate<SSTableReader>() {
               public boolean apply(SSTableReader sstable) {
                  return !sstable.descriptor.cfname.equals(CompactionTask.this.cfs.name);
               }
            });

            UUID taskId = this.transaction.opId();
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            Iterator var7 = this.transaction.originals().iterator();

            while(var7.hasNext()) {
               SSTableReader sstr = (SSTableReader)var7.next();
               ssTableLoggerMsg.append(String.format("%s:level=%d, ", new Object[]{sstr.getFilename(), Integer.valueOf(sstr.getSSTableLevel())}));
            }

            ssTableLoggerMsg.append("]");
            logger.debug("Compacting ({}) {}", taskId, ssTableLoggerMsg);
            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = System.nanoTime();
            long startTime = System.currentTimeMillis();
            long totalKeysWritten = 0L;
            long estimatedKeys = 0L;
            Set<SSTableReader> actuallyCompact = Sets.difference(this.transaction.originals(), fullyExpiredSSTables);
            int nowInSec = FBUtilities.nowInSeconds();
            Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
            Throwable var25 = null;

            long inputSizeBytes;
            Collection newSStables;
            long[] mergedRowCounts;
            long totalSourceCQLRows;
            long lastCheckObsoletion;
            double compressionRatio;
            try {
               AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact);
               Throwable var27 = null;

               try {
                  CompactionIterator ci = new CompactionIterator(this.compactionType, scanners.scanners, controller, nowInSec, taskId);
                  Throwable var29 = null;

                  try {
                     lastCheckObsoletion = start;
                     inputSizeBytes = scanners.getTotalCompressedSize();
                     compressionRatio = scanners.getCompressionRatio();
                     if(compressionRatio == -1.0D) {
                        compressionRatio = 1.0D;
                     }

                     long lastBytesScanned = 0L;
                     if(!controller.cfs.getCompactionStrategyManager().isActive()) {
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                     }

                     if(this.collector != null) {
                        this.collector.beginCompaction(ci);
                     }

                     try {
                        CompactionAwareWriter writer = this.getCompactionAwareWriter(this.cfs, this.getDirectories(), this.transaction, actuallyCompact);
                        Throwable var37 = null;

                        try {
                           estimatedKeys = writer.estimatedKeys();

                           while(ci.hasNext()) {
                              if(ci.isStopRequested()) {
                                 throw new CompactionInterruptedException(ci.getCompactionInfo());
                              }

                              if(writer.append(ci.next())) {
                                 ++totalKeysWritten;
                              }

                              long bytesScanned = scanners.getTotalBytesScanned();
                              CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);
                              lastBytesScanned = bytesScanned;
                              if(System.nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L)) {
                                 controller.maybeRefreshOverlaps();
                                 lastCheckObsoletion = System.nanoTime();
                              }
                           }

                           newSStables = writer.finish();
                        } catch (Throwable var168) {
                           var37 = var168;
                           throw var168;
                        } finally {
                           if(writer != null) {
                              if(var37 != null) {
                                 try {
                                    writer.close();
                                 } catch (Throwable var167) {
                                    var37.addSuppressed(var167);
                                 }
                              } else {
                                 writer.close();
                              }
                           }

                        }
                     } finally {
                        if(this.collector != null) {
                           this.collector.finishCompaction(ci);
                        }

                        mergedRowCounts = ci.getMergedRowCounts();
                        totalSourceCQLRows = ci.getTotalSourceCQLRows();
                     }
                  } catch (Throwable var171) {
                     var29 = var171;
                     throw var171;
                  } finally {
                     if(ci != null) {
                        if(var29 != null) {
                           try {
                              ci.close();
                           } catch (Throwable var166) {
                              var29.addSuppressed(var166);
                           }
                        } else {
                           ci.close();
                        }
                     }

                  }
               } catch (Throwable var173) {
                  var27 = var173;
                  throw var173;
               } finally {
                  if(scanners != null) {
                     if(var27 != null) {
                        try {
                           scanners.close();
                        } catch (Throwable var165) {
                           var27.addSuppressed(var165);
                        }
                     } else {
                        scanners.close();
                     }
                  }

               }
            } catch (Throwable var175) {
               var25 = var175;
               throw var175;
            } finally {
               if(refs != null) {
                  if(var25 != null) {
                     try {
                        refs.close();
                     } catch (Throwable var164) {
                        var25.addSuppressed(var164);
                     }
                  } else {
                     refs.close();
                  }
               }

            }

            if(this.transaction.isOffline()) {
               Refs.release(Refs.selfRefs(newSStables));
            } else {
               long durationInNano = System.nanoTime() - start;
               long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
               lastCheckObsoletion = SSTableReader.getTotalBytes(newSStables);
               compressionRatio = (double)lastCheckObsoletion / (double)inputSizeBytes;
               StringBuilder newSSTableNames = new StringBuilder();
               Iterator var35 = newSStables.iterator();

               while(var35.hasNext()) {
                  SSTableReader reader = (SSTableReader)var35.next();
                  newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
               }

               long totalSourceRows = 0L;

               for(int i = 0; i < mergedRowCounts.length; ++i) {
                  totalSourceRows += mergedRowCounts[i] * (long)(i + 1);
               }

               String mergeSummary = (String)TPCUtils.blockingGet(updateCompactionHistory(this.cfs.keyspace.getName(), this.cfs.getTableName(), mergedRowCounts, inputSizeBytes, lastCheckObsoletion));
               logger.debug(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}", new Object[]{taskId, Integer.valueOf(this.transaction.originals().size()), newSSTableNames.toString(), Integer.valueOf(this.getLevel()), FBUtilities.prettyPrintMemory(inputSizeBytes), FBUtilities.prettyPrintMemory(lastCheckObsoletion), Integer.valueOf((int)(compressionRatio * 100.0D)), Long.valueOf(dTime), FBUtilities.prettyPrintMemoryPerSecond(inputSizeBytes, durationInNano), FBUtilities.prettyPrintMemoryPerSecond(lastCheckObsoletion, durationInNano), Long.valueOf((long)((int)totalSourceCQLRows) / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1L)), Long.valueOf(totalSourceRows), Long.valueOf(totalKeysWritten), mergeSummary}));
               logger.trace("CF Total Bytes Compacted: {}", FBUtilities.prettyPrintMemory(addToTotalBytesCompacted(lastCheckObsoletion)));
               logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", new Object[]{Long.valueOf(totalKeysWritten), Long.valueOf(estimatedKeys), Double.valueOf((double)(totalKeysWritten - estimatedKeys) / (double)totalKeysWritten)});
               this.cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, this.transaction.originals(), System.currentTimeMillis(), newSStables);
               this.cfs.metric.compactionBytesWritten.inc(lastCheckObsoletion);
            }
         } catch (Throwable var177) {
            var3 = var177;
            throw var177;
         } finally {
            if(controller != null) {
               if(var3 != null) {
                  try {
                     controller.close();
                  } catch (Throwable var163) {
                     var3.addSuppressed(var163);
                  }
               } else {
                  controller.close();
               }
            }

         }

      }
   }

   public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction transaction, Set<SSTableReader> nonExpiredSSTables) {
      return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, this.keepOriginals, this.getLevel());
   }

   public static CompletableFuture<String> updateCompactionHistory(String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize) {
      StringBuilder mergeSummary = new StringBuilder(mergedRowCounts.length * 10);
      Map<Integer, Long> mergedRows = new HashMap();

      for(int i = 0; i < mergedRowCounts.length; ++i) {
         long count = mergedRowCounts[i];
         if(count != 0L) {
            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", new Object[]{Integer.valueOf(rows), Long.valueOf(count)}));
            mergedRows.put(Integer.valueOf(rows), Long.valueOf(count));
         }
      }

      return SystemKeyspace.updateCompactionHistory(keyspaceName, columnFamilyName, System.currentTimeMillis(), startSize, endSize, mergedRows).thenApply((resultSet) -> {
         return mergeSummary.toString();
      });
   }

   protected Directories getDirectories() {
      return this.cfs.getDirectories();
   }

   public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact) {
      long minRepairedAt = 9223372036854775807L;

      SSTableReader sstable;
      for(Iterator var3 = actuallyCompact.iterator(); var3.hasNext(); minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt)) {
         sstable = (SSTableReader)var3.next();
      }

      return minRepairedAt == 9223372036854775807L?0L:minRepairedAt;
   }

   public static UUID getPendingRepair(Set<SSTableReader> sstables) {
      if(sstables.isEmpty()) {
         return ActiveRepairService.NO_PENDING_REPAIR;
      } else {
         Set<UUID> ids = new HashSet();
         Iterator var2 = sstables.iterator();

         while(var2.hasNext()) {
            SSTableReader sstable = (SSTableReader)var2.next();
            ids.add(sstable.getSSTableMetadata().pendingRepair);
         }

         if(ids.size() != 1) {
            throw new RuntimeException(String.format("Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s", new Object[]{ids}));
         } else {
            return (UUID)ids.iterator().next();
         }
      }
   }

   protected void buildCompactionCandidatesForAvailableDiskSpace(Set<SSTableReader> fullyExpiredSSTables) {
      if(!this.cfs.isCompactionDiskSpaceCheckEnabled() && this.compactionType == OperationType.COMPACTION) {
         logger.info("Compaction space check is disabled");
      } else {
         Set<SSTableReader> nonExpiredSSTables = Sets.difference(this.transaction.originals(), fullyExpiredSSTables);
         CompactionStrategyManager strategy = this.cfs.getCompactionStrategyManager();
         int sstablesRemoved = 0;

         while(!nonExpiredSSTables.isEmpty()) {
            long expectedWriteSize = this.cfs.getExpectedCompactedFileSize(nonExpiredSSTables, this.compactionType);
            long estimatedSSTables = Math.max(1L, expectedWriteSize / strategy.getMaxSSTableBytes());
            if(this.cfs.getDirectories().hasAvailableDiskSpace(estimatedSSTables, expectedWriteSize)) {
               break;
            }

            if(!this.reduceScopeForLimitedSpace(nonExpiredSSTables, expectedWriteSize)) {
               if(this.partialCompactionsAcceptable() && fullyExpiredSSTables.size() > 0) {
                  assert this.transaction.originals().equals(fullyExpiredSSTables);
                  break;
               }

               String msg = String.format("Not enough space for compaction, estimated sstables = %d, expected write size = %d", new Object[]{Long.valueOf(estimatedSSTables), Long.valueOf(expectedWriteSize)});
               logger.warn(msg);
               CompactionManager.instance.incrementAborted();
               throw new FSDiskFullWriteError(new IOException(msg));
            }

            ++sstablesRemoved;
            logger.warn("Not enough space for compaction, {}MB estimated.  Reducing scope.", Float.valueOf((float)expectedWriteSize / 1024.0F / 1024.0F));
         }

         if(sstablesRemoved > 0) {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions((long)sstablesRemoved);
         }

      }
   }

   protected int getLevel() {
      return 0;
   }

   protected CompactionController getCompactionController(Set<SSTableReader> toCompact) {
      return new CompactionController(this.cfs, toCompact, this.gcBefore, this.ignoreOverlaps);
   }

   protected boolean partialCompactionsAcceptable() {
      return !this.isUserDefined;
   }

   public static long getMaxDataAge(Collection<SSTableReader> sstables) {
      long max = 0L;
      Iterator var3 = sstables.iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         if(sstable.maxDataAge > max) {
            max = sstable.maxDataAge;
         }
      }

      return max;
   }
}
