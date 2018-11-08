package org.apache.cassandra.streaming;

import com.google.common.base.Preconditions;
import io.reactivex.Completable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.ThrottledUnfilteredIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamReceiveTask extends StreamTask {
   private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);
   private static final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("StreamReceiveTask"));
   private static final int MAX_ROWS_PER_BATCH = Integer.getInteger("cassandra.repair.mutation_repair_rows_per_batch", 100).intValue();
   private final int totalFiles;
   private final long totalSize;
   private final LifecycleTransaction txn;
   private volatile boolean done = false;
   protected Collection<SSTableReader> sstables;
   private int remoteSSTablesReceived = 0;

   public StreamReceiveTask(StreamSession session, TableId tableId, int totalFiles, long totalSize) {
      super(session, tableId);
      this.totalFiles = totalFiles;
      this.totalSize = totalSize;
      this.txn = LifecycleTransaction.offline(OperationType.STREAM);
      this.sstables = new ArrayList(totalFiles);
   }

   public synchronized void received(SSTableMultiWriter sstable) {
      Preconditions.checkState(!this.session.isPreview(), "we should never receive sstables when previewing");
      if(this.done) {
         logger.warn("[{}] Received sstable {} on already finished stream received task. Aborting sstable.", this.session.planId(), sstable.getFilename());
         Throwables.maybeFail(sstable.abort((Throwable)null));
      } else {
         ++this.remoteSSTablesReceived;

         assert this.tableId.equals(sstable.getTableId());

         Collection finished = null;

         try {
            finished = sstable.finish(true);
         } catch (Throwable var4) {
            Throwables.maybeFail(sstable.abort(var4));
         }

         this.txn.update(finished, false);
         this.sstables.addAll(finished);
         if(this.remoteSSTablesReceived == this.totalFiles) {
            this.done = true;
            executor.submit(new StreamReceiveTask.OnCompletionRunnable(this));
         }

      }
   }

   public int getTotalNumberOfFiles() {
      return this.totalFiles;
   }

   public long getTotalSize() {
      return this.totalSize;
   }

   public synchronized LifecycleTransaction getTransaction() {
      if(this.done) {
         throw new RuntimeException(String.format("Stream receive task %s of cf %s already finished.", new Object[]{this.session.planId(), this.tableId}));
      } else {
         return this.txn;
      }
   }

   public synchronized void abort() {
      if(!this.done) {
         this.done = true;
         this.abortTransaction();
         this.sstables.clear();
      }
   }

   private synchronized void abortTransaction() {
      this.txn.abort();
   }

   private synchronized void finishTransaction() {
      this.txn.finish();
   }

   private static class OnCompletionRunnable implements Runnable {
      private final StreamReceiveTask task;

      public OnCompletionRunnable(StreamReceiveTask task) {
         this.task = task;
      }

      private boolean requiresWritePath(ColumnFamilyStore cfs) {
         return cfs.isCdcEnabled() || this.task.session.streamOperation().requiresViewBuild() && cfs.hasViews();
      }

      Mutation createMutation(ColumnFamilyStore cfs, UnfilteredRowIterator rowIterator) {
         return new Mutation(PartitionUpdate.fromIterator(rowIterator, ColumnFilter.all(cfs.metadata())));
      }

      private void sendThroughWritePath(ColumnFamilyStore cfs, Collection<SSTableReader> readers) {
         List<Completable> writes = new ArrayList();
         Iterator var4 = readers.iterator();

         while(var4.hasNext()) {
            SSTableReader reader = (SSTableReader)var4.next();
            Keyspace ks = Keyspace.open(reader.getKeyspaceName());
            ISSTableScanner scanner = reader.getScanner();
            Throwable var8 = null;

            try {
               CloseableIterator<UnfilteredRowIterator> throttledPartitions = ThrottledUnfilteredIterator.throttle((UnfilteredPartitionIterator)scanner, StreamReceiveTask.MAX_ROWS_PER_BATCH);
               Throwable var10 = null;

               try {
                  while(throttledPartitions.hasNext()) {
                     writes.add(ks.apply(this.createMutation(cfs, (UnfilteredRowIterator)throttledPartitions.next()), cfs.isCdcEnabled(), true, false));
                  }
               } catch (Throwable var33) {
                  var10 = var33;
                  throw var33;
               } finally {
                  if(throttledPartitions != null) {
                     if(var10 != null) {
                        try {
                           throttledPartitions.close();
                        } catch (Throwable var32) {
                           var10.addSuppressed(var32);
                        }
                     } else {
                        throttledPartitions.close();
                     }
                  }

               }
            } catch (Throwable var35) {
               var8 = var35;
               throw var35;
            } finally {
               if(scanner != null) {
                  if(var8 != null) {
                     try {
                        scanner.close();
                     } catch (Throwable var31) {
                        var8.addSuppressed(var31);
                     }
                  } else {
                     scanner.close();
                  }
               }

            }

            Completable.concat(writes).blockingAwait();
            writes.clear();
         }

      }

      public void run() {
         ColumnFamilyStore cfs = null;
         boolean requiresWritePath = false;

         try {
            cfs = ColumnFamilyStore.getIfExists(this.task.tableId);
            if(cfs == null) {
               this.task.sstables.clear();
               this.task.abortTransaction();
               this.task.session.taskCompleted(this.task);
               return;
            }

            requiresWritePath = this.requiresWritePath(cfs);
            Collection<SSTableReader> readers = this.task.sstables;
            Refs<SSTableReader> refs = Refs.ref(readers);
            Throwable var5 = null;

            try {
               if(requiresWritePath) {
                  this.sendThroughWritePath(cfs, readers);
               } else {
                  this.task.finishTransaction();
                  StreamReceiveTask.logger.debug("[Stream #{}] Received {} sstables from {} ({})", new Object[]{this.task.session.planId(), Integer.valueOf(readers.size()), this.task.session.peer, readers});
                  cfs.addSSTablesFromStreaming(readers);
                  if(cfs.isRowCacheEnabled() || cfs.metadata().isCounter()) {
                     List<Bounds<Token>> boundsToInvalidate = new ArrayList(readers.size());
                     readers.forEach((sstable) -> {
                        boundsToInvalidate.add(new Bounds(sstable.first.getToken(), sstable.last.getToken()));
                     });
                     Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(boundsToInvalidate);
                     int invalidatedKeys;
                     if(cfs.isRowCacheEnabled()) {
                        invalidatedKeys = cfs.invalidateRowCache(nonOverlappingBounds);
                        if(invalidatedKeys > 0) {
                           StreamReceiveTask.logger.debug("[Stream #{}] Invalidated {} row cache entries on table {}.{} after stream receive task completed.", new Object[]{this.task.session.planId(), Integer.valueOf(invalidatedKeys), cfs.keyspace.getName(), cfs.getTableName()});
                        }
                     }

                     if(cfs.metadata().isCounter()) {
                        invalidatedKeys = cfs.invalidateCounterCache(nonOverlappingBounds);
                        if(invalidatedKeys > 0) {
                           StreamReceiveTask.logger.debug("[Stream #{}] Invalidated {} counter cache entries on table {}.{} after stream receive task completed.", new Object[]{this.task.session.planId(), Integer.valueOf(invalidatedKeys), cfs.keyspace.getName(), cfs.getTableName()});
                        }
                     }
                  }
               }
            } catch (Throwable var25) {
               var5 = var25;
               throw var25;
            } finally {
               if(refs != null) {
                  if(var5 != null) {
                     try {
                        refs.close();
                     } catch (Throwable var24) {
                        var5.addSuppressed(var24);
                     }
                  } else {
                     refs.close();
                  }
               }

            }

            this.task.session.taskCompleted(this.task);
         } catch (Throwable var27) {
            JVMStabilityInspector.inspectThrowable(var27);
            this.task.session.onError(var27);
         } finally {
            if(requiresWritePath) {
               if(cfs != null) {
                  cfs.forceBlockingFlush();
               }

               this.task.abortTransaction();
            }

         }

      }
   }
}
