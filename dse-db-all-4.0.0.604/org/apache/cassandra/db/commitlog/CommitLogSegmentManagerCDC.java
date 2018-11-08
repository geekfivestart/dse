package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.function.BiConsumer;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogSegmentManagerCDC extends AbstractCommitLogSegmentManager {
   static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManagerCDC.class);
   private final CommitLogSegmentManagerCDC.CDCSizeTracker cdcSizeTracker = new CommitLogSegmentManagerCDC.CDCSizeTracker(this, new File(DatabaseDescriptor.getCDCLogLocation()));

   public CommitLogSegmentManagerCDC(CommitLog commitLog, String storageDirectory) {
      super(commitLog, storageDirectory);
   }

   void start() {
      this.cdcSizeTracker.start();
      super.start();
   }

   public void discard(CommitLogSegment segment, boolean delete) {
      segment.close();
      this.addSize(-segment.onDiskSize());
      this.cdcSizeTracker.processDiscardedSegment(segment);
      if(segment.getCDCState() == CommitLogSegment.CDCState.CONTAINS) {
         FileUtils.renameWithConfirm(segment.logFile.getAbsolutePath(), DatabaseDescriptor.getCDCLogLocation() + File.separator + segment.logFile.getName());
      } else if(delete) {
         FileUtils.deleteWithConfirm(segment.logFile);
      }

   }

   public void shutdown() {
      this.cdcSizeTracker.shutdown();
      super.shutdown();
   }

   public Single<CommitLogSegment.Allocation> allocate(final Mutation mutation, final int size) {
      return new Single<CommitLogSegment.Allocation>() {
         protected void subscribeActual(final SingleObserver observer) {
            class AllocationSource implements Runnable, Disposable {
               volatile boolean disposed = false;

               AllocationSource() {
               }

               public void run() {
                  if(!this.isDisposed()) {
                     try {
                        CommitLogSegment segment = CommitLogSegmentManagerCDC.this.allocatingFrom();
                        CommitLogSegmentManagerCDC.this.throwIfForbidden(mutation, segment);
                        if(CommitLogSegmentManagerCDC.logger.isTraceEnabled()) {
                           CommitLogSegmentManagerCDC.logger.trace("Allocating mutation of size {} on segment {} with space {}", new Object[]{Integer.valueOf(size), Long.valueOf(segment.id), Long.valueOf(segment.availableSize())});
                        }

                        CommitLogSegment.Allocation alloc = segment.allocate(mutation, size);
                        if(alloc != null) {
                           if(mutation.trackedByCDC()) {
                              segment.setCDCState(CommitLogSegment.CDCState.CONTAINS);
                           }

                           observer.onSuccess(alloc);
                        } else {
                           if(CommitLogSegmentManagerCDC.logger.isTraceEnabled()) {
                              CommitLogSegmentManagerCDC.logger.trace("Waiting for segment allocation...");
                           }

                           StagedScheduler scheduler = mutation.getScheduler();
                           TPCRunnable us = TPCRunnable.wrap(this, ExecutorLocals.create(), TPCTaskType.WRITE_POST_COMMIT_LOG_SEGMENT, scheduler);
                           CommitLogSegmentManagerCDC.this.advanceAllocatingFrom(segment).whenComplete((ignored, error) -> {
                              try {
                                 if(error == null) {
                                    scheduler.execute(us);
                                 }
                              } catch (Throwable var6) {
                                 error = var6;
                              }

                              if(error != null) {
                                 CommitLogSegmentManagerCDC.logger.debug("Got exception whilst allocating CL segment: {}", error.getMessage());
                                 us.cancelled();
                                 observer.onError(error);
                              }

                           });
                        }
                     } catch (Throwable var5) {
                        CommitLogSegmentManagerCDC.logger.debug("Got exception whilst allocating CL segment: {}", var5.getMessage());
                        observer.onError(var5);
                     }

                  }
               }

               public void dispose() {
                  this.disposed = true;
               }

               public boolean isDisposed() {
                  return this.disposed;
               }
            }

            AllocationSource source = new AllocationSource();
            observer.onSubscribe(source);
            source.run();
         }
      };
   }

   private void throwIfForbidden(Mutation mutation, CommitLogSegment segment) throws CDCSegmentFullException {
      if(mutation.trackedByCDC() && segment.getCDCState() == CommitLogSegment.CDCState.FORBIDDEN) {
         this.cdcSizeTracker.submitOverflowSizeRecalculation();
         throw new CDCSegmentFullException();
      }
   }

   void handleReplayedSegment(File file) {
      logger.trace("Moving (Unopened) segment {} to cdc_raw directory after replay", file);
      FileUtils.renameWithConfirm(file.getAbsolutePath(), DatabaseDescriptor.getCDCLogLocation() + File.separator + file.getName());
      this.cdcSizeTracker.addFlushedSize(file.length());
   }

   public CommitLogSegment createSegment() {
      CommitLogSegment segment = CommitLogSegment.createSegment(this.commitLog, this);
      this.cdcSizeTracker.processNewSegment(segment);
      return segment;
   }

   @VisibleForTesting
   public long updateCDCTotalSize() {
      this.cdcSizeTracker.submitOverflowSizeRecalculation();

      try {
         Thread.sleep((long)(DatabaseDescriptor.getCDCDiskCheckInterval() + 10));
      } catch (InterruptedException var2) {
         ;
      }

      return this.cdcSizeTracker.totalCDCSizeOnDisk();
   }

   private static class CDCSizeTracker extends DirectorySizeCalculator {
      private final RateLimiter rateLimiter = RateLimiter.create(1000.0D / (double)DatabaseDescriptor.getCDCDiskCheckInterval());
      private ExecutorService cdcSizeCalculationExecutor;
      private CommitLogSegmentManagerCDC segmentManager;
      private volatile long unflushedCDCSize;
      private volatile long sizeInProgress = 0L;

      CDCSizeTracker(CommitLogSegmentManagerCDC segmentManager, File path) {
         super(path);
         this.segmentManager = segmentManager;
      }

      public void start() {
         this.size = 0L;
         this.unflushedCDCSize = 0L;
         this.cdcSizeCalculationExecutor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.SECONDS, new SynchronousQueue(), new DiscardPolicy());
      }

      void processNewSegment(CommitLogSegment segment) {
         Object var2 = segment.cdcStateLock;
         synchronized(segment.cdcStateLock) {
            segment.setCDCState((long)this.defaultSegmentSize() + this.totalCDCSizeOnDisk() > this.allowableCDCBytes()?CommitLogSegment.CDCState.FORBIDDEN:CommitLogSegment.CDCState.PERMITTED);
            if(segment.getCDCState() == CommitLogSegment.CDCState.PERMITTED) {
               this.unflushedCDCSize += (long)this.defaultSegmentSize();
            }
         }

         this.submitOverflowSizeRecalculation();
      }

      void processDiscardedSegment(CommitLogSegment segment) {
         Object var2 = segment.cdcStateLock;
         synchronized(segment.cdcStateLock) {
            if(segment.getCDCState() == CommitLogSegment.CDCState.CONTAINS) {
               this.size += segment.onDiskSize();
            }

            if(segment.getCDCState() != CommitLogSegment.CDCState.FORBIDDEN) {
               this.unflushedCDCSize -= (long)this.defaultSegmentSize();
            }
         }

         this.submitOverflowSizeRecalculation();
      }

      private long allowableCDCBytes() {
         return (long)DatabaseDescriptor.getCDCSpaceInMB() * 1024L * 1024L;
      }

      public void submitOverflowSizeRecalculation() {
         try {
            this.cdcSizeCalculationExecutor.submit(() -> {
               this.recalculateOverflowSize();
            });
         } catch (RejectedExecutionException var2) {
            ;
         }

      }

      private void recalculateOverflowSize() {
         this.rateLimiter.acquire();
         this.calculateSize();
         CommitLogSegment allocatingFrom = this.segmentManager.allocatingFrom();
         if(allocatingFrom.getCDCState() == CommitLogSegment.CDCState.FORBIDDEN) {
            this.processNewSegment(allocatingFrom);
         }

      }

      private int defaultSegmentSize() {
         return DatabaseDescriptor.getCommitLogSegmentSize();
      }

      private void calculateSize() {
         try {
            this.sizeInProgress = 0L;
            Files.walkFileTree(this.path.toPath(), this);
            this.size = this.sizeInProgress;
         } catch (IOException var2) {
            CommitLog var10000 = CommitLog.instance;
            CommitLog.handleCommitError("Failed CDC Size Calculation", var2);
         }

      }

      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
         this.sizeInProgress += attrs.size();
         return FileVisitResult.CONTINUE;
      }

      private void addFlushedSize(long toAdd) {
         this.size += toAdd;
      }

      private long totalCDCSizeOnDisk() {
         return this.unflushedCDCSize + this.size;
      }

      public void shutdown() {
         this.cdcSizeCalculationExecutor.shutdown();
      }
   }
}
