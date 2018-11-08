package org.apache.cassandra.db.commitlog;

import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import java.io.File;
import java.util.function.BiConsumer;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;

public class CommitLogSegmentManagerStandard extends AbstractCommitLogSegmentManager {
   public CommitLogSegmentManagerStandard(CommitLog commitLog, String storageDirectory) {
      super(commitLog, storageDirectory);
   }

   public void discard(CommitLogSegment segment, boolean delete) {
      segment.close();
      if(delete) {
         FileUtils.deleteWithConfirm(segment.logFile);
      }

      this.addSize(-segment.onDiskSize());
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
                        CommitLogSegment segment = CommitLogSegmentManagerStandard.this.allocatingFrom();
                        if(AbstractCommitLogSegmentManager.logger.isTraceEnabled()) {
                           AbstractCommitLogSegmentManager.logger.trace("Allocating mutation of size {} on segment {} with space {}", new Object[]{Integer.valueOf(size), Long.valueOf(segment.id), Long.valueOf(segment.availableSize())});
                        }

                        CommitLogSegment.Allocation alloc = segment.allocate(mutation, size);
                        if(alloc != null) {
                           observer.onSuccess(alloc);
                        } else {
                           if(AbstractCommitLogSegmentManager.logger.isTraceEnabled()) {
                              AbstractCommitLogSegmentManager.logger.trace("Waiting for segment allocation...");
                           }

                           StagedScheduler scheduler = mutation.getScheduler();
                           TPCRunnable us = TPCRunnable.wrap(this, ExecutorLocals.create(), TPCTaskType.WRITE_POST_COMMIT_LOG_SEGMENT, scheduler);
                           CommitLogSegmentManagerStandard.this.advanceAllocatingFrom(segment).whenComplete((ignored, error) -> {
                              try {
                                 if(error == null) {
                                    scheduler.execute(us);
                                 }
                              } catch (Throwable var6) {
                                 error = var6;
                              }

                              if(error != null) {
                                 AbstractCommitLogSegmentManager.logger.debug("Got exception whilst allocating CL segment: {}", error.getMessage());
                                 us.cancelled();
                                 observer.onError(error);
                              }

                           });
                        }
                     } catch (Throwable var5) {
                        AbstractCommitLogSegmentManager.logger.debug("Got exception whilst allocating CL segment: {}", var5.getMessage());
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

   void handleReplayedSegment(File file) {
      logger.trace("(Unopened) segment {} is no longer needed and will be deleted now", file);
      FileUtils.deleteWithConfirm(file);
   }

   public CommitLogSegment createSegment() {
      return CommitLogSegment.createSegment(this.commitLog, this);
   }
}
