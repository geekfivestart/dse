package org.apache.cassandra.db.commitlog;

import io.reactivex.Completable;
import io.reactivex.functions.Action;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.flow.RxThreads;

class PeriodicCommitLogService extends AbstractCommitLogService {
   private static final long blockWhenSyncLagsNanos = (long)((double)DatabaseDescriptor.getCommitLogSyncPeriod() * 1500000.0D);

   public PeriodicCommitLogService(CommitLog commitLog, TimeSource timeSource) {
      super(commitLog, "PERIODIC-COMMIT-LOG-SYNCER", (long)DatabaseDescriptor.getCommitLogSyncPeriod(), timeSource);
   }

   protected Completable maybeWaitForSync(CommitLogSegment.Allocation alloc, StagedScheduler observeOn) {
      long expectedSyncTime = this.timeSource.nanoTime() - blockWhenSyncLagsNanos;
      if(this.lastSyncedAt < expectedSyncTime) {
         this.pending.incrementAndGet();
         long startTime = this.timeSource.nanoTime();
         Completable sync = this.awaitSyncAt(expectedSyncTime).doOnComplete(() -> {
            this.commitLog.metrics.waitingOnCommit.update(this.timeSource.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            this.pending.decrementAndGet();
         });
         return RxThreads.awaitAndContinueOn(sync, observeOn, TPCTaskType.WRITE_POST_COMMIT_LOG_SYNC);
      } else {
         return Completable.complete();
      }
   }
}
