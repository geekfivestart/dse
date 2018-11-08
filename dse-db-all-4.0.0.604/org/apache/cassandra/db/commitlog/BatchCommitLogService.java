package org.apache.cassandra.db.commitlog;

import io.reactivex.Completable;
import io.reactivex.functions.Action;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.flow.RxThreads;

class BatchCommitLogService extends AbstractCommitLogService {
   private static final int POLL_TIME_MILLIS = 1000;

   public BatchCommitLogService(CommitLog commitLog, TimeSource timeSource) {
      super(commitLog, "COMMIT-LOG-WRITER", 1000L, timeSource);
   }

   protected Completable maybeWaitForSync(CommitLogSegment.Allocation alloc, StagedScheduler observeOn) {
      this.pending.incrementAndGet();
      long startTime = this.timeSource.nanoTime();
      this.requestExtraSync();
      Completable sync = this.awaitSyncAt(startTime).doOnComplete(() -> {
         this.commitLog.metrics.waitingOnCommit.update(this.timeSource.nanoTime() - startTime, TimeUnit.NANOSECONDS);
         this.pending.decrementAndGet();
      });
      return RxThreads.awaitAndContinueOn(sync, observeOn, TPCTaskType.WRITE_POST_COMMIT_LOG_SYNC);
   }
}
