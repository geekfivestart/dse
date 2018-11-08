package org.apache.cassandra.db.commitlog;

import io.reactivex.Completable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.BehaviorSubject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCommitLogService {
   private Thread thread;
   private volatile boolean shutdown = false;
   protected volatile long lastSyncedAt;
   private final AtomicLong written = new AtomicLong(0L);
   protected final AtomicLong pending = new AtomicLong(0L);
   private BehaviorSubject<Long> syncTimePublisher = BehaviorSubject.createDefault(Long.valueOf(0L));
   final CommitLog commitLog;
   private final String name;
   private final long pollIntervalNanos;
   final TimeSource timeSource;
   private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogService.class);

   AbstractCommitLogService(CommitLog commitLog, String name, long pollIntervalMillis, TimeSource timeSource) {
      this.commitLog = commitLog;
      this.name = name;
      this.pollIntervalNanos = TimeUnit.NANOSECONDS.convert(pollIntervalMillis, TimeUnit.MILLISECONDS);
      this.timeSource = timeSource;
      this.lastSyncedAt = timeSource.nanoTime();
   }

   void start() {
      if(this.pollIntervalNanos < 1L) {
         throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %fms", new Object[]{Double.valueOf((double)this.pollIntervalNanos * 1.0E-6D)}));
      } else {
         Runnable runnable = new Runnable() {
            public void run() {
               long firstLagAt = 0L;
               long totalSyncDuration = 0L;
               long syncExceededIntervalBy = 0L;
               int lagCount = 0;
               int syncCount = 0;

               while(true) {
                  boolean shutdownRequested = AbstractCommitLogService.this.shutdown;

                  try {
                     long syncStarted = AbstractCommitLogService.this.timeSource.nanoTime();
                     AbstractCommitLogService.this.commitLog.sync();
                     AbstractCommitLogService.this.lastSyncedAt = syncStarted;
                     AbstractCommitLogService.this.syncTimePublisher.onNext(Long.valueOf(syncStarted));
                     long now = AbstractCommitLogService.this.timeSource.nanoTime();
                     long wakeUpAt = syncStarted + AbstractCommitLogService.this.pollIntervalNanos;
                     if(wakeUpAt - now < 0L) {
                        if(lagCount == 0) {
                           firstLagAt = now;
                           lagCount = 0;
                           syncCount = 0;
                           totalSyncDuration = syncExceededIntervalBy = (long)0;
                        }

                        syncExceededIntervalBy += Math.abs(now - wakeUpAt);
                        ++lagCount;
                     }

                     ++syncCount;
                     totalSyncDuration += Math.abs(now - syncStarted);
                     if(lagCount != 0) {
                        boolean logged = NoSpamLogger.log(AbstractCommitLogService.logger, NoSpamLogger.Level.WARN, 5L, TimeUnit.MINUTES, "Out of {} commit log syncs over the past {}s with average duration of {}ms, {} have exceeded the configured commit interval by an average of {}ms", new Object[]{Integer.valueOf(syncCount), String.format("%.2f", new Object[]{Double.valueOf((double)Math.abs(now - firstLagAt) * 1.0E-9D)}), String.format("%.2f", new Object[]{Double.valueOf((double)totalSyncDuration * 1.0E-6D / (double)syncCount)}), Integer.valueOf(lagCount), String.format("%.2f", new Object[]{Double.valueOf((double)syncExceededIntervalBy * 1.0E-6D / (double)lagCount)})});
                        if(logged) {
                           lagCount = 0;
                        }
                     }

                     if(shutdownRequested) {
                        return;
                     }

                     if(wakeUpAt - now > 0L) {
                        LockSupport.parkNanos(Math.abs(wakeUpAt - now));
                     }
                  } catch (Throwable var17) {
                     if(!CommitLog.handleCommitError("Failed to persist commits to disk", var17)) {
                        AbstractCommitLogService.this.syncTimePublisher.onError(var17);
                        return;
                     }

                     LockSupport.parkNanos(AbstractCommitLogService.this.pollIntervalNanos);
                  }
               }
            }
         };
         this.shutdown = false;
         this.thread = NamedThreadFactory.createThread(runnable, this.name);
         this.thread.setDaemon(true);
         this.thread.start();
      }
   }

   public Completable finishWriteFor(CommitLogSegment.Allocation alloc, StagedScheduler observeOn) {
      return this.maybeWaitForSync(alloc, observeOn).doOnComplete(() -> {
         this.written.incrementAndGet();
      });
   }

   protected abstract Completable maybeWaitForSync(CommitLogSegment.Allocation var1, StagedScheduler var2);

   public void requestExtraSync() {
      LockSupport.unpark(this.thread);
   }

   public void shutdown() {
      this.shutdown = true;
      this.requestExtraSync();
   }

   public void syncBlocking() {
      long requestTime = this.timeSource.nanoTime();
      this.requestExtraSync();
      this.awaitSyncAt(requestTime).blockingGet();
   }

   Completable awaitSyncAt(long syncTime) {
      return this.syncTimePublisher.filter((v) -> {
         return v.longValue() - syncTime >= 0L;
      }).first(Long.valueOf(0L)).toCompletable();
   }

   public void awaitTermination() throws InterruptedException {
      this.thread.join();
   }

   public long getCompletedTasks() {
      return this.written.get();
   }

   public long getPendingTasks() {
      return this.pending.get();
   }
}
