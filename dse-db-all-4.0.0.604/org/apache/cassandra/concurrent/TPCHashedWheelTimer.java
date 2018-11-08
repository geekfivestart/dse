package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.ApproximateTimeSource;
import org.apache.cassandra.utils.HashedWheelTimer;
import org.apache.cassandra.utils.TimeSource;

public class TPCHashedWheelTimer implements TPCTimer {
   private final TPCScheduler scheduler;
   private final HashedWheelTimer timer;

   public TPCHashedWheelTimer(TPCScheduler scheduler) {
      this(new ApproximateTimeSource(), scheduler);
   }

   @VisibleForTesting
   public TPCHashedWheelTimer(TimeSource timeSource, TPCScheduler scheduler) {
      this.scheduler = scheduler;
      this.timer = new HashedWheelTimer(timeSource, scheduler.forTaskType(TPCTaskType.TIMED_TIMEOUT), 100L, TimeUnit.MILLISECONDS, 512, false);
      this.timer.start();
   }

   public TPCScheduler getScheduler() {
      return this.scheduler;
   }

   public Disposable onTimeout(Runnable task, long timeout, TimeUnit unit) {
      final Timeout handle = this.timer.newTimeout((ignored) -> {
         task.run();
      }, timeout, unit);
      return new Disposable() {
         public void dispose() {
            handle.cancel();
         }

         public boolean isDisposed() {
            return handle.isCancelled();
         }
      };
   }
}
