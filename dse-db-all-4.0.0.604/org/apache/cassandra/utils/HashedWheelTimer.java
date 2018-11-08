package org.apache.cassandra.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.internal.ConcurrentSet;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;
import org.apache.cassandra.concurrent.TracingAwareExecutor;

public class HashedWheelTimer implements ParkedThreadsMonitor.MonitorableThread, Timer {
   static final InternalLogger logger = InternalLoggerFactory.getInstance(HashedWheelTimer.class);
   private static final ResourceLeakDetector<HashedWheelTimer> leakDetector = ResourceLeakDetectorFactory.instance().newResourceLeakDetector(HashedWheelTimer.class, 1);
   private final ResourceLeakTracker<HashedWheelTimer> leak;
   private final TimeSource timeSource;
   private final TracingAwareExecutor scheduler;
   private final HashedWheelTimer.Worker worker;
   private final long tickDuration;
   private final HashedWheelTimer.HashedWheelBucket[] wheel;
   private final int mask;
   private final Queue<HashedWheelTimer.HashedWheelTimeout> timeouts;
   private final Queue<HashedWheelTimer.HashedWheelTimeout> cancelledTimeouts;
   private final long startTimeInMillis;
   private volatile long nextTickAt;
   private volatile boolean initialized;
   private volatile boolean stopRequested;
   private final CountDownLatch stopped;
   private volatile ParkedThreadsMonitor.MonitorableThread.ThreadState workState;

   public HashedWheelTimer(TracingAwareExecutor scheduler) {
      this(scheduler, 100L, TimeUnit.MILLISECONDS);
   }

   public HashedWheelTimer(TracingAwareExecutor scheduler, long tickDuration, TimeUnit unit) {
      this(scheduler, tickDuration, unit, 512);
   }

   public HashedWheelTimer(TracingAwareExecutor scheduler, long tickDuration, TimeUnit unit, int ticksPerWheel) {
      this(scheduler, tickDuration, unit, ticksPerWheel, true);
   }

   public HashedWheelTimer(TracingAwareExecutor scheduler, long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection) {
      this(new ApproximateTimeSource(), scheduler, tickDuration, unit, ticksPerWheel, leakDetection);
   }

   public HashedWheelTimer(TimeSource timeSource, TracingAwareExecutor scheduler, long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection) {
      this.timeouts = PlatformDependent.newMpscQueue();
      this.cancelledTimeouts = PlatformDependent.newMpscQueue();
      if(scheduler == null) {
         throw new NullPointerException("scheduler");
      } else if(unit == null) {
         throw new NullPointerException("unit");
      } else if(tickDuration <= 0L) {
         throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
      } else if(ticksPerWheel <= 0) {
         throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
      } else {
         this.timeSource = timeSource;
         this.scheduler = scheduler;
         this.worker = new HashedWheelTimer.Worker();
         this.wheel = createWheel(ticksPerWheel);
         this.mask = this.wheel.length - 1;
         this.tickDuration = unit.toMillis(tickDuration);
         if(this.tickDuration <= 0L) {
            throw new IllegalArgumentException(String.format("tickDuration: %d must be at least one millisecond", new Object[]{Long.valueOf(tickDuration)}));
         } else if(this.tickDuration >= 9223372036854775807L / (long)this.wheel.length) {
            throw new IllegalArgumentException(String.format("tickDuration: %d (expected: 0 < tickDuration in millis < %d", new Object[]{Long.valueOf(tickDuration), Long.valueOf(9223372036854775807L / (long)this.wheel.length)}));
         } else {
            this.leak = leakDetection?leakDetector.track(this):null;
            this.startTimeInMillis = timeSource.currentTimeMillis();
            this.stopped = new CountDownLatch(1);
         }
      }
   }

   private static HashedWheelTimer.HashedWheelBucket[] createWheel(int ticksPerWheel) {
      if(ticksPerWheel <= 0) {
         throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
      } else if(ticksPerWheel > 1073741824) {
         throw new IllegalArgumentException("ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
      } else {
         ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
         HashedWheelTimer.HashedWheelBucket[] wheel = new HashedWheelTimer.HashedWheelBucket[ticksPerWheel];

         for(int i = 0; i < wheel.length; ++i) {
            wheel[i] = new HashedWheelTimer.HashedWheelBucket();
         }

         return wheel;
      }
   }

   private static int normalizeTicksPerWheel(int ticksPerWheel) {
      int normalizedTicksPerWheel;
      for(normalizedTicksPerWheel = 1; normalizedTicksPerWheel < ticksPerWheel; normalizedTicksPerWheel <<= 1) {
         ;
      }

      return normalizedTicksPerWheel;
   }

   public void start() {
      this.workState = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      this.scheduler.execute(() -> {
         this.worker.run();
      }, (ExecutorLocals)null);
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addThreadToMonitor(this);
   }

   public Set<Timeout> stop() {
      boolean var6 = false;

      Set var1;
      try {
         var6 = true;
         ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).removeThreadToMonitor(this);
         this.stopRequested = true;
         this.scheduler.execute(() -> {
            this.worker.run();
         }, (ExecutorLocals)null);
         if(!Uninterruptibles.awaitUninterruptibly(this.stopped, 5L, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Cannot stop timer!");
         }

         var1 = this.worker.unprocessedTimeouts();
         var6 = false;
      } finally {
         if(var6) {
            if(this.leak != null) {
               boolean closed = this.leak.close(this);

               assert closed;
            }

         }
      }

      if(this.leak != null) {
         boolean closed = this.leak.close(this);

         assert closed;
      }

      return var1;
   }

   public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
      if(task == null) {
         throw new NullPointerException("task");
      } else if(unit == null) {
         throw new NullPointerException("unit");
      } else if(this.stopRequested) {
         throw new IllegalStateException("Stop requested!");
      } else {
         long deadline = this.timeSource.currentTimeMillis() + unit.toMillis(delay) - this.startTimeInMillis;
         HashedWheelTimer.HashedWheelTimeout timeout = new HashedWheelTimer.HashedWheelTimeout(this, task, deadline);
         this.timeouts.add(timeout);
         return timeout;
      }
   }

   public boolean shouldUnpark(long ignored) {
      return this.workState == ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED && this.timeSource.currentTimeMillis() >= this.nextTickAt;
   }

   public void unpark() {
      this.workState = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      this.scheduler.execute(() -> {
         this.worker.run();
      }, (ExecutorLocals)null);
   }

   private static final class HashedWheelBucket {
      private HashedWheelTimer.HashedWheelTimeout head;
      private HashedWheelTimer.HashedWheelTimeout tail;

      private HashedWheelBucket() {
      }

      public void addTimeout(HashedWheelTimer.HashedWheelTimeout timeout) {
         assert timeout.bucket == null;

         timeout.bucket = this;
         if(this.head == null) {
            this.head = this.tail = timeout;
         } else {
            this.tail.next = timeout;
            timeout.prev = this.tail;
            this.tail = timeout;
         }

      }

      public void expireTimeouts(long deadline) {
         HashedWheelTimer.HashedWheelTimeout next;
         for(HashedWheelTimer.HashedWheelTimeout timeout = this.head; timeout != null; timeout = next) {
            next = timeout.next;
            if(timeout.remainingRounds <= 0L) {
               next = this.remove(timeout);
               if(timeout.deadline > deadline) {
                  throw new IllegalStateException(String.format("timeout.deadline (%d) > deadline (%d)", new Object[]{Long.valueOf(timeout.deadline), Long.valueOf(deadline)}));
               }

               timeout.expire();
            } else if(timeout.isCancelled()) {
               next = this.remove(timeout);
            } else {
               --timeout.remainingRounds;
            }
         }

      }

      public HashedWheelTimer.HashedWheelTimeout remove(HashedWheelTimer.HashedWheelTimeout timeout) {
         HashedWheelTimer.HashedWheelTimeout next = timeout.next;
         if(timeout.prev != null) {
            timeout.prev.next = next;
         }

         if(timeout.next != null) {
            timeout.next.prev = timeout.prev;
         }

         if(timeout == this.head) {
            if(timeout == this.tail) {
               this.tail = null;
               this.head = null;
            } else {
               this.head = next;
            }
         } else if(timeout == this.tail) {
            this.tail = timeout.prev;
         }

         timeout.prev = null;
         timeout.next = null;
         timeout.bucket = null;
         return next;
      }

      public void clearTimeouts(Set<Timeout> set) {
         while(true) {
            HashedWheelTimer.HashedWheelTimeout timeout = this.pollTimeout();
            if(timeout == null) {
               return;
            }

            if(!timeout.isExpired() && !timeout.isCancelled()) {
               set.add(timeout);
            }
         }
      }

      private HashedWheelTimer.HashedWheelTimeout pollTimeout() {
         HashedWheelTimer.HashedWheelTimeout head = this.head;
         if(head == null) {
            return null;
         } else {
            HashedWheelTimer.HashedWheelTimeout next = head.next;
            if(next == null) {
               this.tail = this.head = null;
            } else {
               this.head = next;
               next.prev = null;
            }

            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
         }
      }
   }

   private static final class HashedWheelTimeout implements Timeout {
      private static final int ST_INIT = 0;
      private static final int ST_CANCELLED = 1;
      private static final int ST_EXPIRED = 2;
      private static final AtomicIntegerFieldUpdater<HashedWheelTimer.HashedWheelTimeout> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.HashedWheelTimeout.class, "state");
      private final HashedWheelTimer timer;
      private final TimerTask task;
      private final long deadline;
      private volatile int state = 0;
      long remainingRounds;
      HashedWheelTimer.HashedWheelTimeout next;
      HashedWheelTimer.HashedWheelTimeout prev;
      HashedWheelTimer.HashedWheelBucket bucket;

      HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
         this.timer = timer;
         this.task = task;
         this.deadline = deadline;
      }

      public Timer timer() {
         return this.timer;
      }

      public TimerTask task() {
         return this.task;
      }

      public boolean cancel() {
         if(!this.compareAndSetState(0, 1)) {
            return false;
         } else {
            this.timer.cancelledTimeouts.add(this);
            return true;
         }
      }

      void remove() {
         HashedWheelTimer.HashedWheelBucket bucket = this.bucket;
         if(bucket != null) {
            bucket.remove(this);
         }

      }

      public boolean compareAndSetState(int expected, int state) {
         return STATE_UPDATER.compareAndSet(this, expected, state);
      }

      public int state() {
         return this.state;
      }

      public boolean isCancelled() {
         return this.state() == 1;
      }

      public boolean isExpired() {
         return this.state() == 2;
      }

      public void expire() {
         if(this.compareAndSetState(0, 2)) {
            try {
               this.task.run(this);
            } catch (Throwable var2) {
               if(HashedWheelTimer.logger.isWarnEnabled()) {
                  HashedWheelTimer.logger.warn("An exception was thrown by timer task " + this.task.getClass().getSimpleName() + '.', var2);
               }
            }

         }
      }

      public String toString() {
         long currentTime = this.timer.timeSource.currentTimeMillis();
         long remaining = this.deadline - currentTime + this.timer.startTimeInMillis;
         StringBuilder buf = (new StringBuilder(192)).append(StringUtil.simpleClassName(this)).append('(').append("deadline: ");
         if(remaining > 0L) {
            buf.append(remaining).append(" ns later");
         } else if(remaining < 0L) {
            buf.append(-remaining).append(" ns ago");
         } else {
            buf.append("now");
         }

         if(this.isCancelled()) {
            buf.append(", cancelled");
         }

         return buf.append(", task: ").append(this.task()).append(')').toString();
      }
   }

   private final class Worker implements Runnable {
      private final ConcurrentSet<Timeout> unprocessedTimeouts;
      private long tick;

      private Worker() {
         this.unprocessedTimeouts = new ConcurrentSet();
         this.tick = 0L;
      }

      public void run() {
         int idx;
         if(!HashedWheelTimer.this.stopRequested) {
            if(HashedWheelTimer.this.initialized) {
               long currentTime = HashedWheelTimer.this.timeSource.currentTimeMillis();
               this.processCancelledTasks();
               this.transferTimeoutsToBuckets();

               while(currentTime >= HashedWheelTimer.this.nextTickAt) {
                  idx = (int)(this.tick & (long)HashedWheelTimer.this.mask);
                  HashedWheelTimer.this.wheel[idx].expireTimeouts(currentTime);
                  ++this.tick;
                  HashedWheelTimer.this.nextTickAt = this.computeNextTick();
               }
            } else {
               HashedWheelTimer.this.initialized = true;
               HashedWheelTimer.this.nextTickAt = this.computeNextTick();
            }
         } else if(HashedWheelTimer.this.stopped.getCount() == 1L) {
            HashedWheelTimer.HashedWheelBucket[] var5 = HashedWheelTimer.this.wheel;
            int var2 = var5.length;

            for(idx = 0; idx < var2; ++idx) {
               HashedWheelTimer.HashedWheelBucket bucket = var5[idx];
               bucket.clearTimeouts(this.unprocessedTimeouts);
            }

            while(true) {
               HashedWheelTimer.HashedWheelTimeout timeout = (HashedWheelTimer.HashedWheelTimeout)HashedWheelTimer.this.timeouts.poll();
               if(timeout == null) {
                  this.processCancelledTasks();
                  HashedWheelTimer.this.stopped.countDown();
                  break;
               }

               if(!timeout.isCancelled()) {
                  this.unprocessedTimeouts.add(timeout);
               }
            }
         }

         HashedWheelTimer.this.workState = ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED;
      }

      private void transferTimeoutsToBuckets() {
         for(int i = 0; i < 100000; ++i) {
            HashedWheelTimer.HashedWheelTimeout timeout = (HashedWheelTimer.HashedWheelTimeout)HashedWheelTimer.this.timeouts.poll();
            if(timeout == null) {
               break;
            }

            if(timeout.state() != 1) {
               long calculated = timeout.deadline / HashedWheelTimer.this.tickDuration;
               timeout.remainingRounds = (calculated - this.tick) / (long)HashedWheelTimer.this.wheel.length;
               long ticks = Math.max(calculated, this.tick);
               int stopIndex = (int)(ticks & (long)HashedWheelTimer.this.mask);
               HashedWheelTimer.HashedWheelBucket bucket = HashedWheelTimer.this.wheel[stopIndex];
               bucket.addTimeout(timeout);
            }
         }

      }

      private void processCancelledTasks() {
         while(true) {
            HashedWheelTimer.HashedWheelTimeout timeout = (HashedWheelTimer.HashedWheelTimeout)HashedWheelTimer.this.cancelledTimeouts.poll();
            if(timeout == null) {
               return;
            }

            try {
               timeout.remove();
            } catch (Throwable var3) {
               if(HashedWheelTimer.logger.isWarnEnabled()) {
                  HashedWheelTimer.logger.warn("An exception was thrown while process a cancellation task", var3);
               }
            }
         }
      }

      private long computeNextTick() {
         long deadline = HashedWheelTimer.this.tickDuration * (this.tick + 1L);
         return deadline + HashedWheelTimer.this.startTimeInMillis;
      }

      public Set<Timeout> unprocessedTimeouts() {
         return Collections.unmodifiableSet(this.unprocessedTimeouts);
      }
   }
}
