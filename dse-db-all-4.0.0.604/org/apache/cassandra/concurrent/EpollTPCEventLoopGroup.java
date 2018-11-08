package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.channel.epoll.AIOContext.Config;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;

public class EpollTPCEventLoopGroup extends MultithreadEventLoopGroup implements TPCEventLoopGroup {
   private static final String DEBUG_RUNNING_TIME_NAME = "dse.tpc.debug_task_running_time_seconds";
   private static final long DEBUG_RUNNING_TIME_NANOS;
   private static final Logger LOGGER;
   private static final boolean DISABLE_BACKPRESSURE;
   private static final int REMOTE_BACKPRESSURE_MULTIPLIER;
   private static final int GLOBAL_BACKPRESSURE_MULTIPLIER;
   private static final long SCHEDULED_CHECK_INTERVAL_NANOS;
   private static final long EPOLL_CHECK_INTERVAL_NANOS;
   private static final boolean DO_EPOLL_CHECK;
   private static final long BUSY_BACKOFF;
   private static final long YIELD_BACKOFF;
   private static final long PARK_BACKOFF;
   private static final long SKIP_BACKOFF_STAGE = 0L;
   private static final long LAST_BACKOFF_STAGE = -1L;
   private static final int TASKS_LIMIT;
   @Contended
   private final ImmutableList<EpollTPCEventLoopGroup.SingleCoreEventLoop> eventLoops;
   private volatile boolean shutdown;

   @VisibleForTesting
   public EpollTPCEventLoopGroup(int nThreads, String name) {
      this(nThreads);
   }

   public EpollTPCEventLoopGroup(int nThreads) {
      super(nThreads, TPCThread.newTPCThreadFactory(), new Object[0]);
      this.eventLoops = ImmutableList.copyOf(Iterables.transform(this, (e) -> {
         return (EpollTPCEventLoopGroup.SingleCoreEventLoop)e;
      }));
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addThreadsToMonitor(new ArrayList(this.eventLoops));
      if(!DISABLE_BACKPRESSURE) {
         LOGGER.info("Enabled TPC backpressure with {} pending requests limit, remote multiplier at {}, global multiplier at {}", new Object[]{Integer.valueOf(DatabaseDescriptor.getTPCPendingRequestsLimit()), Integer.valueOf(REMOTE_BACKPRESSURE_MULTIPLIER), Integer.valueOf(GLOBAL_BACKPRESSURE_MULTIPLIER)});
      } else {
         LOGGER.warn("TPC backpressure is disabled. NOT RECOMMENDED.");
      }

   }

   public ImmutableList<? extends TPCEventLoop> eventLoops() {
      return this.eventLoops;
   }

   public void shutdown() {
      super.shutdown();
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).removeThreadsToMonitor(new ArrayList(this.eventLoops));
      this.shutdown = true;
   }

   protected EventLoop newChild(Executor executor, Object... args) throws Exception {
      assert executor instanceof TPCThread.TPCThreadsCreator;

      TPCThread.TPCThreadsCreator tpcThreadsCreator = (TPCThread.TPCThreadsCreator)executor;
      int nextCore = tpcThreadsCreator.lastCreatedThread() == null?0:tpcThreadsCreator.lastCreatedThread().coreId() + 1;
      return new EpollTPCEventLoopGroup.SingleCoreEventLoop(this, tpcThreadsCreator, TPC.aioCoordinator.getIOConfig(nextCore));
   }

   static {
      DEBUG_RUNNING_TIME_NANOS = TimeUnit.SECONDS.toNanos((long)Integer.parseInt(System.getProperty("dse.tpc.debug_task_running_time_seconds", "0")));
      LOGGER = LoggerFactory.getLogger(EpollTPCEventLoopGroup.class);
      DISABLE_BACKPRESSURE = Boolean.parseBoolean(System.getProperty("dse.tpc.disable_backpressure", "false"));
      REMOTE_BACKPRESSURE_MULTIPLIER = Integer.parseInt(System.getProperty("dse.tpc.remote_backpressure_multiplier", "5"));
      GLOBAL_BACKPRESSURE_MULTIPLIER = Integer.parseInt(System.getProperty("dse.tpc.global_backpressure_multiplier", Integer.toString(Math.max(10, DatabaseDescriptor.getTPCCores()))));
      SCHEDULED_CHECK_INTERVAL_NANOS = Long.parseLong(System.getProperty("netty.schedule_check_interval_nanos", "1000"));
      EPOLL_CHECK_INTERVAL_NANOS = Long.parseLong(System.getProperty("netty.epoll_check_interval_nanos", "2000"));
      DO_EPOLL_CHECK = EPOLL_CHECK_INTERVAL_NANOS != -1L;
      BUSY_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.busy_extra_spins", "10"));
      YIELD_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.yield_extra_spins", "0"));
      PARK_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.park_extra_spins", "0"));
      TASKS_LIMIT = Integer.parseInt(System.getProperty("netty.eventloop.tasks_processing_limit", "1024"));
   }

   public static class SingleCoreEventLoop extends EpollEventLoop implements TPCEventLoop, ParkedThreadsMonitor.MonitorableThread {
      private volatile long lastDrainTime;
      private final EpollTPCEventLoopGroup parent;
      private final TPCThread thread;
      private final TPCMetrics metrics;
      private final TPCMetricsAndLimits.TaskStats busySpinStats;
      private final TPCMetricsAndLimits.TaskStats yieldStats;
      private final TPCMetricsAndLimits.TaskStats parkStats;
      private final MpscArrayQueue<Runnable> queue;
      private final MpscArrayQueue<TPCRunnable> pendingQueue;
      private final MpscArrayQueue<Runnable> priorityQueue;
      @Contended
      private volatile ParkedThreadsMonitor.MonitorableThread.ThreadState state;
      private final CountDownLatch racyInit;
      private int pendingEpollEvents;
      private long lastEpollCheckTime;
      private long lastScheduledCheckTime;
      private boolean hasGlobalBackpressure;
      private static final int LOCAL_BACKPRESSURE_THRESHOLD = DatabaseDescriptor.getTPCPendingRequestsLimit();
      private static final int REMOTE_BACKPRESSURE_THRESHOLD;
      private static final int GLOBAL_BACKPRESSURE_THRESHOLD;

      private SingleCoreEventLoop(EpollTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor, Config aio) {
         super(parent, executor, 0, DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject(), aio);
         this.racyInit = new CountDownLatch(1);
         this.pendingEpollEvents = 0;
         this.lastEpollCheckTime = nanoTimeSinceStartup();
         this.lastScheduledCheckTime = this.lastEpollCheckTime;
         this.parent = parent;
         this.queue = new MpscArrayQueue(65536);
         this.pendingQueue = new MpscArrayQueue(65536);
         this.priorityQueue = new MpscArrayQueue(16);
         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
         this.lastDrainTime = -1L;
         this.submit(() -> {
         });
         this.thread = executor.lastCreatedThread();

         assert this.thread != null;

         TPCMetricsAndLimits metrics = (TPCMetricsAndLimits)this.thread.metrics();
         this.metrics = metrics;
         this.busySpinStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SPIN);
         this.yieldStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_YIELD);
         this.parkStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_PARK);
      }

      public void start() {
         this.racyInit.countDown();
      }

      public boolean shouldBackpressure(boolean remote) {
         if(EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE) {
            return false;
         } else if(remote && this.remoteBackpressure()) {
            NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Remote TPC backpressure is active with count {}.", new Object[]{Long.valueOf(this.metrics.backpressureCountedRemoteTaskCount())});
            return true;
         } else if(!remote && this.localBackpressure()) {
            NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Local TPC backpressure is active with count {}.", new Object[]{Long.valueOf(this.metrics.backpressureCountedLocalTaskCount())});
            return true;
         } else {
            int globallyBackpressuredCores = TPCMetrics.globallyBackpressuredCores();
            if(globallyBackpressuredCores > 0) {
               NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Global TPC backpressure is active, thread count is {}.", new Object[]{Integer.valueOf(globallyBackpressuredCores)});
               return true;
            } else {
               return false;
            }
         }
      }

      private boolean localBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedLocalTaskCount() >= (long)LOCAL_BACKPRESSURE_THRESHOLD;
      }

      private boolean remoteBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedRemoteTaskCount() >= (long)REMOTE_BACKPRESSURE_THRESHOLD;
      }

      private boolean globalBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedTotalTaskCount() >= (long)GLOBAL_BACKPRESSURE_THRESHOLD;
      }

      public TPCThread thread() {
         return this.thread;
      }

      public TPCEventLoopGroup parent() {
         return this.parent;
      }

      public boolean wakesUpForTask(Runnable task) {
         return false;
      }

      public void unpark() {
         Native.eventFdWrite(this.eventFd.intValue(), 1L);
      }

      public boolean shouldUnpark(long nanoTimeSinceStartup) {
         if(EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS > 0L && this.state == ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING) {
            this.checkLongRunningTasks(nanoTimeSinceStartup);
         }

         return this.state == ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED && (this.hasQueueTasks() || this.hasPendingTasks());
      }

      public boolean canExecuteImmediately(TPCTaskType taskType) {
         return this.coreId() == TPC.getCoreId() && !taskType.alwaysEnqueue()?(!taskType.pendable()?true:(!this.pendingQueue.isEmpty()?false:this.queue.size() < this.metrics.maxQueueSize())):false;
      }

      protected void run() {
         Uninterruptibles.awaitUninterruptibly(this.racyInit);

         while(!this.parent.shutdown) {
            try {
               if(this.processEvents(nanoTimeSinceStartup()) == 0) {
                  this.waitForWork();
               }
            } catch (Throwable var2) {
               JVMStabilityInspector.inspectThrowable(var2);
               EpollTPCEventLoopGroup.LOGGER.error("Error in event loop:", var2);
            }
         }

         if(this.isShuttingDown()) {
            this.closeAll();
            this.confirmShutdown();
         }

      }

      protected boolean removeTask(Runnable task) {
         return true;
      }

      protected void addTask(Runnable task) {
         if(task instanceof TPCRunnable) {
            TPCRunnable tpc = (TPCRunnable)task;
            if(tpc.hasPriority() && this.priorityQueue.relaxedOffer(tpc)) {
               return;
            }

            if(tpc.isPendable()) {
               if(this.pendingQueue.isEmpty() && this.queue.offerIfBelowThreshold(tpc, this.metrics.maxQueueSize())) {
                  return;
               }

               if(this.pendingQueue.relaxedOffer(tpc)) {
                  tpc.setPending();
                  return;
               }

               tpc.blocked();
               this.reject(task);
            }
         }

         if(!this.queue.relaxedOffer(task)) {
            this.reject(task);
         }

      }

      protected boolean hasTasks() {
         assert this.inEventLoop();

         return this.hasQueueTasks() || this.hasPendingTasks() || this.throttledHasScheduledEvents(nanoTimeSinceStartup());
      }

      private void waitForWork() {
         int idle = 0;

         boolean shouldContinue;
         do {
            ++idle;
            shouldContinue = this.backoff(idle);
         } while(!this.parent.shutdown && shouldContinue && this.isIdle());

      }

      private boolean isIdle() {
         if(!this.hasQueueTasks() && !this.hasPendingTasks()) {
            long nanoTimeSinceStartup = nanoTimeSinceStartup();
            return !this.throttledHasScheduledEvents(nanoTimeSinceStartup) && !this.throttledHasEpollEvents(nanoTimeSinceStartup);
         } else {
            return false;
         }
      }

      private boolean throttledHasScheduledEvents(long nanoTimeSinceStartup) {
         if(nanoTimeSinceStartup - this.lastScheduledCheckTime > EpollTPCEventLoopGroup.SCHEDULED_CHECK_INTERVAL_NANOS) {
            boolean result = this.hasScheduledTasks(nanoTimeSinceStartup);
            if(!result) {
               this.lastScheduledCheckTime = nanoTimeSinceStartup;
            }

            return result;
         } else {
            return false;
         }
      }

      private boolean throttledHasEpollEvents(long nanoTimeSinceStartup) {
         if(EpollTPCEventLoopGroup.DO_EPOLL_CHECK && nanoTimeSinceStartup - this.lastEpollCheckTime > EpollTPCEventLoopGroup.EPOLL_CHECK_INTERVAL_NANOS && this.pendingEpollEvents == 0) {
            this.epollSelectNow(nanoTimeSinceStartup);
         }

         return this.pendingEpollEvents != 0;
      }

      private boolean backoff(int idle) {
         if(EpollTPCEventLoopGroup.BUSY_BACKOFF != 0L && (EpollTPCEventLoopGroup.BUSY_BACKOFF == -1L || (long)idle < EpollTPCEventLoopGroup.BUSY_BACKOFF)) {
            this.busySpinStats.scheduledTasks.add(1L);
            this.busySpinStats.completedTasks.add(1L);
         } else if(EpollTPCEventLoopGroup.YIELD_BACKOFF != 0L && (EpollTPCEventLoopGroup.YIELD_BACKOFF == -1L || (long)idle < EpollTPCEventLoopGroup.BUSY_BACKOFF + EpollTPCEventLoopGroup.YIELD_BACKOFF)) {
            this.yieldStats.scheduledTasks.add(1L);
            this.yieldStats.completedTasks.add(1L);
            Thread.yield();
            this.lastEpollCheckTime = -1L;
         } else {
            if(EpollTPCEventLoopGroup.PARK_BACKOFF == 0L || EpollTPCEventLoopGroup.PARK_BACKOFF != -1L && (long)idle >= EpollTPCEventLoopGroup.BUSY_BACKOFF + EpollTPCEventLoopGroup.YIELD_BACKOFF + EpollTPCEventLoopGroup.PARK_BACKOFF) {
               this.parkOnEpollWait();
               return false;
            }

            this.parkStats.scheduledTasks.add(1L);
            this.parkStats.completedTasks.add(1L);
            LockSupport.parkNanos(1L);
            this.lastEpollCheckTime = -1L;
         }

         return true;
      }

      private void parkOnEpollWait() {
         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED;
         this.epollSelect();
         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      }

      private void checkLongRunningTasks(long nanoTimeSinceStartup) {
         if(this.lastDrainTime > 0L && Math.abs(nanoTimeSinceStartup - this.lastDrainTime) > EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS) {
            if(EpollTPCEventLoopGroup.LOGGER.isDebugEnabled()) {
               EpollTPCEventLoopGroup.LOGGER.debug("Detected task running for {} seconds for thread with stack:\n{}", Long.valueOf(TimeUnit.SECONDS.convert(Math.abs(nanoTimeSinceStartup - this.lastDrainTime), TimeUnit.NANOSECONDS)), FBUtilities.Debug.getStackTrace((Thread)this.thread));
            }

            this.lastDrainTime = -1L;
         }

      }

      private int processEvents(long nanoTimeSinceStartup) {
         if(EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS > 0L) {
            this.lastDrainTime = nanoTimeSinceStartup;
         }

         int processed = 0;
         if(this.throttledHasEpollEvents(nanoTimeSinceStartup)) {
            processed += this.processEpollEvents();
         }

         if(nanoTimeSinceStartup - this.lastScheduledCheckTime > EpollTPCEventLoopGroup.SCHEDULED_CHECK_INTERVAL_NANOS) {
            processed += this.runScheduledTasks(nanoTimeSinceStartup);
         }

         processed += this.processTasks();
         processed += this.transferFromPendingTasks();
         return processed;
      }

      private int processEpollEvents() {
         int currPendingEpollEvents = this.pendingEpollEvents;
         if(currPendingEpollEvents == 0) {
            return 0;
         } else {
            this.pendingEpollEvents = 0;

            try {
               this.processReady(this.events, currPendingEpollEvents);
               if(this.allowGrowing && currPendingEpollEvents == this.events.length()) {
                  this.events.increase();
               }

               return currPendingEpollEvents;
            } catch (Throwable var3) {
               this.handleEpollEventError(var3);
               return 0;
            }
         }
      }

      private void epollSelect() {
         if(this.pendingEpollEvents != 0) {
            throw new IllegalStateException("Should not be doing a blocking select with pendingEpollEvents=" + this.pendingEpollEvents);
         } else {
            try {
               this.pendingEpollEvents = this.epollWait(false);
               this.lastEpollCheckTime = nanoTimeSinceStartup();

               assert this.pendingEpollEvents >= 0;
            } catch (Exception var2) {
               EpollTPCEventLoopGroup.LOGGER.error("Error selecting socket ", var2);
            }

         }
      }

      private void epollSelectNow(long nanoTimeSinceStartup) {
         if(this.pendingEpollEvents != 0) {
            throw new IllegalStateException("Should not be doing a selectNow with pendingEpollEvents=" + this.pendingEpollEvents);
         } else {
            this.lastEpollCheckTime = nanoTimeSinceStartup;

            try {
               this.pendingEpollEvents = this.selectNowSupplier.get();

               assert this.pendingEpollEvents >= 0;
            } catch (Exception var4) {
               EpollTPCEventLoopGroup.LOGGER.error("Error selecting socket ", var4);
            }

         }
      }

      private void handleEpollEventError(Throwable e) {
         EpollTPCEventLoopGroup.LOGGER.error("Unexpected exception in the selector loop.", e);
         Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
      }

      private int processTasks() {
         int processed = 0;

         try {
            MpscArrayQueue<Runnable> queue = this.queue;
            MpscArrayQueue priorityQueue = this.priorityQueue;

            Runnable p;
            Runnable q;
            do {
               p = (Runnable)priorityQueue.relaxedPoll();
               if(p != null) {
                  p.run();
                  ++processed;
               }

               q = (Runnable)queue.relaxedPoll();
               if(q != null) {
                  q.run();
                  ++processed;
               }
            } while((p != null || q != null) && processed < EpollTPCEventLoopGroup.TASKS_LIMIT - 1);
         } catch (Throwable var6) {
            this.handleTaskException(var6);
         }

         return processed;
      }

      private void handleTaskException(Throwable t) {
         JVMStabilityInspector.inspectThrowable(t);
         EpollTPCEventLoopGroup.LOGGER.error("Task exception encountered: ", t);

         try {
            RxJavaPlugins.getErrorHandler().accept(t);
         } catch (Exception var3) {
            throw new RuntimeException(var3);
         }
      }

      private int transferFromPendingTasks() {
         try {
            int processed = 0;
            int maxQueueSize = this.metrics.maxQueueSize();
            MpscArrayQueue pendingQueue = this.pendingQueue;

            TPCRunnable tpc;
            while(this.queue.size() < maxQueueSize && (tpc = (TPCRunnable)pendingQueue.relaxedPeek()) != null && this.queue.relaxedOffer(tpc)) {
               ++processed;
               pendingQueue.relaxedPoll();
               tpc.unsetPending();
            }

            int var5 = processed;
            return var5;
         } finally {
            this.checkGlobalBackpressure();
         }
      }

      private int runScheduledTasks(long nanoTimeSinceStartup) {
         this.lastScheduledCheckTime = nanoTimeSinceStartup;

         int processed;
         Runnable scheduledTask;
         for(processed = 0; processed < EpollTPCEventLoopGroup.TASKS_LIMIT && (scheduledTask = this.pollScheduledTask(nanoTimeSinceStartup)) != null; ++processed) {
            try {
               scheduledTask.run();
            } catch (Throwable var6) {
               this.handleTaskException(var6);
            }
         }

         return processed;
      }

      private void checkGlobalBackpressure() {
         if(this.metrics != null) {
            boolean hadGlobalBackpressure = this.hasGlobalBackpressure;
            this.hasGlobalBackpressure = this.globalBackpressure();
            if(hadGlobalBackpressure != this.hasGlobalBackpressure) {
               TPCMetrics.globallyBackpressuredCores(this.hasGlobalBackpressure?1:-1);
            }
         }

      }

      private boolean hasPendingTasks() {
         return this.pendingQueue.relaxedPeek() != null;
      }

      private boolean hasQueueTasks() {
         return this.queue.relaxedPeek() != null || this.priorityQueue.relaxedPeek() != null;
      }

      private static long nanoTimeSinceStartup() {
         return TPC.nanoTimeSinceStartup();
      }

      static {
         REMOTE_BACKPRESSURE_THRESHOLD = LOCAL_BACKPRESSURE_THRESHOLD * EpollTPCEventLoopGroup.REMOTE_BACKPRESSURE_MULTIPLIER;
         GLOBAL_BACKPRESSURE_THRESHOLD = LOCAL_BACKPRESSURE_THRESHOLD * EpollTPCEventLoopGroup.GLOBAL_BACKPRESSURE_MULTIPLIER;
      }
   }
}
