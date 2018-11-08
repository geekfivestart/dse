package org.apache.cassandra.concurrent;

import io.netty.channel.epoll.Aio;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.AioCoordinator;
import org.apache.cassandra.metrics.TPCAggregatedStageMetrics;
import org.apache.cassandra.metrics.TPCTotalMetrics;
import org.apache.cassandra.rx.RxSubscriptionDebugger;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderThreaded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPC {
   private static final Logger logger = LoggerFactory.getLogger(TPC.class);
   private static final boolean LOG_CALLER_STACK_ON_EXCEPTION = System.getProperty("dse.tpc.log_caller_stack_on_exception", "false").equalsIgnoreCase("true");
   private static final boolean ENABLE_RX_SUBSCRIPTION_DEBUG = System.getProperty("dse.tpc.enable_rx_subscription_debug", "false").equalsIgnoreCase("true");
   private static final int NUM_CORES = DatabaseDescriptor.getTPCCores();
   private static final int TIMERS_RATIO = Integer.valueOf(System.getProperty("dse.tpc.timers_ratio", "5")).intValue();
   private static final int NIO_IO_RATIO = Integer.valueOf(System.getProperty("io.netty.ratioIO", "50")).intValue();
   public static final boolean USE_EPOLL = Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true")) && Epoll.isAvailable();
   public static final boolean USE_AIO;
   public static final int READ_ASYNC_TIMEOUT_MILLIS;
   public static final AioCoordinator aioCoordinator;
   private static final AtomicLong schedulerRoundRobinIndex;
   private static final AtomicLong[] timerRoundRobinIndex;
   private static final TPCEventLoopGroup eventLoopGroup;
   private static final TPCScheduler[] perCoreSchedulers;
   private static final ArrayList<TPCTimer> timers;
   private static final IOScheduler ioScheduler;
   private static final OpOrderThreaded.ThreadIdentifier threadIdentifier;
   public static final TPCMetrics[] perCoreMetrics;

   public TPC() {
   }

   private static void register(TPCEventLoop loop) {
      int coreId = loop.thread().coreId();

      assert coreId >= 0 && coreId < NUM_CORES;

      assert perCoreSchedulers[coreId] == null;

      TPCScheduler scheduler = new TPCScheduler(loop);
      perCoreSchedulers[coreId] = scheduler;
      if(timers.size() < getNumTimers()) {
         timers.add(new TPCHashedWheelTimer(scheduler));
      }

   }

   private static int getNumTimers() {
      return Math.max(1, getNumCores() / TIMERS_RATIO);
   }

   private static void initRx() {
      RxJavaPlugins.setComputationSchedulerHandler((s) -> {
         return bestTPCScheduler();
      });
      RxJavaPlugins.setIoSchedulerHandler((s) -> {
         return ioScheduler();
      });
      RxJavaPlugins.setErrorHandler((e) -> {
         CassandraDaemon.defaultExceptionHandler.accept(Thread.currentThread(), e);
      });
      RxJavaPlugins.setScheduleHandler((runnable) -> {
         Runnable runnablex = TPCRunnable.wrap(runnable);
         Runnable runnablexx = LOG_CALLER_STACK_ON_EXCEPTION?new TPC.RunnableWithCallerThreadInfo(runnablex):runnablex;
         return (Runnable)runnablexx;
      });
      if(ENABLE_RX_SUBSCRIPTION_DEBUG) {
         RxSubscriptionDebugger.enable();
      }

   }

   public static void ensureInitialized(boolean initStageManager) {
      if(DatabaseDescriptor.setTPCInitialized()) {
         eventLoopGroup.eventLoops().forEach((e) -> {
            e.start();
         });
      }

      if(initStageManager) {
         StageManager.initDummy();
      }

   }

   public static TPCEventLoopGroup eventLoopGroup() {
      return eventLoopGroup;
   }

   public static TPCScheduler currentThreadTPCScheduler() {
      int coreId = getCoreId();

      assert isValidCoreId(coreId) : "This method should not be called from a non-TPC thread.";

      return getForCore(coreId);
   }

   public static TPCScheduler bestTPCScheduler() {
      int coreId = getCoreId();
      return isValidCoreId(coreId)?getForCore(coreId):getForCore(getNextCore());
   }

   public static TPCTimer bestTPCTimer() {
      return (TPCTimer)timers.get((int)(timerRoundRobinIndex[getCoreId()].incrementAndGet() % (long)getNumTimers()));
   }

   public static TPCScheduler getNextTPCScheduler() {
      return getForCore(getNextCore());
   }

   public static int bestTPCCore() {
      int coreId = getCoreId();
      return isValidCoreId(coreId)?coreId:getNextCore();
   }

   public static TPCEventLoop bestIOEventLoop() {
      return (TPCEventLoop)getForCore(aioCoordinator.getIOCore(bestTPCCore())).eventLoop;
   }

   public static IOScheduler ioScheduler() {
      return ioScheduler;
   }

   public static OpOrder newOpOrder(Object creator) {
      return new OpOrderThreaded(creator, threadIdentifier, NUM_CORES + 1);
   }

   public static int getCoreId() {
      return getCoreId(Thread.currentThread());
   }

   public static boolean isOnCore(int coreId) {
      return getCoreId() == coreId;
   }

   public static boolean isOnIO() {
      return isIOThread(Thread.currentThread());
   }

   private static int getCoreId(Thread t) {
      return t instanceof TPCThread?((TPCThread)t).coreId():NUM_CORES;
   }

   private static boolean isTPCThread(Thread thread) {
      return thread instanceof TPCThread;
   }

   public static boolean isTPCThread() {
      return isTPCThread(Thread.currentThread());
   }

   private static boolean isIOThread(Thread thread) {
      return thread instanceof IOThread;
   }

   public static int getNumCores() {
      return NUM_CORES;
   }

   public static int getNextCore() {
      return (int)(schedulerRoundRobinIndex.getAndIncrement() % (long)getNumCores());
   }

   public static TPCScheduler getForCore(int core) {
      return perCoreSchedulers[core];
   }

   public static boolean isValidCoreId(int coreId) {
      return coreId >= 0 && coreId < getNumCores();
   }

   public static int getCoreForKey(Keyspace keyspace, DecoratedKey key) {
      return getCoreForKey(keyspace.getTPCBoundaries(), key);
   }

   public static int getCoreForKey(TPCBoundaries boundaries, DecoratedKey key) {
      if(boundaries == TPCBoundaries.NONE) {
         return 0;
      } else {
         Token keyToken = key.getToken();
         if(key.getPartitioner() != DatabaseDescriptor.getPartitioner()) {
            keyToken = DatabaseDescriptor.getPartitioner().getToken(key.getKey());
         }

         return boundaries.getCoreFor(keyToken);
      }
   }

   public static TPCScheduler getForKey(Keyspace keyspace, DecoratedKey key) {
      return getForCore(getCoreForKey(keyspace, key));
   }

   public static int getCoreForBound(Keyspace keyspace, PartitionPosition position) {
      return getCoreForBound(keyspace.getTPCBoundaries(), position);
   }

   public static int getCoreForBound(TPCBoundaries boundaries, PartitionPosition position) {
      return boundaries == TPCBoundaries.NONE?0:(position.getPartitioner() != DatabaseDescriptor.getPartitioner()?0:boundaries.getCoreFor(position.getToken()));
   }

   public static TPCScheduler getForBound(Keyspace keyspace, PartitionPosition position) {
      return getForCore(getCoreForBound(keyspace, position));
   }

   public static TPCMetrics metrics() {
      return perCoreMetrics[getCoreId()];
   }

   public static TPCMetrics metrics(int forCore) {
      return perCoreMetrics[forCore];
   }

   public static Executor getWrappedExecutor() {
      return (command) -> {
         bestTPCScheduler().getExecutor().execute(command);
      };
   }

   public static long nanoTimeSinceStartup() {
      return AbstractScheduledEventExecutor.nanoTime();
   }

   static {
      USE_AIO = Boolean.parseBoolean(System.getProperty("dse.io.aio.enabled", "true")) && Aio.isAvailable() && USE_EPOLL && (Boolean.parseBoolean(System.getProperty("dse.io.aio.force", "false")) || DatabaseDescriptor.assumeDataDirectoriesOnSSD());
      READ_ASYNC_TIMEOUT_MILLIS = Integer.valueOf(System.getProperty("dse.tpc.read_async_timeout_millis", "4000")).intValue();
      aioCoordinator = new AioCoordinator(NUM_CORES, USE_AIO?DatabaseDescriptor.getTPCIOCores():0, DatabaseDescriptor.getIOGlobalQueueDepth());
      schedulerRoundRobinIndex = new AtomicLong(0L);
      timerRoundRobinIndex = new AtomicLong[NUM_CORES + 1];
      perCoreSchedulers = new TPCScheduler[NUM_CORES];
      timers = new ArrayList(getNumTimers());
      ioScheduler = new IOScheduler();
      threadIdentifier = new OpOrderThreaded.ThreadIdentifier() {
         public int idFor(Thread t) {
            return TPC.getCoreId(t);
         }

         public boolean barrierPermitted() {
            return !TPC.isTPCThread();
         }
      };
      perCoreMetrics = new TPCMetrics[NUM_CORES + 1];

      int i;
      for(i = 0; i <= NUM_CORES; ++i) {
         perCoreMetrics[i] = new TPCMetricsAndLimits();
         timerRoundRobinIndex[i] = new AtomicLong();
      }

      if(USE_EPOLL) {
         eventLoopGroup = new EpollTPCEventLoopGroup(NUM_CORES);
         logger.info("Created {} epoll event loops.", Integer.valueOf(NUM_CORES));
      } else {
         NioTPCEventLoopGroup group = new NioTPCEventLoopGroup(NUM_CORES);
         group.setIoRatio(NIO_IO_RATIO);
         eventLoopGroup = group;
         logger.info("Created {} NIO event loops (with I/O ratio set to {}).", Integer.valueOf(NUM_CORES), Integer.valueOf(NIO_IO_RATIO));
      }

      eventLoopGroup.eventLoops().forEach(TPC::register);
      logger.info("Created {} TPC timers due to configured ratio of {}.", Integer.valueOf(timers.size()), Integer.valueOf(TIMERS_RATIO));
      initRx();

      for(i = 0; i < NUM_CORES; ++i) {
         new TPCTotalMetrics(perCoreMetrics[i], "internal", "TPC/" + i);
      }

      new TPCTotalMetrics(perCoreMetrics[NUM_CORES], "internal", "TPC/other");
      TPCTaskType[] var5 = TPCTaskType.values();
      int var1 = var5.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         TPCTaskType type = var5[var2];
         new TPCAggregatedStageMetrics(perCoreMetrics, type, "internal", "TPC/all");
      }

   }

   private abstract static class NettyTime extends AbstractScheduledEventExecutor {
      private NettyTime() {
      }

      public static long nanoSinceStartup() {
         return AbstractScheduledEventExecutor.nanoTime();
      }
   }

   private static final class RunnableWithCallerThreadInfo implements Runnable {
      private final Runnable runnable;
      private final FBUtilities.Debug.ThreadInfo threadInfo;

      RunnableWithCallerThreadInfo(Runnable runnable) {
         this.runnable = runnable;
         this.threadInfo = new FBUtilities.Debug.ThreadInfo();
      }

      public void run() {
         try {
            this.runnable.run();
         } catch (Throwable var2) {
            TPC.logger.error("Got exception {} with message <{}> when running Rx task. Caller's thread stack:\n{}", new Object[]{var2.getClass().getName(), var2.getMessage(), FBUtilities.Debug.getStackTrace(this.threadInfo)});
            throw var2;
         }
      }
   }
}
