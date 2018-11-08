package org.apache.cassandra.concurrent;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import java.util.EnumMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class StageManager {
   private static final EnumMap<Stage, TracingAwareExecutorService> stages = new EnumMap(Stage.class);
   private static final EnumMap<Stage, Scheduler> schedulers = new EnumMap(Stage.class);
   public static final long KEEPALIVE = 60L;
   public static final ThreadPoolExecutor tracingExecutor;

   public StageManager() {
   }

   public static void initDummy() {
   }

   private static ThreadPoolExecutor tracingExecutor() {
      RejectedExecutionHandler reh = (r, executor) -> {
         Tracing.instance.onDroppedTask();
      };
      return new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue(1000), new NamedThreadFactory("TracingStage"), reh);
   }

   private static JMXEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads) {
      return new JMXEnabledThreadPoolExecutor(numThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory(stage.getJmxName()), stage.getJmxType());
   }

   public static TracingAwareExecutorService getStage(Stage stage) {
      return (TracingAwareExecutorService)stages.get(stage);
   }

   public static Scheduler getScheduler(Stage stage) {
      return (Scheduler)schedulers.get(stage);
   }

   public static void shutdownNow() {
      Stage[] var0 = Stage.values();
      int var1 = var0.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         Stage stage = var0[var2];
         ((TracingAwareExecutorService)stages.get(stage)).shutdownNow();
      }

   }

   static {
      stages.put(Stage.REQUEST_RESPONSE, multiThreadedStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
      stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors()));
      stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors()));
      stages.put(Stage.BACKGROUND_IO, multiThreadedStage(Stage.BACKGROUND_IO, DatabaseDescriptor.getMaxBackgroundIOThreads()));
      stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP));
      stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
      stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION));
      stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC));
      stages.put(Stage.AUTHZ, new JMXEnabledThreadPoolExecutor(Stage.AUTHZ));
      stages.forEach((stage, executor) -> {
         Scheduler var10000 = (Scheduler)schedulers.put(stage, Schedulers.from(executor));
      });
      tracingExecutor = tracingExecutor();
   }
}
