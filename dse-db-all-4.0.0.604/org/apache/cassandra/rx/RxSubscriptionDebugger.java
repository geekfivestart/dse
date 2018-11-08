package org.apache.cassandra.rx;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RxSubscriptionDebugger {
   static final Logger logger = LoggerFactory.getLogger(RxSubscriptionDebugger.class);
   static final AtomicBoolean enabled = new AtomicBoolean();
   static final long timeoutInSeconds = 5L;
   static final long timeoutInNanos;
   private static final ConcurrentMap<Integer, Pair<Long, StackTraceElement[]>> observables;

   public RxSubscriptionDebugger() {
   }

   public static void enable() {
      if(enabled.compareAndSet(false, true)) {
         RxJavaPlugins.setOnObservableAssembly(RxSubscriptionDebugger::onCreate);
         RxJavaPlugins.setOnObservableSubscribe(RxSubscriptionDebugger::onSubscribe);
         RxJavaPlugins.setOnCompletableAssembly(RxSubscriptionDebugger::onCreate);
         RxJavaPlugins.setOnCompletableSubscribe(RxSubscriptionDebugger::onSubscribe);
         RxJavaPlugins.setOnSingleAssembly(RxSubscriptionDebugger::onCreate);
         RxJavaPlugins.setOnSingleSubscribe(RxSubscriptionDebugger::onSubscribe);
         RxJavaPlugins.setOnMaybeAssembly(RxSubscriptionDebugger::onCreate);
         RxJavaPlugins.setOnMaybeSubscribe(RxSubscriptionDebugger::onSubscribe);
         RxJavaPlugins.setOnFlowableAssembly(RxSubscriptionDebugger::onCreate);
         RxJavaPlugins.setOnFlowableSubscribe(RxSubscriptionDebugger::onSubscribe);
         startWatcher();
      }

   }

   public static void disable() {
      if(enabled.compareAndSet(true, false)) {
         RxJavaPlugins.setOnCompletableAssembly((Function)null);
         RxJavaPlugins.setOnSingleAssembly((Function)null);
         RxJavaPlugins.setOnMaybeAssembly((Function)null);
         RxJavaPlugins.setOnObservableAssembly((Function)null);
         RxJavaPlugins.setOnFlowableAssembly((Function)null);
         RxJavaPlugins.setOnCompletableSubscribe((BiFunction)null);
         RxJavaPlugins.setOnSingleSubscribe((BiFunction)null);
         RxJavaPlugins.setOnMaybeSubscribe((BiFunction)null);
         RxJavaPlugins.setOnObservableSubscribe((BiFunction)null);
         RxJavaPlugins.setOnFlowableSubscribe((BiFunction)null);
      }

   }

   static <T> T onCreate(T observable) {
      observables.putIfAbsent(Integer.valueOf(System.identityHashCode(observable)), Pair.create(Long.valueOf(System.nanoTime()), Thread.currentThread().getStackTrace()));
      return observable;
   }

   static <T, O> O onSubscribe(T observable, O observer) {
      observables.remove(Integer.valueOf(System.identityHashCode(observable)));
      return observer;
   }

   static void startWatcher() {
      Thread watcher = new Thread(() -> {
         while(enabled.get()) {
            long now = System.nanoTime();
            Iterator var2 = observables.entrySet().iterator();

            while(var2.hasNext()) {
               Entry<Integer, Pair<Long, StackTraceElement[]>> entry = (Entry)var2.next();
               if(now - ((Long)((Pair)entry.getValue()).left).longValue() > timeoutInNanos) {
                  logger.error("No Subscription after {} seconds: {}", Long.valueOf(5L), printStackTrace((StackTraceElement[])((Pair)entry.getValue()).right));
                  observables.remove(entry.getKey());
               }
            }

            Uninterruptibles.sleepUninterruptibly(5L, TimeUnit.SECONDS);
         }

      });
      watcher.setName("rx-debug-subscription-watcher");
      watcher.start();
   }

   static String printStackTrace(StackTraceElement[] elements) {
      StringBuilder sb = new StringBuilder();

      for(int i = 4; i < elements.length; ++i) {
         StackTraceElement s = elements[i];
         sb.append("\n\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
      }

      return sb.toString();
   }

   static {
      timeoutInNanos = TimeUnit.SECONDS.toNanos(5L);
      observables = Maps.newConcurrentMap();
   }
}
