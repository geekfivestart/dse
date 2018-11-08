package org.apache.cassandra.utils.flow;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;

public abstract class DeferredFlow<T> extends Flow<T> {
   public DeferredFlow() {
   }

   public abstract boolean onSource(Flow<T> var1);

   public abstract boolean hasSource();

   @VisibleForTesting
   static <T> DeferredFlow<T> createWithTimeout(long timeoutNanos, Supplier<Consumer<Flow<T>>> notification) {
      return create(System.nanoTime() + timeoutNanos, () -> {
         return TPC.bestTPCScheduler();
      }, () -> {
         return Flow.error(new TimeoutException());
      }, notification);
   }

   public static <T> DeferredFlow<T> create(long deadlineNanos, Supplier<StagedScheduler> schedulerSupplier, Supplier<Flow<T>> timeoutSupplier, Supplier<Consumer<Flow<T>>> notification) {
      return new DeferredFlowImpl(deadlineNanos, schedulerSupplier, timeoutSupplier, notification);
   }
}
