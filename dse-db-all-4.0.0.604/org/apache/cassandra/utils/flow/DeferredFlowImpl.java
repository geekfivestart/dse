package org.apache.cassandra.utils.flow;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeferredFlowImpl<T> extends DeferredFlow<T> implements FlowSubscriptionRecipient {
   private static final Logger logger = LoggerFactory.getLogger(DeferredFlowImpl.class);
   private final AtomicReference<Flow<T>> source;
   private final long deadlineNanos;
   private final Supplier<Flow<T>> timeoutSupplier;
   private final Supplier<StagedScheduler> schedulerSupplier;
   private final Supplier<Consumer<Flow<T>>> notification;
   private volatile FlowSubscriber<T> subscriber;
   private volatile FlowSubscriptionRecipient subscriptionRecipient;
   private volatile FlowSubscription subscription;
   private volatile TPCTimeoutTask<DeferredFlow<T>> timeoutTask;
   private final AtomicBoolean subscribed = new AtomicBoolean(false);

   DeferredFlowImpl(long deadlineNanos, Supplier<StagedScheduler> schedulerSupplier, Supplier<Flow<T>> timeoutSupplier, Supplier<Consumer<Flow<T>>> notification) {
      assert schedulerSupplier != null;

      this.source = new AtomicReference((Object)null);
      this.deadlineNanos = deadlineNanos;
      this.timeoutSupplier = timeoutSupplier;
      this.schedulerSupplier = schedulerSupplier;
      this.notification = notification;
   }

   public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      assert this.subscriber == null : "Only one subscriber is supported";

      this.subscriptionRecipient = subscriptionRecipient;
      this.subscriber = subscriber;
      if(this.source.get() == null) {
         this.startTimeoutTask();
      }

      this.maybeSubscribe();
   }

   public void onSubscribe(FlowSubscription source) {
      this.subscription = source;
   }

   public boolean onSource(Flow<T> value) {
      if(this.source.compareAndSet(null, value)) {
         if(logger.isTraceEnabled()) {
            logger.trace("{} - got source", Integer.valueOf(this.hashCode()));
         }

         Consumer<Flow<T>> action = this.notification != null?(Consumer)this.notification.get():null;
         if(action != null) {
            try {
               action.accept(value);
            } catch (Throwable var4) {
               logger.warn("onSource notification error: " + var4.getMessage(), var4);
            }
         }

         this.maybeSubscribe();
         return true;
      } else {
         return false;
      }
   }

   private void startTimeoutTask() {
      assert this.timeoutTask == null : "timeout task already running!";

      long timeoutNanos = this.deadlineNanos - System.nanoTime();
      if(timeoutNanos <= 0L) {
         this.onSource((Flow)this.timeoutSupplier.get());
      } else {
         this.timeoutTask = new TPCTimeoutTask(this);
         this.timeoutTask.submit(new DeferredFlowImpl.TimeoutAction(this.timeoutSupplier, this.schedulerSupplier), timeoutNanos, TimeUnit.NANOSECONDS);
         if(this.hasSource()) {
            this.timeoutTask.dispose();
         }

      }
   }

   private void disposeTimeoutTask() {
      TPCTimeoutTask<DeferredFlow<T>> timeoutTask = this.timeoutTask;
      if(timeoutTask != null) {
         timeoutTask.dispose();
      }

   }

   public boolean hasSource() {
      return this.source.get() != null;
   }

   private void maybeSubscribe() {
      if(logger.isTraceEnabled()) {
         logger.trace("{} - maybeSubscribe {}/{}", new Object[]{Integer.valueOf(this.hashCode()), this.source, this.subscription});
      }

      if(this.subscriber != null && this.source.get() != null) {
         if(this.subscribed.compareAndSet(false, true)) {
            this.disposeTimeoutTask();
            ((Flow)this.source.get()).requestFirst(this.subscriber, this.subscriptionRecipient);
         }
      }
   }

   private static class TimeoutAction<T> implements Consumer<DeferredFlow<T>> {
      private final Supplier<Flow<T>> timeoutSupplier;
      private final Supplier<StagedScheduler> schedulerSupplier;

      public TimeoutAction(Supplier<Flow<T>> timeoutSupplier, Supplier<StagedScheduler> schedulerSupplier) {
         this.timeoutSupplier = timeoutSupplier;
         this.schedulerSupplier = schedulerSupplier;
      }

      public void accept(DeferredFlow<T> flow) {
         if(!flow.hasSource()) {
            flow.onSource(((Flow)this.timeoutSupplier.get()).lift(Threads.requestOn((StagedScheduler)this.schedulerSupplier.get(), TPCTaskType.READ_TIMEOUT)));
         }

      }
   }
}
