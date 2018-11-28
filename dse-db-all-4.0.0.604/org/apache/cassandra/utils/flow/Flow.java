package org.apache.cassandra.utils.flow;

import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BooleanSupplier;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Flow<T> {
   private static final Logger logger = LoggerFactory.getLogger(Flow.class);
   public static final LineNumberInference LINE_NUMBERS = new LineNumberInference();
   private static final boolean DEBUG_ENABLED;
   private static final Flow.ConsumingOp<Object> NO_OP_CONSUMER;
   private static final Flow EMPTY;

   public Flow() {
   }

   public abstract void requestFirst(FlowSubscriber<T> var1, FlowSubscriptionRecipient var2);

   public void requestFirst(FlowSubscriber<T> subscriber) {
      this.requestFirst(subscriber, subscriber);
   }

   public String toString() {
      return formatTrace(this.getClass().getSimpleName());
   }

   public <O> Flow<O> map(Function<T, O> mapper) {
      return new Flow.Map(this, mapper);
   }

   public <O> Flow<O> skippingMap(Function<T, O> mapper) {
      return new Flow.SkippingMap(this, mapper);
   }

   public Flow<T> filter(Predicate<T> tester) {
      return new Flow.Filter(this, tester);
   }

   public <O> Flow<O> stoppingMap(Function<T, O> mapper) {
      return new Flow.StoppingMap(this, mapper);
   }

   public Flow<T> takeWhile(Predicate<T> tester) {
      return new Flow.TakeWhile(this, tester);
   }

   public Flow<T> takeUntil(BooleanSupplier tester) {
      return new Flow.TakeUntil(this, tester);
   }

   public Flow<T> doOnClose(final Action onClose) {
      class DoOnClose extends FlowSource<T> implements FlowSubscriptionRecipient {
         FlowSubscription source;

         DoOnClose() {
         }

         public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            Flow.this.requestFirst(subscriber, this);
         }

         public void onSubscribe(FlowSubscription source) {
            this.source = source;
         }

         public void requestNext() {
            this.source.requestNext();
         }

         public void close() throws Exception {
            try {
               this.source.close();
            } finally {
               onClose.run();
            }

         }

         public String toString() {
            return Flow.formatTrace(this.getClass().getSimpleName(), onClose, Flow.this);
         }
      }

      return new DoOnClose();
   }

   public Flow<T> takeUntilAndDoOnClose(BooleanSupplier tester, final Action onClose) {
      return new Flow.TakeUntil(this, tester) {
         public void close() throws Exception {
            try {
               if(this.source != null) {
                  this.source.close();
               }
            } finally {
               onClose.run();
            }

         }
      };
   }

   public Flow<T> doOnError(final Consumer<Throwable> onError) {
      class DoOnError extends FlowTransformNext<T, T> {
         protected DoOnError(Flow<T> source) {
            super(source);
         }

         public void onNext(T item) {
            this.subscriber.onNext(item);
         }

         public void onFinal(T item) {
            this.subscriber.onFinal(item);
         }

         public void onError(Throwable t) {
            try {
               onError.accept(t);
            } catch (Throwable var3) {
               t.addSuppressed(var3);
            }

            this.subscriber.onError(t);
         }

         public String toString() {
            return Flow.formatTrace(this.getClass().getSimpleName(), onError, this.sourceFlow);
         }
      }

      return new DoOnError(this);
   }

   public Flow<T> doOnComplete(final Runnable onComplete) {
      class DoOnComplete extends FlowTransformNext<T, T> {
         DoOnComplete(Flow<T> source) {
            super(source);
         }

         public void onNext(T item) {
            this.subscriber.onNext(item);
         }

         public void onFinal(T item) {
            this.subscriber.onFinal(item);
            onComplete.run();
         }

         public void onComplete() {
            this.subscriber.onComplete();
            onComplete.run();
         }

         public String toString() {
            return Flow.formatTrace(this.getClass().getSimpleName(), onComplete, this.sourceFlow);
         }
      }

      return new DoOnComplete(this);
   }

   public <O> Flow<O> group(GroupOp<T, O> op) {
      return GroupOp.group(this, op);
   }

   public <O> Flow<O> flatMap(Function<T, Flow<O>> op) {
      return FlatMap.flatMap(this, op);
   }

   public static <T> Flow<T> concat(Flow<Flow<T>> source) {
      return Concat.concat(source);
   }

   public static <T> Flow<T> concat(Iterable<Flow<T>> sources) {
      return Concat.concat(sources);
   }

   public static <O> Flow<O> concat(Flow... o) {
      return Concat.concat(o);
   }

   public Flow<T> concatWith(Callable<Flow<T>> supplier) {
      return Concat.concatWith(this, supplier);
   }

   public Completable flatMapCompletable(Function<? super T, ? extends CompletableSource> mapper) {
      return FlatMapCompletable.flatMap(this, mapper);
   }

   public Flow<T> onErrorResumeNext(final Function<Throwable, Flow<T>> nextSourceSupplier) {
      class OnErrorResumeNext extends FlowTransform<T, T> {
         OnErrorResumeNext(Flow<T> source) {
            super(source);
         }

         public void onNext(T item) {
            this.subscriber.onNext(item);
         }

         public void onFinal(T item) {
            this.subscriber.onFinal(item);
         }

         public String toString() {
            return formatTrace("onErrorResumeNext", nextSourceSupplier, this.sourceFlow);
         }

         public final void onError(Throwable t) {
            Throwable error = wrapException(t, this);
            FlowSubscription prevSubscription = this.source;

            try {
               this.sourceFlow = (Flow)nextSourceSupplier.apply(error);
               this.sourceFlow.requestFirst(this.subscriber, this);
            } catch (Exception var6) {
               this.subscriber.onError(Throwables.merge(t, var6));
               return;
            }

            try {
               prevSubscription.close();
            } catch (Throwable var5) {
               Flow.logger.debug("Failed to close previous subscription in onErrorResumeNext: {}/{}", var5.getClass(), var5.getMessage());
            }

         }
      }

      return new OnErrorResumeNext(this);
   }

   public Flow<T> mapError(final Function<Throwable, Throwable> mapper) {
      class MapError extends FlowTransformNext<T, T> {
         protected MapError(Flow<T> source) {
            super(source);
         }

         public void onNext(T item) {
            this.subscriber.onNext(item);
         }

         public void onFinal(T item) {
            this.subscriber.onFinal(item);
         }

         public void onError(Throwable t) {
            try {
               t = (Throwable)mapper.apply(t);
            } catch (Throwable var3) {
               var3.addSuppressed(t);
               t = var3;
            }

            this.subscriber.onError(t);
         }

         public String toString() {
            return Flow.formatTrace(this.getClass().getSimpleName(), mapper, this.sourceFlow);
         }
      }

      return new MapError(this);
   }

   public static <O> Flow<O> fromIterable(Iterable<O> o) {
      return new Flow.IteratorSubscription(o.iterator());
   }

   public static <O> Flow<O> fromFuture(final CompletableFuture<O> future) {
      return new Flow<O>() {
         public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            future.whenComplete((v, e) -> {
               if(e != null) {
                  subscriber.onError(e);
               } else {
                  subscriber.onFinal(v);
               }

            });
         }
      };
   }

   public static <O> Flow<O> fromSingle(final Single<O> single) {
      return new Flow<O>() {
         public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            single.doOnError(subscriber::onError).subscribe(subscriber::onFinal);
         }
      };
   }

   public static <O> Flow<O> fromCallable(final Callable<O> callable) {
      return new Flow<O>() {
         public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);

            try {
               O v = callable.call();
               subscriber.onFinal(v);
            } catch (Throwable var4) {
               subscriber.onError(var4);
            }

         }
      };
   }

   public <O> O reduceBlocking(final O seed, Flow.ReduceFunction<O, T> reducer) throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      class ReduceBlocking extends Flow.ReduceSubscriber<T, O> {
         Throwable error = null;

         ReduceBlocking(Flow<T> source, Flow.ReduceFunction<O, T> reducer) {
            super(seed, source, reducer);
         }

         public void onComplete() {
            latch.countDown();
         }

         public void onErrorInternal(Throwable t) {
            this.error = t;
            latch.countDown();
         }
      }

      ReduceBlocking s = new ReduceBlocking(this, reducer);
      s.start();
      Uninterruptibles.awaitUninterruptibly(latch);
      Throwable error = s.error;

      try {
         s.close();
      } catch (Throwable var7) {
         error = Throwables.merge(error, var7);
      }

      Throwables.maybeFail(error);
      return s.current;
   }

   public <O> CompletableFuture<O> reduceToFuture(O seed, Flow.ReduceFunction<O, T> reducer) {
      final Flow.Future<O> future = new Flow.Future();
      class ReduceToFuture extends Flow.ReduceSubscriber<T, O> {
         ReduceToFuture(O seed, Flow<T> source, Flow.ReduceFunction<O, T> reducer) {
            super(seed, source, reducer);
         }

         public void onNext(T item) {
            try {
               this.current = this.reducer.apply(this.current, item);
            } catch (Throwable var4) {
               this.onError(var4);
               return;
            }

            if(future.isCancelled()) {
               try {
                  this.close();
               } catch (Throwable var3) {
                  Flow.logger.error("Error closing flow after cancellation", var3);
               }

            } else {
               this.requestInLoop(this.source);
            }
         }

         public void onComplete() {
            try {
               this.close();
            } catch (Throwable var2) {
               future.completeExceptionallyInternal(var2);
               return;
            }

            future.completeInternal(this.current);
         }

         protected void onErrorInternal(Throwable t) {
            try {
               this.close();
            } catch (Throwable var3) {
               t.addSuppressed(var3);
            }

            future.completeExceptionallyInternal(t);
         }
      }

      ReduceToFuture s = new ReduceToFuture(seed, this, reducer);
      s.start();
      return future;
   }

   public Flow<T> last() {
      return this.reduce(null, (prev, next) -> {
         return next;
      });
   }

   public <O> Single<O> reduceToRxSingle(final O seed, final Flow.ReduceFunction<O, T> reducerToSingle) {
      class SingleFromFlow extends Single<O> {
         SingleFromFlow() {
         }

         protected void subscribeActual(final SingleObserver<? super O> observer) {
            class ReduceToSingle extends Flow.DisposableReduceSubscriber<T, O> {
               ReduceToSingle() {
                  super(seed, Flow.this, reducerToSingle);
               }

               public void signalSuccess(O value) {
                  observer.onSuccess(value);
               }

               public void signalError(Throwable t) {
                  observer.onError(t);
               }
            }

            ReduceToSingle s = new ReduceToSingle();
            observer.onSubscribe(s);
            s.start();
         }
      }

      return new SingleFromFlow();
   }

   public <O> Single<O> mapToRxSingle(Flow.RxSingleMapper<T, O> mapper) {
      return this.reduceToRxSingle(null, mapper);
   }

   public Single<T> mapToRxSingle() {
      return this.mapToRxSingle((x) -> {
         return x;
      });
   }

   public Completable processToRxCompletable(final Flow.ConsumingOp<T> consumer) {
      class CompletableFromFlow extends Completable {
         CompletableFromFlow() {
         }

         protected void subscribeActual(final CompletableObserver observer) {
            class CompletableFromFlowSubscriber extends Flow.DisposableReduceSubscriber<T, Void> {
               CompletableFromFlowSubscriber() {
                  super(null, Flow.this, consumer);
               }

               public void signalSuccess(Void value) {
                  observer.onComplete();
               }

               public void signalError(Throwable t) {
                  observer.onError(t);
               }
            }

            CompletableFromFlowSubscriber cs = new CompletableFromFlowSubscriber();
            observer.onSubscribe(cs);
            cs.start();
         }
      }

      return new CompletableFromFlow();
   }

   public Completable processToRxCompletable() {
      return this.processToRxCompletable((v) -> {
      });
   }

   public CompletableFuture<Void> processToFuture() {
      return this.reduceToFuture(null, (v, t) -> {
         return null;
      });
   }

   public <O> Flow<O> reduce(final O seed, final Flow.ReduceFunction<O, T> reducer) {
      class Reduce extends FlowTransform<T, O> {
         O current = seed;

         public Reduce(Flow<T> source) {
            super(source);
         }

         public void onNext(T item) {
            try {
               this.current = reducer.apply(this.current, item);
            } catch (Throwable var3) {
               this.onError(var3);
               return;
            }

            this.requestInLoop(this.source);
         }

         public void onFinal(T item) {
            try {
               this.current = reducer.apply(this.current, item);
            } catch (Throwable var3) {
               this.onError(var3);
               return;
            }

            this.onComplete();
         }

         public void onComplete() {
            this.subscriber.onFinal(this.current);
         }

         public String toString() {
            return formatTrace(this.getClass().getSimpleName(), reducer, this.sourceFlow);
         }
      }

      return new Reduce(this);
   }

   static <T> Flow.ConsumingOp<T> noOp() {
      return (ConsumingOp<T>)NO_OP_CONSUMER;
   }

   public Flow<Void> process(Flow.ConsumingOp<T> consumer) {
      return this.reduce(null, consumer);
   }

   public Flow<Void> process() {
      return this.process(noOp());
   }

   public <O> Flow<Void> flatProcess(Function<T, Flow<O>> mapper) {
      return this.flatMap(mapper).process();
   }

   public Flow<T> ifEmpty(T value) {
      return ifEmpty(this, value);
   }

   public static <T> Flow<T> ifEmpty(Flow<T> source, T value) {
      return new Flow.IfEmptyFlow(source, value);
   }

   public Flow<List<T>> toList() {
      return new Flow.ToList(this);
   }

   public T blockingSingle() {
      try {
         class BlockingSingle<T> implements Flow.ReduceFunction<T, T> {
            boolean called = false;

            BlockingSingle() {
            }

            public T apply(T prev, T item) {
               assert !this.called : "Call to blockingSingle with more than one element: " + item;

               this.called = true;
               return item;
            }
         }

         BlockingSingle<T> reducer = new BlockingSingle();
         T res = this.reduceBlocking(null, reducer);

         assert reducer.called : "Call to blockingSingle with empty flow.";

         return res;
      } catch (Exception var3) {
         throw com.google.common.base.Throwables.propagate(var3);
      }
   }

   public T blockingLast(T def) throws Exception {
      return this.reduceBlocking(def, (prev, item) -> {
         return item;
      });
   }

   public void executeBlocking() throws Exception {
      this.blockingLast(null);
   }

   public static <T> Flow<T> just(T value) {
      return new Flow.Just(value);
   }

   public static <T> Flow<T> empty() {
      return EMPTY;
   }

   public static <T> Flow<T> error(final Throwable t) {
      class ErrorFlow extends Flow<T> {
         ErrorFlow() {
         }

         public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            subscriber.onError(t);
         }
      }

      return new ErrorFlow();
   }

   public static <T> Flow<T> concat(final Completable completable, final Flow<T> source) {
      class CompletableFlow extends Flow<T> implements CompletableObserver {
         private FlowSubscriber<T> subscriber;
         private FlowSubscriptionRecipient subscriptionRecipient;

         CompletableFlow() {
         }

         public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscriber = subscriber;
            this.subscriptionRecipient = subscriptionRecipient;
            completable.subscribe(this);
         }

         public void onSubscribe(Disposable disposable) {
         }

         public void onComplete() {
            source.requestFirst(this.subscriber, this.subscriptionRecipient);
         }

         public void onError(Throwable t) {
            this.subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            this.subscriber.onError(t);
         }

         public String toString() {
            return formatTrace("concatCompletableFlow", completable, source);
         }
      }

      return new CompletableFlow();
   }

   public static <T> Flow<T> concat(Flow<T> source, final Completable completable) {
      return new FlowTransform<T, T>(source) {
         boolean completeOnNextRequest = false;

         public void requestNext() {
            if(this.completeOnNextRequest) {
               this.onComplete();
            } else {
               super.requestNext();
            }

         }

         public void onNext(T item) {
            this.subscriber.onNext(item);
         }

         public void onFinal(T item) {
            this.completeOnNextRequest = true;
            this.onNext(item);
         }

         public void onComplete() {
            completable.subscribe(() -> {
               this.subscriber.onComplete();
            }, (error) -> {
               this.onError(error);
            });
         }

         public String toString() {
            return formatTrace("concatFlowCompletable", completable, this.sourceFlow);
         }
      };
   }

   public static <I, O> Flow<O> merge(List<Flow<I>> flows, Comparator<? super I> comparator, Reducer<I, O> reducer) {
      return Merge.get(flows, comparator, reducer);
   }

   public static <I> Flow<List<I>> zipToList(final List<Flow<I>> flows) {
      return merge(flows, Comparator.comparing((c) -> {
         return Integer.valueOf(0);
      }), new Reducer<I, List<I>>() {
         List<I> list = new ArrayList(flows.size());

         public void reduce(int idx, I current) {
            this.list.add(current);
         }

         public List<I> getReduced() {
            return this.list;
         }
      });
   }

   public <O> Flow<O> lift(final Flow.Operator<T, O> operator) {
      return new Flow<O>() {
         public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            operator.requestFirst(Flow.this, subscriber, subscriptionRecipient);
         }
      };
   }

   public static <T, R> Flow<T> using(final Callable<R> resourceSupplier, final Function<R, Flow<T>> flowSupplier, final Consumer<R> resourceDisposer) {
      class Using extends Flow<T> implements FlowSubscription, FlowSubscriptionRecipient {
         Flow<T> sourceFlow;
         FlowSubscription source;
         R resource;

         Using() {
         }

         public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(this);

            try {
               this.resource = resourceSupplier.call();

               assert this.resource != null : "Null resource is not allowed; use defer.";

               this.sourceFlow = (Flow)flowSupplier.apply(this.resource);
            } catch (Throwable var4) {
               subscriber.onError(var4);
               return;
            }

            this.sourceFlow.requestFirst(subscriber, this);
         }

         public void onSubscribe(FlowSubscription source) {
            this.source = source;
         }

         public void requestNext() {
            this.source.requestNext();
         }

         public void close() throws Exception {
            if(this.resource != null) {
               try {
                  resourceDisposer.accept(this.resource);
               } finally {
                  if(this.source != null) {
                     this.source.close();
                  }

               }
            }

         }

         public String toString() {
            return Flow.formatTrace("using", flowSupplier, this.sourceFlow);
         }
      }

      return new Using();
   }

   public static <T> Flow<T> defer(final Callable<Flow<T>> flowSupplier) {
      class Defer extends Flow<T> {
         Flow<T> sourceFlow;

         Defer() {
         }

         public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            try {
               this.sourceFlow = (Flow)flowSupplier.call();
            } catch (Throwable var4) {
               subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
               subscriber.onError(var4);
               return;
            }

            this.sourceFlow.requestFirst(subscriber, subscriptionRecipient);
         }

         public String toString() {
            return Flow.formatTrace("defer", flowSupplier, this.sourceFlow);
         }
      }

      return new Defer();
   }

   public Flow<T> take(long count) {
      AtomicLong cc = new AtomicLong(count);
      return this.takeUntil(() -> {
         return cc.decrementAndGet() < 0L;
      });
   }

   public Flow<T> delayOnNext(final long sleepFor, final TimeUnit timeUnit, final TPCTaskType taskType) {
      return new FlowTransformNext<T, T>(this) {
         public void onNext(T item) {
            TPC.bestTPCScheduler().schedule(() -> {
               this.subscriber.onNext(item);
            }, taskType, sleepFor, timeUnit);
         }

         public void onFinal(T item) {
            TPC.bestTPCScheduler().schedule(() -> {
               this.subscriber.onFinal(item);
            }, taskType, sleepFor, timeUnit);
         }
      };
   }

   public long countBlocking() throws Exception {
      return ((AtomicLong)this.reduceBlocking(new AtomicLong(0L), (count, value) -> {
         count.incrementAndGet();
         return count;
      })).get();
   }

   public Flow<Flow<T>> skipEmpty() {
      return SkipEmpty.skipEmpty(this);
   }

   public <U> Flow<U> skipMapEmpty(Function<Flow<T>, U> mapper) {
      return SkipEmpty.skipMapEmpty(this, mapper);
   }

   public static <T> CloseableIterator<T> toIterator(Flow<T> source) throws Exception {
      return new Flow.ToIteratorSubscriber(source);
   }

   public Flow.Tee<T> tee(int count) {
      return new TeeImpl(this, count);
   }

   public Flow.Tee<T> tee() {
      return new TeeImpl(this, 2);
   }

   public static StackTraceElement[] maybeGetStackTrace() {
      return DEBUG_ENABLED?Thread.currentThread().getStackTrace():null;
   }

   public static Throwable wrapException(Throwable throwable, Object tag) {
      if(throwable instanceof Flow.NonWrappableException) {
         return throwable;
      } else {
         Throwable[] var2 = throwable.getSuppressed();
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Throwable t = var2[var4];
            if(t instanceof Flow.FlowException && ((Flow.FlowException)t).tag == tag) {
               return throwable;
            }
         }

         throwable.addSuppressed(new Flow.FlowException(tag));
         return throwable;
      }
   }

   public static String stackTraceString(StackTraceElement[] stackTrace) {
      return stackTrace == null?"":" created during\n\t   " + (String)Stream.of(stackTrace).skip(4L).map(Object::toString).collect(Collectors.joining("\n\tat ")) + "\n";
   }

   public static String withLineNumber(Object obj) {
      LineNumberInference.Descriptor lineNumber;
      if(obj == null) {
         lineNumber = LineNumberInference.UNKNOWN_SOURCE;
      } else {
         lineNumber = LINE_NUMBERS.getLine(obj.getClass());
      }

      return obj + "(" + lineNumber.source() + ":" + lineNumber.line() + ")";
   }

   public static String formatTrace(String prefix) {
      return String.format("\t%-20s", new Object[]{prefix});
   }

   public static String formatTrace(String prefix, Object tag) {
      return String.format("\t%-20s%s", new Object[]{prefix, withLineNumber(tag)});
   }

   public static String formatTrace(String prefix, Object tag, Flow<?> sourceFlow) {
      return String.format("%s\n\t%-20s%s", new Object[]{sourceFlow, prefix, withLineNumber(tag)});
   }

   public static String formatTrace(String prefix, Flow<?> sourceFlow) {
      return String.format("%s\n\t%-20s", new Object[]{sourceFlow, prefix});
   }

   static {
      LINE_NUMBERS.preloadLambdas();
      DEBUG_ENABLED = Boolean.getBoolean("dse.debug_flow");
      NO_OP_CONSUMER = (v) -> {
      };
      EMPTY = new Flow() {
         public void requestFirst(FlowSubscriber subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            subscriber.onComplete();
         }
      };
   }

   private static class FlowException extends RuntimeException {
      Object tag;

      private FlowException(Object tag) {
         super("Flow call chain:\n" + tag.toString());
         this.tag = tag;
      }
   }

   public interface NonWrappableException {
   }

   public interface Tee<T> {
      Flow<T> child(int var1);
   }

   static class ToIteratorSubscriber<T> implements CloseableIterator<T>, FlowSubscriber<T> {
      static final Object POISON_PILL = new Object();
      FlowSubscription source;
      BlockingQueue<Object> queue = new ArrayBlockingQueue(1);
      boolean completeOnNextRequest = false;
      Throwable error = null;
      Object next = null;

      public ToIteratorSubscriber(Flow<T> source) throws Exception {
         source.requestFirst(this);
         this.awaitReply();
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void close() {
         try {
            this.source.close();
         } catch (Exception var2) {
            throw com.google.common.base.Throwables.propagate(var2);
         }
      }

      public void onComplete() {
         Uninterruptibles.putUninterruptibly(this.queue, POISON_PILL);
      }

      public void onError(Throwable arg0) {
         this.error = Throwables.merge(this.error, arg0);
         Uninterruptibles.putUninterruptibly(this.queue, POISON_PILL);
      }

      public void onNext(T arg0) {
         Uninterruptibles.putUninterruptibly(this.queue, arg0);
      }

      public void onFinal(T arg0) {
         this.completeOnNextRequest = true;
         Uninterruptibles.putUninterruptibly(this.queue, arg0);
      }

      protected Object computeNext() {
         if(this.next != null) {
            return this.next;
         } else {
            assert this.queue.isEmpty();

            if(this.completeOnNextRequest) {
               return POISON_PILL;
            } else {
               this.source.requestNext();
               return this.awaitReply();
            }
         }
      }

      private Object awaitReply() {
         this.next = Uninterruptibles.takeUninterruptibly(this.queue);
         if(this.error != null) {
            throw com.google.common.base.Throwables.propagate(this.error);
         } else {
            return this.next;
         }
      }

      public boolean hasNext() {
         return this.computeNext() != POISON_PILL;
      }

      public String toString() {
         return Flow.formatTrace("toIterator");
      }

      public T next() {
         boolean has = this.hasNext();

         assert has;

         T toReturn = (T)this.next;
         this.next = null;
         return toReturn;
      }
   }

   public interface Operator<I, O> {
      void requestFirst(Flow<I> var1, FlowSubscriber<O> var2, FlowSubscriptionRecipient var3);
   }

   static class Just<T> extends Flow<T> {
      final T value;

      Just(T value) {
         this.value = value;
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
         subscriber.onFinal(this.value);
      }
   }

   static class ToList<T> extends FlowTransformNext<T, List<T>> {
      List<T> entries = new ArrayList();

      public ToList(Flow<T> source) {
         super(source);
      }

      public void onNext(T entry) {
         this.entries.add(entry);
         this.requestInLoop(this.source);
      }

      public void onFinal(T entry) {
         this.entries.add(entry);
         this.onComplete();
      }

      public void onComplete() {
         this.subscriber.onFinal(this.entries);
      }
   }

   static class IfEmptyFlow<T> extends FlowTransform<T, T> {
      final T value;
      boolean hadItem;
      boolean completed;

      IfEmptyFlow(Flow<T> source, T value) {
         super(source);
         this.value = value;
      }

      public void requestNext() {
         if(!this.completed) {
            this.source.requestNext();
         } else {
            this.subscriber.onComplete();
         }

      }

      public void close() throws Exception {
         this.source.close();
      }

      public void onNext(T item) {
         this.hadItem = true;
         this.subscriber.onNext(item);
      }

      public void onFinal(T item) {
         this.hadItem = true;
         this.subscriber.onFinal(item);
      }

      public void onComplete() {
         this.completed = true;
         if(this.hadItem) {
            this.subscriber.onComplete();
         } else {
            this.subscriber.onNext(this.value);
         }

      }

      public void onError(Throwable t) {
         this.subscriber.onError(t);
      }
   }

   public interface ConsumingOp<T> extends Flow.ReduceFunction<Void, T> {
      void accept(T var1) throws Exception;

      default Void apply(Void v, T item) throws Exception {
         this.accept(item);
         return v;
      }
   }

   public interface RxSingleMapper<I, O> extends Function<I, O>, Flow.ReduceFunction<O, I> {

      default O apply(final O prev, final I curr) throws Exception {
         assert prev == null;
         return (O)this.apply(curr);
      }
   }

   private abstract static class DisposableReduceSubscriber<T, O> extends Flow.ReduceSubscriber<T, O> implements Disposable {
      private volatile boolean isDisposed = false;
      private volatile int isClosed = 0;
      private static AtomicIntegerFieldUpdater<Flow.DisposableReduceSubscriber> isClosedUpdater = AtomicIntegerFieldUpdater.newUpdater(Flow.DisposableReduceSubscriber.class, "isClosed");

      DisposableReduceSubscriber(O seed, Flow<T> source, Flow.ReduceFunction<O, T> reducer) {
         super(seed, source, reducer);
      }

      public void requestNext() {
         if(!this.isDisposed()) {
            super.requestNext();
         } else {
            try {
               this.close();
            } catch (Throwable var2) {
               this.onError(var2);
            }
         }

      }

      public void dispose() {
         this.isDisposed = true;
      }

      public boolean isDisposed() {
         return this.isDisposed;
      }

      public void onComplete() {
         try {
            this.close();
         } catch (Throwable var2) {
            this.signalError(Flow.wrapException(var2, this));
            return;
         }

         this.signalSuccess(this.current);
      }

      public void onErrorInternal(Throwable t) {
         try {
            this.close();
         } catch (Throwable var3) {
            t = Throwables.merge(t, var3);
         }

         this.signalError(t);
      }

      public void close() throws Exception {
         if(isClosedUpdater.compareAndSet(this, 0, 1)) {
            assert this.isClosed == 1;

            super.close();
         }
      }

      abstract void signalError(Throwable var1);

      abstract void signalSuccess(O var1);
   }

   public static class Future<T> extends CompletableFuture<T> {
      public Future() {
      }

      public boolean complete(T t) {
         throw new UnsupportedOperationException();
      }

      public boolean completeExceptionally(Throwable throwable) {
         throw new UnsupportedOperationException();
      }

      private void completeInternal(T t) {
         super.complete(t);
      }

      private void completeExceptionallyInternal(Throwable throwable) {
         super.completeExceptionally(throwable);
      }
   }

   public interface ReduceFunction<ACC, I> extends BiFunction<ACC, I, ACC> {
   }

   private abstract static class ReduceSubscriber<T, O> extends Flow.RequestLoop implements FlowSubscriber<T>, AutoCloseable {
      final BiFunction<O, T, O> reducer;
      final Flow<T> sourceFlow;
      FlowSubscription source;
      O current;
      private final StackTraceElement[] stackTrace;

      ReduceSubscriber(O seed, Flow<T> source, BiFunction<O, T, O> reducer) {
         this.reducer = reducer;
         this.sourceFlow = source;
         this.current = seed;
         this.stackTrace = Flow.maybeGetStackTrace();
      }

      public void start() {
         this.sourceFlow.requestFirst(this);
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void onNext(T item) {
         try {
            this.current = this.reducer.apply(this.current, item);
         } catch (Throwable var3) {
            this.onError(var3);
            return;
         }

         this.requestNext();
      }

      public void onFinal(T item) {
         try {
            this.current = this.reducer.apply(this.current, item);
         } catch (Throwable var3) {
            this.onError(var3);
            return;
         }

         this.onComplete();
      }

      public void requestNext() {
         this.requestInLoop(this.source);
      }

      public void close() throws Exception {
         this.source.close();
      }

      public String toString() {
         return Flow.formatTrace(this.getClass().getSimpleName(), this.reducer, this.sourceFlow) + "\n" + Flow.stackTraceString(this.stackTrace);
      }

      public final void onError(Throwable t) {
         this.onErrorInternal(Flow.wrapException(t, this));
      }

      protected abstract void onErrorInternal(Throwable var1);
   }

   abstract static class DebugRequestLoopFlow<T> extends Flow.RequestLoopFlow<T> {
      DebugRequestLoopFlow() {
      }

      abstract FlowSubscriber<T> errorRecipient();

      public void requestInLoop(FlowSubscription source) {
         if(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.IN_LOOP_REQUESTED)) {
            if(this.verifyStateChange(Flow.RequestLoopState.OUT_OF_LOOP, Flow.RequestLoopState.IN_LOOP_REQUESTED)) {
               do {
                  this.verifyStateChange(Flow.RequestLoopState.IN_LOOP_REQUESTED, Flow.RequestLoopState.IN_LOOP_READY);
                  source.requestNext();
               } while(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.OUT_OF_LOOP));

            }
         }
      }

      private boolean verifyStateChange(Flow.RequestLoopState from, Flow.RequestLoopState to) {
         Flow.RequestLoopState prev = (Flow.RequestLoopState)stateUpdater.getAndSet(this, to);
         if(prev == from) {
            return true;
         } else {
            this.errorRecipient().onError(new AssertionError("Invalid state " + prev));
            return false;
         }
      }
   }

   public abstract static class RequestLoopFlow<T> extends Flow<T> {
      volatile Flow.RequestLoopState state;
      static final AtomicReferenceFieldUpdater<Flow.RequestLoopFlow, Flow.RequestLoopState> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(Flow.RequestLoopFlow.class, Flow.RequestLoopState.class, "state");

      public RequestLoopFlow() {
         this.state = Flow.RequestLoopState.OUT_OF_LOOP;
      }

      public void requestInLoop(FlowSubscription source) {
         if(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.IN_LOOP_REQUESTED)) {
            do {
               this.state = Flow.RequestLoopState.IN_LOOP_READY;
               source.requestNext();
            } while(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.OUT_OF_LOOP));

         }
      }
   }

   public static class RequestLoop {
      volatile Flow.RequestLoopState state;
      static final AtomicReferenceFieldUpdater<Flow.RequestLoop, Flow.RequestLoopState> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(Flow.RequestLoop.class, Flow.RequestLoopState.class, "state");

      public RequestLoop() {
         this.state = Flow.RequestLoopState.OUT_OF_LOOP;
      }

      public void requestInLoop(FlowSubscription source) {
         if(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.IN_LOOP_REQUESTED)) {
            do {
               this.state = Flow.RequestLoopState.IN_LOOP_READY;
               source.requestNext();
            } while(!stateUpdater.compareAndSet(this, Flow.RequestLoopState.IN_LOOP_READY, Flow.RequestLoopState.OUT_OF_LOOP));

         }
      }
   }

   static enum RequestLoopState {
      OUT_OF_LOOP,
      IN_LOOP_READY,
      IN_LOOP_REQUESTED;

      private RequestLoopState() {
      }
   }

   static class IteratorSubscription<O> extends FlowSource<O> {
      final Iterator<O> iter;

      IteratorSubscription(Iterator<O> iter) {
         this.iter = iter;
      }

      public void requestNext() {
         boolean hasNext = false;
         O next = null;

         try {
            if(this.iter.hasNext()) {
               hasNext = true;
               next = this.iter.next();
            }
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(!hasNext) {
            this.subscriber.onComplete();
         } else {
            this.subscriber.onNext(next);
         }

      }

      public void close() throws Exception {
         if(this.iter instanceof AutoCloseable) {
            ((AutoCloseable)this.iter).close();
         }

      }
   }

   static class TakeUntil<T> extends FlowSource<T> implements FlowSubscriptionRecipient {
      final Flow<T> sourceFlow;
      final BooleanSupplier tester;
      FlowSubscription source;

      TakeUntil(Flow<T> sourceFlow, BooleanSupplier tester) {
         this.sourceFlow = sourceFlow;
         this.tester = tester;
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         this.subscribe(subscriber, subscriptionRecipient);

         boolean stop;
         try {
            stop = this.tester.getAsBoolean();
         } catch (Throwable var5) {
            subscriber.onError(var5);
            return;
         }

         if(stop) {
            subscriber.onComplete();
         } else {
            this.sourceFlow.requestFirst(subscriber, this);
         }
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void requestNext() {
         assert this.source != null : "requestNext without onSubscribe";

         boolean stop;
         try {
            stop = this.tester.getAsBoolean();
         } catch (Throwable var3) {
            this.subscriber.onError(var3);
            return;
         }

         if(stop) {
            this.subscriber.onComplete();
         } else {
            this.source.requestNext();
         }

      }

      public void close() throws Exception {
         if(this.source != null) {
            this.source.close();
         }

      }

      public String toString() {
         return Flow.formatTrace(this.getClass().getSimpleName(), this.tester, this.sourceFlow);
      }
   }

   static class TakeWhile<I> extends FlowTransformNext<I, I> {
      final Predicate<I> tester;

      public TakeWhile(Flow<I> source, Predicate<I> tester) {
         super(source);
         this.tester = tester;
      }

      public void onNext(I next) {
         boolean pass;
         try {
            pass = this.tester.test(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(pass) {
            this.subscriber.onNext(next);
         } else {
            this.subscriber.onComplete();
         }

      }

      public void onFinal(I next) {
         boolean pass;
         try {
            pass = this.tester.test(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(pass) {
            this.subscriber.onFinal(next);
         } else {
            this.subscriber.onComplete();
         }

      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.tester, this.sourceFlow);
      }
   }

   static class StoppingMap<I, O> extends FlowTransformNext<I, O> {
      final Function<I, O> mapper;

      public StoppingMap(Flow<I> source, Function<I, O> mapper) {
         super(source);
         this.mapper = mapper;
      }

      public void onNext(I next) {
         O out;
         try {
            out = this.mapper.apply(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(out != null) {
            this.subscriber.onNext(out);
         } else {
            this.subscriber.onComplete();
         }

      }

      public void onFinal(I next) {
         O out;
         try {
            out = this.mapper.apply(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(out != null) {
            this.subscriber.onFinal(out);
         } else {
            this.subscriber.onComplete();
         }

      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.mapper, this.sourceFlow);
      }
   }

   public static class Filter<I> extends FlowTransformNext<I, I> {
      final Predicate<I> tester;

      public Filter(Flow<I> source, Predicate<I> tester) {
         super(source);
         this.tester = tester;
      }

      public void onNext(I next) {
         boolean pass;
         try {
            pass = this.test(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(pass) {
            this.subscriber.onNext(next);
         } else {
            this.requestInLoop(this.source);
         }

      }

      public void onFinal(I next) {
         boolean pass;
         try {
            pass = this.test(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(pass) {
            this.subscriber.onFinal(next);
         } else {
            this.subscriber.onComplete();
         }

      }

      public boolean test(I next) throws Exception {
         return this.tester.test(next);
      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.tester, this.sourceFlow);
      }
   }

   public static class SkippingMap<I, O> extends FlowTransformNext<I, O> {
      final Function<I, O> mapper;

      public SkippingMap(Flow<I> source, Function<I, O> mapper) {
         super(source);
         this.mapper = mapper;
      }

      public void onNext(I next) {
         O out;
         try {
            out = this.map(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(out != null) {
            this.subscriber.onNext(out);
         } else {
            this.requestInLoop(this.source);
         }

      }

      public void onFinal(I next) {
         O out;
         try {
            out = this.map(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         if(out != null) {
            this.subscriber.onFinal(out);
         } else {
            this.subscriber.onComplete();
         }

      }

      public O map(I item) throws Exception {
         return this.mapper.apply(item);
      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.mapper, this.sourceFlow);
      }
   }

   public static class Map<I, O> extends FlowTransformNext<I, O> {
      final Function<I, O> mapper;

      public Map(Flow<I> source, Function<I, O> mapper) {
         super(source);
         this.mapper = mapper;
      }

      public void onNext(I next) {
         O out;
         try {
            out = this.map(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         this.subscriber.onNext(out);
      }

      public void onFinal(I next) {
         O out;
         try {
            out = this.map(next);
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         this.subscriber.onFinal(out);
      }

      public O map(I item) throws Exception {
         return this.mapper.apply(item);
      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.mapper, this.sourceFlow);
      }
   }
}
