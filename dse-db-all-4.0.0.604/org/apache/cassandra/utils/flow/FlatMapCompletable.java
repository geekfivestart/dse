package org.apache.cassandra.utils.flow;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.internal.disposables.DisposableHelper;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

class FlatMapCompletable<I> extends Flow.RequestLoop implements FlowSubscriber<I>, Disposable {
   private final CompletableObserver observer;
   private final Function<? super I, ? extends CompletableSource> mapper;
   private FlowSubscription source;
   private final AtomicBoolean isDisposed;
   private volatile FlatMapCompletable<I>.FlatMapChild current;
   boolean completeOnNextRequest;

   public static <I> Completable flatMap(final Flow<I> source, final Function<? super I, ? extends CompletableSource> mapper) {
      class FlatMapCompletableCompletable extends Completable {
         FlatMapCompletableCompletable() {
         }

         protected void subscribeActual(CompletableObserver observer) {
            try {
               FlatMapCompletable<I> subscriber = new FlatMapCompletable(observer, mapper);
               observer.onSubscribe(subscriber);
               subscriber.requestFirst(source);
            } catch (Throwable var3) {
               JVMStabilityInspector.inspectThrowable(var3);
               observer.onError(var3);
            }

         }
      }

      return new FlatMapCompletableCompletable();
   }

   private FlatMapCompletable(CompletableObserver observer, Function<? super I, ? extends CompletableSource> mapper) throws Exception {
      this.completeOnNextRequest = false;
      this.observer = observer;
      this.mapper = mapper;
      this.isDisposed = new AtomicBoolean(false);
   }

   void requestFirst(Flow<I> source) {
      source.requestFirst(this, this);
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
   }

   public void requestNext() {
      if(!this.isDisposed() && !this.completeOnNextRequest) {
         this.requestInLoop(this.source);
      } else {
         this.close((Throwable)null);
      }

   }

   public void onFinal(I next) {
      this.completeOnNextRequest = true;
      this.onNext(next);
   }

   public void onNext(I next) {
      if(this.verify(this.current == null, (Throwable)null)) {
         try {
            CompletableSource child = (CompletableSource)this.mapper.apply(next);
            this.current = new FlatMapCompletable.FlatMapChild(child);
            if(this.isDisposed()) {
               this.current.dispose();
            } else {
               child.subscribe(this.current);
            }
         } catch (Throwable var3) {
            this.onError(var3);
         }

      }
   }

   public void onError(Throwable throwable) {
      this.onErrorInternal(Flow.wrapException(throwable, this));
   }

   private void onErrorInternal(Throwable throwable) {
      if(this.verify(this.current == null, throwable)) {
         this.close(throwable);
      }
   }

   public void onComplete() {
      if(this.verify(this.current == null, (Throwable)null)) {
         this.close((Throwable)null);
      }
   }

   private void close(Throwable t) {
      try {
         this.source.close();
      } catch (Throwable var3) {
         t = Throwables.merge(t, var3);
      }

      if(!this.isDisposed()) {
         if(t == null) {
            this.observer.onComplete();
         } else {
            this.observer.onError(t);
         }

      }
   }

   private boolean verify(boolean test, Throwable existingFail) {
      if(!test) {
         this.observer.onError(Throwables.merge(existingFail, new AssertionError("FlatMapCompletable unexpected state\n\t" + this)));
      }

      return test;
   }

   public String toString() {
      return Flow.formatTrace("flatMapCompletable", (Object)this.mapper);
   }

   public void dispose() {
      if(this.isDisposed.compareAndSet(false, true)) {
         FlatMapCompletable<I>.FlatMapChild current = this.current;
         if(current != null) {
            current.dispose();
         }
      }

   }

   public boolean isDisposed() {
      return this.isDisposed.get();
   }

   class FlatMapChild extends AtomicReference<Disposable> implements CompletableObserver, Disposable {
      final CompletableSource source;

      FlatMapChild(CompletableSource source) throws Exception {
         this.source = source;
      }

      public void dispose() {
         DisposableHelper.dispose(this);
      }

      public boolean isDisposed() {
         return DisposableHelper.isDisposed((Disposable)this.get());
      }

      public void onSubscribe(Disposable d) {
         DisposableHelper.replace(this, d);
      }

      public void onError(Throwable throwable) {
         if(FlatMapCompletable.this.verify(FlatMapCompletable.this.current == this, throwable)) {
            FlatMapCompletable.this.current = null;
            FlatMapCompletable.this.close(throwable);
         }
      }

      public void onComplete() {
         if(FlatMapCompletable.this.verify(FlatMapCompletable.this.current == this, (Throwable)null)) {
            FlatMapCompletable.this.current = null;
            FlatMapCompletable.this.requestNext();
         }
      }

      public String toString() {
         return FlatMapCompletable.this.toString();
      }
   }
}
