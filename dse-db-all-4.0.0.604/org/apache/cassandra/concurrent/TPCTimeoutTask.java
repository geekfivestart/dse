package org.apache.cassandra.concurrent;

import io.reactivex.disposables.Disposable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TPCTimeoutTask<T> implements Runnable {
   private final TPCTimer timer;
   private final AtomicReference<T> valueRef;
   private final AtomicReference<Consumer<T>> actionRef;
   private volatile Disposable disposable;

   public TPCTimeoutTask(T value) {
      this(TPC.bestTPCTimer(), value);
   }

   public TPCTimeoutTask(TPCTimer timer, T value) {
      if(timer != null && value != null) {
         this.timer = timer;
         this.valueRef = new AtomicReference(value);
         this.actionRef = new AtomicReference();
      } else {
         throw new IllegalArgumentException("Timer and value must be both non-null!");
      }
   }

   public void run() {
      T value = this.valueRef.get();
      if(value != null) {
         ((Consumer)this.actionRef.getAndSet((Object)null)).accept(value);
         this.valueRef.set((Object)null);
      }

   }

   public void submit(Consumer<T> action, long timeoutNanos, TimeUnit timeUnit) {
      if(this.actionRef.compareAndSet((Object)null, action)) {
         this.disposable = this.timer.onTimeout(this, timeoutNanos, timeUnit);
      } else {
         throw new IllegalStateException("Task was already submitted!");
      }
   }

   public void dispose() {
      if(this.disposable != null) {
         this.disposable.dispose();
         if(this.disposable.isDisposed()) {
            this.valueRef.set((Object)null);
            this.actionRef.set((Object)null);
         }
      }

   }

   public boolean isDisposed() {
      return this.disposable.isDisposed();
   }

   public T getValue() {
      return this.valueRef.get();
   }
}
