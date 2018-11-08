package org.apache.cassandra.utils.memory;

import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableAllocator {
   private final MemtableAllocator.SubAllocator onHeap;
   private final MemtableAllocator.SubAllocator offHeap;
   volatile MemtableAllocator.LifeCycle state;

   MemtableAllocator(MemtableAllocator.SubAllocator onHeap, MemtableAllocator.SubAllocator offHeap) {
      this.state = MemtableAllocator.LifeCycle.LIVE;
      this.onHeap = onHeap;
      this.offHeap = offHeap;
   }

   public abstract Row.Builder rowBuilder();

   public abstract DecoratedKey clone(DecoratedKey var1);

   public abstract boolean onHeapOnly();

   public MemtableAllocator.SubAllocator onHeap() {
      return this.onHeap;
   }

   public MemtableAllocator.SubAllocator offHeap() {
      return this.offHeap;
   }

   public <T> Single<T> whenBelowLimits(final Callable<Single<T>> singleCallable, final OpOrder.Group opGroup, final StagedScheduler observeOnScheduler, final TPCTaskType nextTaskType) {
      return new Single<T>() {
         protected void subscribeActual(final SingleObserver<? super T> subscriber) {
            class WhenBelowLimits implements Disposable {
               final AtomicReference<TPCRunnable> task = new AtomicReference((Object)null);
               final Timer.Context timerContext;

               WhenBelowLimits() {
                  CompletableFuture<Void> releaseFuture = MemtableAllocator.this.onHeap.parent.releaseFuture();
                  if(!opGroup.isBlocking() && (!MemtableAllocator.this.onHeap.parent.belowLimit() || !MemtableAllocator.this.offHeap.parent.belowLimit())) {
                     this.timerContext = MemtableAllocator.this.onHeap.parent.blockedTimerContext();
                     this.task.set(TPCRunnable.wrap(this::subscribeChild, ExecutorLocals.create(), TPCTaskType.WRITE_POST_MEMTABLE_FULL, observeOnScheduler));
                     opGroup.whenBlocking().thenRun(this::complete);
                     releaseFuture.thenRun(this::onRelease);
                  } else {
                     observeOnScheduler.execute(this::subscribeChild, nextTaskType);
                     this.timerContext = null;
                  }
               }

               void subscribeChild() {
                  Single child;
                  try {
                     child = (Single)singleCallable.call();
                  } catch (Throwable var3) {
                     subscriber.onError(var3);
                     return;
                  }

                  child.subscribe(subscriber);
               }

               public void dispose() {
                  TPCRunnable prev = (TPCRunnable)this.task.getAndSet((Object)null);
                  if(prev != null) {
                     this.timerContext.close();
                     prev.cancelled();
                  }

               }

               public boolean isDisposed() {
                  return this.task.get() == null;
               }

               public void complete() {
                  TPCRunnable prev = (TPCRunnable)this.task.getAndSet((Object)null);
                  if(prev != null) {
                     this.timerContext.close();

                     try {
                        observeOnScheduler.execute(prev);
                     } catch (RejectedExecutionException var3) {
                        prev.cancelled();
                        subscriber.onError(var3);
                     }
                  }

               }

               public void onRelease() {
                  if(this.task.get() != null) {
                     CompletableFuture<Void> releaseFuture = MemtableAllocator.this.onHeap.parent.releaseFuture();
                     if(MemtableAllocator.this.offHeap.parent.belowLimit() && MemtableAllocator.this.onHeap.parent.belowLimit()) {
                        this.complete();
                     } else {
                        releaseFuture.thenRun(this::onRelease);
                     }

                  }
               }
            }

            new WhenBelowLimits();
         }
      };
   }

   public void setDiscarding() {
      this.state = this.state.transition(MemtableAllocator.LifeCycle.DISCARDING);
      this.onHeap.markAllReclaiming();
      this.offHeap.markAllReclaiming();
   }

   public void setDiscarded() {
      this.state = this.state.transition(MemtableAllocator.LifeCycle.DISCARDED);
      this.onHeap.releaseAll();
      this.offHeap.releaseAll();
   }

   public boolean isLive() {
      return this.state == MemtableAllocator.LifeCycle.LIVE;
   }

   public static final class SubAllocator {
      private final MemtablePool.SubPool parent;
      private volatile long owns;
      private volatile long reclaiming;
      private static final AtomicLongFieldUpdater<MemtableAllocator.SubAllocator> ownsUpdater = AtomicLongFieldUpdater.newUpdater(MemtableAllocator.SubAllocator.class, "owns");
      private static final AtomicLongFieldUpdater<MemtableAllocator.SubAllocator> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(MemtableAllocator.SubAllocator.class, "reclaiming");

      SubAllocator(MemtablePool.SubPool parent) {
         this.parent = parent;
      }

      void releaseAll() {
         this.parent.released(ownsUpdater.getAndSet(this, 0L));
         this.parent.reclaimed(reclaimingUpdater.getAndSet(this, 0L));
      }

      public void adjust(long size) {
         if(size <= 0L) {
            this.released(-size);
         } else {
            this.allocated(size);
         }

      }

      public void allocated(long size) {
         this.parent.allocated(size);
         ownsUpdater.addAndGet(this, size);
      }

      void released(long size) {
         this.parent.released(size);
         ownsUpdater.addAndGet(this, -size);
      }

      void markAllReclaiming() {
         long cur;
         long prev;
         do {
            cur = this.owns;
            prev = this.reclaiming;
         } while(!reclaimingUpdater.compareAndSet(this, prev, cur));

         this.parent.reclaiming(cur - prev);
      }

      public long owns() {
         return this.owns;
      }

      public float ownershipRatio() {
         float r = (float)this.owns / (float)this.parent.limit;
         return Float.isNaN(r)?0.0F:r;
      }
   }

   static enum LifeCycle {
      LIVE,
      DISCARDING,
      DISCARDED;

      private LifeCycle() {
      }

      MemtableAllocator.LifeCycle transition(MemtableAllocator.LifeCycle targetState) {
         switch(null.$SwitchMap$org$apache$cassandra$utils$memory$MemtableAllocator$LifeCycle[targetState.ordinal()]) {
         case 1:
            assert this == LIVE;

            return DISCARDING;
         case 2:
            assert this == DISCARDING;

            return DISCARDED;
         default:
            throw new IllegalStateException();
         }
      }
   }
}
