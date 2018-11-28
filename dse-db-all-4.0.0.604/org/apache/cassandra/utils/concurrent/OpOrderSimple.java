package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class OpOrderSimple implements OpOrder {
   private static final int FINISHED = -1;
   private volatile OpOrderSimple.Group current = new OpOrderSimple.Group();

   public OpOrderSimple() {
   }

   public OpOrderSimple.Group start() {
      OpOrderSimple.Group current;
      do {
         current = this.current;
      } while(!current.register());

      return current;
   }

   public OpOrderSimple.Barrier newBarrier() {
      return new OpOrderSimple.Barrier();
   }

   public OpOrderSimple.Group getCurrent() {
      return this.current;
   }

   public void awaitNewBarrier() {
      OpOrderSimple.Barrier barrier = this.newBarrier();
      barrier.issue();
      barrier.await();
   }

   public final class Barrier implements OpOrder.Barrier {
      private volatile OpOrderSimple.Group orderOnOrBefore;

      public Barrier() {
      }

      public boolean isAfter(OpOrder.Group group) {
         return this.orderOnOrBefore == null?true:this.orderOnOrBefore.id - ((OpOrderSimple.Group)group).id >= 0L;
      }

      public void issue() {
         if(this.orderOnOrBefore != null) {
            throw new IllegalStateException("Can only call issue() once on each Barrier");
         } else {
            OpOrderSimple var2 = OpOrderSimple.this;
            OpOrderSimple.Group current;
            synchronized(OpOrderSimple.this) {
               current = OpOrderSimple.this.current;
               this.orderOnOrBefore = current;
               OpOrderSimple.this.current = current.next = new OpOrderSimple.Group(current);
            }

            current.expire();
         }
      }

      public void markBlocking() {
         for(OpOrderSimple.Group current = this.orderOnOrBefore; current != null; current = current.prev) {
            current.markBlocking();
         }

      }

      public WaitQueue.Signal register() {
         return this.orderOnOrBefore.waiting.register();
      }

      public boolean allPriorOpsAreFinished() {
         OpOrderSimple.Group current = this.orderOnOrBefore;
         if(current == null) {
            throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
         } else {
            return current.next.prev == null;
         }
      }

      public void await() {
         while(!this.allPriorOpsAreFinished()) {
            WaitQueue.Signal signal = this.register();
            if(this.allPriorOpsAreFinished()) {
               signal.cancel();
               return;
            }

            signal.awaitUninterruptibly();
         }

         assert this.orderOnOrBefore.running == -1;

      }

      public OpOrderSimple.Group getSyncPoint() {
         return this.orderOnOrBefore;
      }
   }

   public static final class Group implements Comparable<OpOrderSimple.Group>, OpOrder.Group {
      volatile OpOrderSimple.Group prev;
      volatile OpOrderSimple.Group next;
      final long id;
      private volatile int running = 0;
      final CompletableFuture<Void> isBlockingSignal = new CompletableFuture();
      final WaitQueue waiting = new WaitQueue();
      static final AtomicIntegerFieldUpdater<OpOrderSimple.Group> runningUpdater = AtomicIntegerFieldUpdater.newUpdater(OpOrderSimple.Group.class, "running");
      private static final boolean ENABLE_DEBUGGING = false;
      CopyOnWriteArrayList<StackTraceElement[]> startTraces = null;
      CopyOnWriteArrayList<StackTraceElement[]> closeTraces = null;

      Group() {
         this.id = 0L;
      }

      Group(OpOrderSimple.Group prev) {
         this.id = prev.id + 1L;
         this.prev = prev;
      }

      void expire() {
         int current;
         do {
            current = this.running;
            if(current < 0) {
               throw new IllegalStateException();
            }
         } while(!runningUpdater.compareAndSet(this, current, -1 - current));

         if(current == 0) {
            this.unlink();
         }

      }

      boolean register() {
         int current;
         do {
            current = this.running;
            if(current < 0) {
               return false;
            }
         } while(!runningUpdater.compareAndSet(this, current, current + 1));

         return true;
      }

      public void close() {
         while(true) {
            int current = this.running;
            if(current < 0) {
               if(runningUpdater.compareAndSet(this, current, current + 1)) {
                  if(current + 1 == -1) {
                     this.unlink();
                  }

                  return;
               }
            } else if(runningUpdater.compareAndSet(this, current, current - 1)) {
               return;
            }
         }
      }

      private void unlink() {
         OpOrderSimple.Group start = this;

         while(true) {
            OpOrderSimple.Group end = start.prev;
            if(end == null) {
               for(end = this.next; end.running == -1; end = end.next) {
                  ;
               }

               while(start != end) {
                  OpOrderSimple.Group next = start.next;
                  next.prev = null;
                  start.waiting.signalAll();
                  start = next;
               }

               return;
            }

            if(end.running != -1) {
               return;
            }

            start = end;
         }
      }

      void markBlocking() {
         this.isBlockingSignal.complete(null);
      }

      public boolean isBlocking() {
         return this.isBlockingSignal.isDone();
      }

      public CompletableFuture<Void> whenBlocking() {
         return this.isBlockingSignal;
      }

      boolean isComplete() {
         return this.next.prev == null;
      }

      public int compareTo(OpOrderSimple.Group that) {
         long c = this.id - that.id;
         return c > 0L?1:(c < 0L?-1:0);
      }
   }
}
