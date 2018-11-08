package org.apache.cassandra.utils.concurrent;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

public class SimpleCondition implements Condition {
   private static final AtomicReferenceFieldUpdater<SimpleCondition, WaitQueue> waitingUpdater = AtomicReferenceFieldUpdater.newUpdater(SimpleCondition.class, WaitQueue.class, "waiting");
   private volatile WaitQueue waiting;
   private volatile boolean signaled = false;

   public SimpleCondition() {
   }

   public void await() throws InterruptedException {
      if(!this.isSignaled()) {
         if(this.waiting == null) {
            waitingUpdater.compareAndSet(this, (Object)null, new WaitQueue());
         }

         WaitQueue.Signal s = this.waiting.register(Thread.currentThread());
         if(this.isSignaled()) {
            s.cancel();
         } else {
            s.await();
         }

         assert this.isSignaled();

      }
   }

   public boolean await(long time, TimeUnit unit) throws InterruptedException {
      if(this.isSignaled()) {
         return true;
      } else {
         long start = System.nanoTime();
         long until = start + unit.toNanos(time);
         if(this.waiting == null) {
            waitingUpdater.compareAndSet(this, (Object)null, new WaitQueue());
         }

         WaitQueue.Signal s = this.waiting.register(Thread.currentThread());
         if(this.isSignaled()) {
            s.cancel();
            return true;
         } else {
            return s.awaitUntil(until) || this.isSignaled();
         }
      }
   }

   public void signal() {
      throw new UnsupportedOperationException();
   }

   public boolean isSignaled() {
      return this.signaled;
   }

   public void signalAll() {
      this.signaled = true;
      if(this.waiting != null) {
         this.waiting.signalAll();
      }

   }

   public void awaitUninterruptibly() {
      throw new UnsupportedOperationException();
   }

   public long awaitNanos(long nanosTimeout) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   public boolean awaitUntil(Date deadline) throws InterruptedException {
      throw new UnsupportedOperationException();
   }
}
