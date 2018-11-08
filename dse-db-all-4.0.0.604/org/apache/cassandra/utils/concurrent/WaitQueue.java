package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.apache.cassandra.metrics.Timer;

public final class WaitQueue {
   private static final int CANCELLED = -1;
   private static final int SIGNALLED = 1;
   private static final int NOT_SET = 0;
   private static final AtomicIntegerFieldUpdater signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(WaitQueue.RegisteredSignal.class, "state");
   private final ConcurrentLinkedQueue<WaitQueue.RegisteredSignal> queue = new ConcurrentLinkedQueue();

   public WaitQueue() {
   }

   public WaitQueue.Signal register() {
      return this.register(Thread.currentThread());
   }

   public WaitQueue.Signal register(Thread caller) {
      WaitQueue.RegisteredSignal signal = new WaitQueue.RegisteredSignal(caller);
      this.queue.add(signal);
      return signal;
   }

   public WaitQueue.Signal register(Thread caller, Timer.Context context) {
      assert context != null;

      WaitQueue.RegisteredSignal signal = new WaitQueue.TimedSignal(caller, context);
      this.queue.add(signal);
      return signal;
   }

   public boolean signal() {
      if(!this.hasWaiters()) {
         return false;
      } else {
         WaitQueue.RegisteredSignal s;
         do {
            s = (WaitQueue.RegisteredSignal)this.queue.poll();
         } while(s != null && s.signal() == null);

         return s != null;
      }
   }

   public void signalAll() {
      if(this.hasWaiters()) {
         int i = 0;
         int s = 5;
         Thread randomThread = null;

         for(Iterator iter = this.queue.iterator(); iter.hasNext(); iter.remove()) {
            WaitQueue.RegisteredSignal signal = (WaitQueue.RegisteredSignal)iter.next();
            Thread signalled = signal.signal();
            if(signalled != null) {
               if(signalled == randomThread) {
                  break;
               }

               ++i;
               if(i == s) {
                  randomThread = signalled;
                  s <<= 1;
               }
            }
         }

      }
   }

   private void cleanUpCancelled() {
      Iterator iter = this.queue.iterator();

      while(iter.hasNext()) {
         WaitQueue.RegisteredSignal s = (WaitQueue.RegisteredSignal)iter.next();
         if(s.isCancelled()) {
            iter.remove();
         }
      }

   }

   public boolean hasWaiters() {
      return !this.queue.isEmpty();
   }

   public int getWaiting() {
      if(!this.hasWaiters()) {
         return 0;
      } else {
         Iterator<WaitQueue.RegisteredSignal> iter = this.queue.iterator();
         int count = 0;

         while(iter.hasNext()) {
            WaitQueue.Signal next = (WaitQueue.Signal)iter.next();
            if(!next.isCancelled()) {
               ++count;
            }
         }

         return count;
      }
   }

   public static WaitQueue.Signal any(WaitQueue.Signal... signals) {
      return new WaitQueue.AnySignal(signals);
   }

   public static WaitQueue.Signal all(WaitQueue.Signal... signals) {
      return new WaitQueue.AllSignal(signals);
   }

   public static void waitOnCondition(BooleanSupplier condition, WaitQueue queue) {
      while(!condition.getAsBoolean()) {
         WaitQueue.Signal s = queue.register();
         if(!condition.getAsBoolean()) {
            s.awaitUninterruptibly();
         } else {
            s.cancel();
         }
      }

   }

   private static class AllSignal extends WaitQueue.MultiSignal {
      protected AllSignal(WaitQueue.Signal... signals) {
         super(signals);
      }

      public boolean isSignalled() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            if(!signal.isSignalled()) {
               return false;
            }
         }

         return true;
      }

      public boolean isSet() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            if(!signal.isSet()) {
               return false;
            }
         }

         return true;
      }
   }

   private static class AnySignal extends WaitQueue.MultiSignal {
      protected AnySignal(WaitQueue.Signal... signals) {
         super(signals);
      }

      public boolean isSignalled() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            if(signal.isSignalled()) {
               return true;
            }
         }

         return false;
      }

      public boolean isSet() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            if(signal.isSet()) {
               return true;
            }
         }

         return false;
      }
   }

   private abstract static class MultiSignal extends WaitQueue.AbstractSignal {
      final WaitQueue.Signal[] signals;

      protected MultiSignal(WaitQueue.Signal[] signals) {
         this.signals = signals;
      }

      public boolean isCancelled() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            if(!signal.isCancelled()) {
               return false;
            }
         }

         return true;
      }

      public boolean checkAndClear() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            signal.checkAndClear();
         }

         return this.isSignalled();
      }

      public void cancel() {
         WaitQueue.Signal[] var1 = this.signals;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            WaitQueue.Signal signal = var1[var3];
            signal.cancel();
         }

      }
   }

   private final class TimedSignal extends WaitQueue.RegisteredSignal {
      private final Timer.Context context;

      private TimedSignal(Thread caller, Timer.Context context) {
         super(caller);
         this.context = context;
      }

      public boolean checkAndClear() {
         this.context.stop();
         return super.checkAndClear();
      }

      public void cancel() {
         if(!this.isCancelled()) {
            this.context.stop();
            super.cancel();
         }

      }
   }

   private class RegisteredSignal extends WaitQueue.AbstractSignal {
      private volatile Thread thread;
      volatile int state;

      RegisteredSignal(Thread thread) {
         this.thread = thread;
      }

      public boolean isSignalled() {
         return this.state == 1;
      }

      public boolean isCancelled() {
         return this.state == -1;
      }

      public boolean isSet() {
         return this.state != 0;
      }

      private Thread signal() {
         if(!this.isSet() && WaitQueue.signalledUpdater.compareAndSet(this, 0, 1)) {
            Thread thread = this.thread;
            LockSupport.unpark(thread);
            this.thread = null;
            return thread;
         } else {
            return null;
         }
      }

      public boolean checkAndClear() {
         if(!this.isSet() && WaitQueue.signalledUpdater.compareAndSet(this, 0, -1)) {
            this.thread = null;
            WaitQueue.this.cleanUpCancelled();
            return false;
         } else {
            return true;
         }
      }

      public void cancel() {
         if(!this.isCancelled()) {
            if(!WaitQueue.signalledUpdater.compareAndSet(this, 0, -1)) {
               this.state = -1;
               WaitQueue.this.signal();
            }

            this.thread = null;
            WaitQueue.this.cleanUpCancelled();
         }
      }
   }

   public abstract static class AbstractSignal implements WaitQueue.Signal {
      public AbstractSignal() {
      }

      public void awaitUninterruptibly() {
         boolean interrupted;
         for(interrupted = false; !this.isSignalled(); LockSupport.park()) {
            if(Thread.interrupted()) {
               interrupted = true;
            }
         }

         if(interrupted) {
            Thread.currentThread().interrupt();
         }

         this.checkAndClear();
      }

      public void await() throws InterruptedException {
         while(!this.isSignalled()) {
            this.checkInterrupted();
            LockSupport.park();
         }

         this.checkAndClear();
      }

      public boolean awaitUntil(long until) throws InterruptedException {
         long now;
         while(until > (now = System.nanoTime()) && !this.isSignalled()) {
            this.checkInterrupted();
            long delta = until - now;
            LockSupport.parkNanos(delta);
         }

         return this.checkAndClear();
      }

      private void checkInterrupted() throws InterruptedException {
         if(Thread.interrupted()) {
            this.cancel();
            throw new InterruptedException();
         }
      }
   }

   public interface Signal {
      boolean isSignalled();

      boolean isCancelled();

      boolean isSet();

      boolean checkAndClear();

      void cancel();

      void awaitUninterruptibly();

      void await() throws InterruptedException;

      boolean awaitUntil(long var1) throws InterruptedException;
   }
}
