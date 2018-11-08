package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.WaitQueue;

class MemtableCleanerThread<P extends MemtablePool> extends Thread {
   final P pool;
   final Runnable cleaner;
   final WaitQueue wait = new WaitQueue();

   MemtableCleanerThread(P pool, Runnable cleaner) {
      super(pool.getClass().getSimpleName() + "Cleaner");
      this.pool = pool;
      this.cleaner = cleaner;
      this.setDaemon(true);
   }

   boolean needsCleaning() {
      return this.pool.onHeap.needsCleaning() || this.pool.offHeap.needsCleaning();
   }

   void trigger() {
      this.wait.signal();
   }

   public void run() {
      while(true) {
         if(!this.needsCleaning()) {
            WaitQueue.Signal signal = this.wait.register(Thread.currentThread());
            if(!this.needsCleaning()) {
               signal.awaitUninterruptibly();
            } else {
               signal.cancel();
            }
         } else {
            this.cleaner.run();
         }
      }
   }
}
