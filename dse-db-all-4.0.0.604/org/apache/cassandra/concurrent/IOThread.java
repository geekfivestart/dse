package org.apache.cassandra.concurrent;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;

class IOThread extends FastThreadLocalThread {
   private final int id;

   private IOThread(ThreadGroup group, Runnable target, int id) {
      super(group, target, "IOThread-" + id);
      this.id = id;
   }

   public int id() {
      return this.id;
   }

   static class Factory extends DefaultThreadFactory {
      private int id = 0;

      Factory() {
         super(IOThread.class, true, 10);
      }

      protected Thread newThread(Runnable r, String name) {
         return new IOThread(this.threadGroup, r, this.id++);
      }
   }
}
