package org.apache.cassandra.concurrent;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCThread extends FastThreadLocalThread {
   private final int coreId;

   private TPCThread(ThreadGroup group, Runnable target, int coreId) {
      super(group, target, "CoreThread-" + coreId);
      this.coreId = coreId;
   }

   public int coreId() {
      return this.coreId;
   }

   static Executor newTPCThreadFactory() {
      return new TPCThread.TPCThreadsCreator();
   }

   public TPCMetrics metrics() {
      return TPC.metrics(this.coreId);
   }

   public static class TPCThreadsCreator implements Executor {
      private final TPCThread.TPCThreadsCreator.Factory factory = new TPCThread.TPCThreadsCreator.Factory();
      private volatile TPCThread lastCreatedThread;

      public TPCThreadsCreator() {
      }

      public void execute(Runnable runnable) {
         this.factory.newThread(runnable).start();
      }

      public TPCThread lastCreatedThread() {
         return this.lastCreatedThread;
      }

      private class Factory extends DefaultThreadFactory {
         private final AtomicInteger coreIdGenerator;

         private Factory() {
            super(TPCThread.class, true, 10);
            this.coreIdGenerator = new AtomicInteger();
         }

         protected Thread newThread(Runnable r, String name) {
            TPCThreadsCreator.this.lastCreatedThread = new TPCThread(this.threadGroup, r, this.coreIdGenerator.getAndIncrement());
            return TPCThreadsCreator.this.lastCreatedThread;
         }
      }
   }
}
