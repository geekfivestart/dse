package org.apache.cassandra.service;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingRangeCalculatorService {
   public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();
   private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);
   private final JMXEnabledThreadPoolExecutor executor;
   private AtomicInteger updateJobs;

   public PendingRangeCalculatorService() {
      this.executor = new JMXEnabledThreadPoolExecutor(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(1), new NamedThreadFactory("PendingRangeCalculator"), "internal");
      this.updateJobs = new AtomicInteger(0);
      this.executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
         public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            PendingRangeCalculatorService.instance.finishUpdate();
         }
      });
   }

   private void finishUpdate() {
      this.updateJobs.decrementAndGet();
   }

   public void update() {
      this.updateJobs.incrementAndGet();
      this.executor.submit(new PendingRangeCalculatorService.PendingRangeTask(null));
   }

   public void blockUntilFinished() {
      while(this.updateJobs.get() > 0) {
         try {
            Thread.sleep(100L);
         } catch (InterruptedException var2) {
            throw new RuntimeException(var2);
         }
      }

   }

   public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName) {
      StorageService.instance.getTokenMetadata().calculatePendingRanges(strategy, keyspaceName);
   }

   private static class PendingRangeTask implements Runnable {
      private PendingRangeTask() {
      }

      public void run() {
         try {
            long start = System.currentTimeMillis();
            List<String> keyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
            Iterator var4 = keyspaces.iterator();

            while(true) {
               if(!var4.hasNext()) {
                  if(PendingRangeCalculatorService.logger.isTraceEnabled()) {
                     PendingRangeCalculatorService.logger.trace("Finished PendingRangeTask for {} keyspaces in {}ms", Integer.valueOf(keyspaces.size()), Long.valueOf(System.currentTimeMillis() - start));
                  }
                  break;
               }

               String keyspaceName = (String)var4.next();
               PendingRangeCalculatorService.calculatePendingRanges(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName);
            }
         } finally {
            PendingRangeCalculatorService.instance.finishUpdate();
         }

      }
   }
}
