package org.apache.cassandra.cql3.functions;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.FBUtilities;

final class UDFExecutorService extends JMXEnabledThreadPoolExecutor {
   private static int KEEPALIVE = Integer.getInteger("cassandra.udf_executor_thread_keepalive_ms", 30000).intValue();

   UDFExecutorService(NamedThreadFactory threadFactory, String jmxPath) {
      super(FBUtilities.getAvailableProcessors(), (long)KEEPALIVE, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(), threadFactory, jmxPath);
   }

   protected void afterExecute(Runnable r, Throwable t) {
   }

   protected void beforeExecute(Thread t, Runnable r) {
   }
}
