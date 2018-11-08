package org.apache.cassandra.concurrent;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

public class JMXEnabledThreadPoolExecutor extends DebuggableThreadPoolExecutor implements JMXEnabledThreadPoolExecutorMBean {
   private final String mbeanName;
   public final ThreadPoolMetrics metrics;

   public JMXEnabledThreadPoolExecutor(String threadPoolName) {
      this(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory(threadPoolName), "internal");
   }

   public JMXEnabledThreadPoolExecutor(String threadPoolName, String jmxPath) {
      this(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory(threadPoolName), jmxPath);
   }

   public JMXEnabledThreadPoolExecutor(String threadPoolName, int priority) {
      this(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory(threadPoolName, priority), "internal");
   }

   public JMXEnabledThreadPoolExecutor(NamedThreadFactory threadFactory, String jmxPath) {
      this(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), threadFactory, jmxPath);
   }

   public JMXEnabledThreadPoolExecutor(int corePoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory, String jmxPath) {
      this(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue, threadFactory, jmxPath);
   }

   public JMXEnabledThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory, String jmxPath) {
      super(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      super.prestartAllCoreThreads();
      this.metrics = new ThreadPoolMetrics(this, jmxPath, threadFactory.id);
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      this.mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + threadFactory.id;

      try {
         mbs.registerMBean(this, new ObjectName(this.mbeanName));
      } catch (Exception var11) {
         throw new RuntimeException(var11);
      }
   }

   public JMXEnabledThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory, String jmxPath, RejectedExecutionHandler rejectedExecutionHandler) {
      this(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue, threadFactory, jmxPath);
      this.setRejectedExecutionHandler(rejectedExecutionHandler);
   }

   public JMXEnabledThreadPoolExecutor(Stage stage) {
      this(stage.getJmxName(), stage.getJmxType());
   }

   private void unregisterMBean() {
      try {
         ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(this.mbeanName));
      } catch (Exception var2) {
         throw new RuntimeException(var2);
      }

      this.metrics.release();
   }

   public synchronized void shutdown() {
      if(!this.isShutdown()) {
         this.unregisterMBean();
      }

      super.shutdown();
   }

   public synchronized List<Runnable> shutdownNow() {
      if(!this.isShutdown()) {
         this.unregisterMBean();
      }

      return super.shutdownNow();
   }

   public int getTotalBlockedTasks() {
      return (int)this.metrics.totalBlocked.getCount();
   }

   public int getCurrentlyBlockedTasks() {
      return (int)this.metrics.currentBlocked.getCount();
   }

   public int getCoreThreads() {
      return this.getCorePoolSize();
   }

   public void setCoreThreads(int number) {
      this.setCorePoolSize(number);
   }

   public int getMaximumThreads() {
      return this.getMaximumPoolSize();
   }

   public void setMaximumThreads(int number) {
      this.setMaximumPoolSize(number);
   }

   protected void onInitialRejection(Runnable task) {
      this.metrics.totalBlocked.inc();
      this.metrics.currentBlocked.inc();
   }

   protected void onFinalAccept(Runnable task) {
      this.metrics.currentBlocked.dec();
   }

   protected void onFinalRejection(Runnable task) {
      this.metrics.currentBlocked.dec();
   }
}
