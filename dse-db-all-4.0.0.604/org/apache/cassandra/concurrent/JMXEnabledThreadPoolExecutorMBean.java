package org.apache.cassandra.concurrent;

public interface JMXEnabledThreadPoolExecutorMBean {
   int getCoreThreads();

   void setCoreThreads(int var1);

   int getMaximumThreads();

   void setMaximumThreads(int var1);
}
