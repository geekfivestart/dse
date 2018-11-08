package org.apache.cassandra.concurrent;

public interface TPCLimitsMBean {
   int getMaxPendingRequests();

   void setMaxPendingRequests(int var1);

   int getMaxConcurrentRequests();

   void setMaxConcurrentRequests(int var1);
}
