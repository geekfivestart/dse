package org.apache.cassandra.batchlog;

public interface BatchlogManagerMBean {
   int countAllBatches();

   long getTotalBatchesReplayed();

   void forceBatchlogReplay() throws Exception;
}
