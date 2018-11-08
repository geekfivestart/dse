package org.apache.cassandra.concurrent;

public interface SchedulableMessage {
   TracingAwareExecutor getRequestExecutor();

   default TracingAwareExecutor getResponseExecutor() {
      return this.getRequestExecutor();
   }

   StagedScheduler getScheduler();
}
