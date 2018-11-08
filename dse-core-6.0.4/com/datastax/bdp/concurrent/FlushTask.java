package com.datastax.bdp.concurrent;

import com.datastax.bdp.system.TimeSource;

public class FlushTask extends BaseTask {
   private final boolean shutdown;

   public FlushTask(TimeSource timeSource, boolean shutdown) {
      super(timeSource);
      this.shutdown = shutdown;
   }

   public int run() {
      throw new FlushTask.FlushedQueueException(this);
   }

   public boolean isShutdown() {
      return this.shutdown;
   }

   static class FlushedQueueException extends RuntimeException {
      private final FlushTask task;

      public FlushedQueueException(FlushTask task) {
         this.task = task;
      }

      public FlushTask task() {
         return this.task;
      }
   }
}
