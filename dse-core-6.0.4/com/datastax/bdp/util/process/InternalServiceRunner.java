package com.datastax.bdp.util.process;

import com.datastax.bdp.system.TimeSource;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InternalServiceRunner<T> extends ServiceRunner<T> {
   private static final Logger logger = LoggerFactory.getLogger(InternalServiceRunner.class);
   private static final long WAIT_AFTER_FAILURE_MS;
   private volatile long scheduledStartTime = 0L;

   public InternalServiceRunner(TimeSource timeSource) {
      super(timeSource);
   }

   protected synchronized void waitUntilReady() throws InterruptedException {
      while(this.timeSource.currentTimeMillis() - this.scheduledStartTime < 0L && this.getState() == ServiceRunner.State.NOT_STARTED) {
         this.wait(this.scheduledStartTime - this.timeSource.currentTimeMillis());
      }

   }

   protected synchronized void interrupt() {
      logger.info("Interrupt {}", this.threadName());
      this.serviceThread.interrupt();
   }

   protected ServiceRunner.Action onError(Throwable error, ServiceRunner.State state) {
      logger.error(this.threadName() + " caused an exception in state " + state + ": ", error);
      this.scheduledStartTime = this.timeSource.currentTimeMillis() + WAIT_AFTER_FAILURE_MS;
      return !(error instanceof OutOfMemoryError) && !(error.getCause() instanceof OutOfMemoryError)?ServiceRunner.Action.RELOAD:ServiceRunner.Action.TERMINATE;
   }

   static {
      WAIT_AFTER_FAILURE_MS = TimeUnit.SECONDS.toMillis((long)Integer.getInteger("dse.service_runner.wait_after_failure_seconds", 10).intValue());
   }
}
