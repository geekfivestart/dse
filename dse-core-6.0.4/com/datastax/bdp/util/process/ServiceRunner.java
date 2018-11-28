package com.datastax.bdp.util.process;

import com.datastax.bdp.system.TimeSource;
import com.datastax.bdp.util.LoggingUtil;
import com.google.common.collect.Sets;
import java.nio.channels.ClosedByInterruptException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ServiceRunner<T> implements Runnable {
   private static Logger logger = LoggerFactory.getLogger(ServiceRunner.class);
   public static final int SIGINT_JVM_EXIT_CODE = 130;
   public static final int SIGTERM_JVM_EXIT_CODE = 143;
   private static final long STATE_CHANGE_TIMEOUT;
   private static final long SERVICE_THREAD_TERMINATION_TIMEOUT;
   private volatile boolean terminated = false;
   private ServiceRunner.State state;
   protected final AtomicReference<T> service;
   protected final Thread serviceThread;
   private final ExecutorService backgroundExecutor;
   protected final TimeSource timeSource;

   public ServiceRunner(TimeSource timeSource) {
      this.state = ServiceRunner.State.NOT_STARTED;
      this.service = new AtomicReference();
      this.serviceThread = new Thread(this, this.threadName());
      this.backgroundExecutor = Executors.newCachedThreadPool();
      this.timeSource = timeSource;
   }

   protected abstract void waitUntilReady() throws InterruptedException;

   protected abstract void interrupt();

   protected abstract T initService() throws Exception;

   protected abstract void runService(T var1) throws Exception;

   protected abstract void shutdownService(T var1) throws Exception;

   protected abstract ServiceRunner.Action onError(Throwable var1, ServiceRunner.State var2);

   protected abstract String threadName();

   protected final void throwIEIfInterrupted() throws InterruptedException {
      if(Thread.interrupted()) {
         throw new InterruptedException();
      }
   }

   public void run() {
      LoggingUtil.setService(this.threadName());

      while(!this.terminated) {
         try {
            this.setState(ServiceRunner.State.SHUTTING_DOWN, ServiceRunner.State.NOT_STARTED);
            this.waitUntilReady();
            if(this.setState(ServiceRunner.State.NOT_STARTED, ServiceRunner.State.STARTING)) {
               this.throwIEIfInterrupted();
               T s = this.initService();
               this.service.set(s);
               if(this.setState(ServiceRunner.State.STARTING, ServiceRunner.State.RUNNING)) {
                  this.throwIEIfInterrupted();
                  this.runService(s);
               }
            }
         } catch (InterruptedException | ClosedByInterruptException var6) {
            logger.info(this.threadName() + " got interrupted");
            this.setState(ServiceRunner.State.SHUTTING_DOWN);
         } catch (Throwable var7) {
            if(this.onError(var7, this.getState()) == ServiceRunner.Action.TERMINATE) {
               this.terminated = true;
            }

            this.setState(ServiceRunner.State.SHUTTING_DOWN);
         } finally {
            this.shutdownIfNeeded();
         }
      }

      this.setState(ServiceRunner.State.SHUTTING_DOWN, ServiceRunner.State.TERMINATED);
   }

   public T getService() {
      return this.service.get();
   }

   public synchronized ServiceRunner.State getState() {
      if(logger.isDebugEnabled()) {
         logger.debug("Service " + this.threadName() + " state is: " + this.state);
      }

      return this.state;
   }

   private synchronized void setState(ServiceRunner.State newState) {
      if(logger.isDebugEnabled()) {
         logger.debug("Service " + this.threadName() + " set state: " + this.state + " -> " + newState);
      }

      this.state = newState;
      this.notifyAll();
   }

   private synchronized boolean setState(ServiceRunner.State oldState, ServiceRunner.State newState) {
      if(this.state == oldState) {
         if(logger.isDebugEnabled()) {
            logger.debug("Service " + this.threadName() + " changing state: " + oldState + " -> " + newState);
         }

         this.setState(newState);
         return true;
      } else {
         logger.debug("Service " + this.threadName() + " FAILED changing state: " + oldState + " -> " + newState);
         return false;
      }
   }

   public void start() {
      this.serviceThread.start();
   }

   public void restart() {
      logger.debug("Restart called for " + this.threadName(), (new Exception()).fillInStackTrace());
      this.backgroundExecutor.submit(this::shutdownIfNeeded);
   }

   public void terminate() throws InterruptedException {
      logger.debug("Terminate called for " + this.threadName(), (new Exception()).fillInStackTrace());
      this.terminated = true;
      this.shutdownIfNeeded();
      this.serviceThread.join(SERVICE_THREAD_TERMINATION_TIMEOUT);
      this.setState(ServiceRunner.State.TERMINATED);
   }

   private synchronized void shutdownIfNeeded() {
      try {
         this.waitFor(Sets.newHashSet(new ServiceRunner.State[]{ServiceRunner.State.NOT_STARTED, ServiceRunner.State.RUNNING, ServiceRunner.State.SHUTTING_DOWN, ServiceRunner.State.TERMINATED}), STATE_CHANGE_TIMEOUT);
         if(this.setState(ServiceRunner.State.NOT_STARTED, ServiceRunner.State.SHUTTING_DOWN)) {
            this.interrupt();
         } else if(this.setState(ServiceRunner.State.STARTING, ServiceRunner.State.SHUTTING_DOWN)) {
            this.interrupt();
         } else if(this.setState(ServiceRunner.State.RUNNING, ServiceRunner.State.SHUTTING_DOWN)) {
            this.shutdownService(this.service.getAndSet(null));
         }
      } catch (Exception var2) {
         if(this.onError(var2, this.getState()) == ServiceRunner.Action.TERMINATE) {
            this.terminated = true;
         }
      }

   }

   public synchronized void waitFor(Set<ServiceRunner.State> states, long timeout) throws InterruptedException {
      long currentTime = this.timeSource.currentTimeMillis();
      long endTime = currentTime + timeout;

      while(!states.contains(this.getState()) && (currentTime = this.timeSource.currentTimeMillis()) < endTime) {
         this.wait(endTime - currentTime);
      }

      ServiceRunner.State state = this.getState();
      if(!states.contains(state)) {
         logger.error("{} timed out to wait for any of the following states: {}. Current state: {}", new Object[]{this.threadName(), states, state});
      }

   }

   static {
      STATE_CHANGE_TIMEOUT = TimeUnit.SECONDS.toMillis((long)Integer.getInteger("dse.service_runner.state_change_timeout_seconds", 10).intValue());
      SERVICE_THREAD_TERMINATION_TIMEOUT = TimeUnit.SECONDS.toMillis((long)Integer.getInteger("dse.service_runner.termination_timeout_seconds", 30).intValue());
   }

   public static enum Action {
      TERMINATE,
      RELOAD;

      private Action() {
      }
   }

   public static enum State {
      NOT_STARTED,
      STARTING,
      RUNNING,
      SHUTTING_DOWN,
      TERMINATED;

      private State() {
      }
   }
}
