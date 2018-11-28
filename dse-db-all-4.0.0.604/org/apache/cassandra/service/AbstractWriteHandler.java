package org.apache.cassandra.service;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.internal.disposables.EmptyDisposable;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.concurrent.TPCTimer;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;

abstract class AbstractWriteHandler extends WriteHandler {
   private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
   protected final WriteEndpoints endpoints;
   protected final ConsistencyLevel consistency;
   protected final WriteType writeType;
   private final long queryStartNanos;
   private final TPCTimer requestExpirer;
   protected final int blockFor;
   private static final AtomicIntegerFieldUpdater<AbstractWriteHandler> FAILURES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteHandler.class, "failures");
   private volatile int failures = 0;
   private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap();

   AbstractWriteHandler(WriteEndpoints endpoints, ConsistencyLevel consistency, int blockFor, WriteType writeType, long queryStartNanos, TPCTimer requestExpirer) {
      this.endpoints = endpoints;
      this.consistency = consistency;
      this.writeType = writeType;
      this.queryStartNanos = queryStartNanos;
      this.requestExpirer = requestExpirer;
      this.blockFor = blockFor < 0?consistency.blockFor(endpoints.keyspace()) + this.pendingToBlockFor():blockFor;
   }

   public WriteEndpoints endpoints() {
      return this.endpoints;
   }

   public ConsistencyLevel consistencyLevel() {
      return this.consistency;
   }

   public WriteType writeType() {
      return this.writeType;
   }

   protected long queryStartNanos() {
      return this.queryStartNanos;
   }

   public Void get() throws WriteTimeoutException, WriteFailureException {
      long timeout = this.currentTimeout();

      try {
         return (Void)super.get(timeout, TimeUnit.NANOSECONDS);
      } catch (InterruptedException var5) {
         throw new AssertionError(var5);
      } catch (TimeoutException var6) {
         int acks = this.ackCount();
         if(acks >= this.blockFor) {
            acks = this.blockFor - 1;
         }

         throw new WriteTimeoutException(this.writeType, this.consistency, acks, this.blockFor);
      } catch (ExecutionException var7) {
         assert var7.getCause() instanceof WriteFailureException;

         throw (WriteFailureException)var7.getCause();
      }
   }

   public Completable toObservable() {
      return new Completable() {
         protected void subscribeActual(CompletableObserver subscriber) {
            subscriber.onSubscribe(EmptyDisposable.INSTANCE);
            TPCTimeoutTask<WriteHandler> timeoutTask = new TPCTimeoutTask(AbstractWriteHandler.this.requestExpirer, AbstractWriteHandler.this);
            timeoutTask.submit(new AbstractWriteHandler.TimeoutAction(), AbstractWriteHandler.this.currentTimeout(), TimeUnit.NANOSECONDS);
            AbstractWriteHandler.this.whenComplete((result, error) -> {
               if(WriteHandler.logger.isTraceEnabled()) {
                  WriteHandler.logger.trace("{} - Completed with {}/{}", new Object[]{Integer.valueOf(AbstractWriteHandler.this.hashCode()), result, error == null?null:error.getClass().getName()});
               }

               timeoutTask.dispose();
               if(error != null) {
                  if(WriteHandler.logger.isTraceEnabled()) {
                     WriteHandler.logger.trace("{} - Returning error {}", Integer.valueOf(AbstractWriteHandler.this.hashCode()), error.getClass().getName());
                  }

                  if(error instanceof TimeoutException) {
                     int acks = AbstractWriteHandler.this.ackCount();
                     if(acks >= AbstractWriteHandler.this.blockFor) {
                        acks = AbstractWriteHandler.this.blockFor - 1;
                     }

                     subscriber.onError(new WriteTimeoutException(AbstractWriteHandler.this.writeType, AbstractWriteHandler.this.consistency, acks, AbstractWriteHandler.this.blockFor));
                  } else {
                     subscriber.onError(error);
                  }
               } else {
                  subscriber.onComplete();
               }

            });
         }
      };
   }

   protected int pendingToBlockFor() {
      return this.endpoints.pendingCount();
   }

   protected abstract int ackCount();

   protected boolean waitingFor(InetAddress from) {
      return true;
   }

   public void onFailure(FailureResponse<EmptyPayload> response) {
      InetAddress from = response.from();
      if(logger.isTraceEnabled()) {
         logger.trace("{} - Got failure from {}: {}", new Object[]{Integer.valueOf(this.hashCode()), from, response});
      }

      int n = this.waitingFor(from)?FAILURES_UPDATER.incrementAndGet(this):this.failures;
      this.failureReasonByEndpoint.put(from, response.reason());
      if(this.blockFor + n > this.endpoints.liveCount()) {
         this.completeExceptionally(new WriteFailureException(this.consistency, this.ackCount(), this.blockFor, this.writeType, this.failureReasonByEndpoint));
      }

   }

   public void onTimeout(InetAddress host) {
   }

   private static class TimeoutAction implements Consumer<WriteHandler> {
      private TimeoutAction() {
      }

      public void accept(WriteHandler handler) {
         handler.completeExceptionally(AbstractWriteHandler.TIMEOUT_EXCEPTION);
      }
   }
}
