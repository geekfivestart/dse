package org.apache.cassandra.service;

import io.reactivex.Completable;
import java.net.InetAddress;
import java.util.function.BiConsumer;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Response;

public abstract class WrappingWriteHandler extends WriteHandler {
   protected final WriteHandler wrapped;

   protected WrappingWriteHandler(WriteHandler wrapped) {
      this.wrapped = wrapped;
      wrapped.whenComplete((x, t) -> {
         if(logger.isTraceEnabled()) {
            logger.trace("{}/{} - Completed", Integer.valueOf(this.hashCode()), Integer.valueOf(wrapped.hashCode()));
         }

         if(t == null) {
            this.complete((Void)null);
         } else {
            this.completeExceptionally(t);
         }

      });
   }

   public WriteEndpoints endpoints() {
      return this.wrapped.endpoints();
   }

   public ConsistencyLevel consistencyLevel() {
      return this.wrapped.consistencyLevel();
   }

   public WriteType writeType() {
      return this.wrapped.writeType();
   }

   protected long queryStartNanos() {
      return this.wrapped.queryStartNanos();
   }

   public Void get() throws WriteTimeoutException, WriteFailureException {
      return this.wrapped.get();
   }

   public Completable toObservable() {
      return this.wrapped.toObservable();
   }

   public void onResponse(Response<EmptyPayload> response) {
      this.wrapped.onResponse(response);
   }

   public void onFailure(FailureResponse<EmptyPayload> response) {
      this.wrapped.onFailure(response);
   }

   public void onTimeout(InetAddress host) {
      this.wrapped.onTimeout(host);
   }

   public boolean complete(Void value) {
      if(!this.wrapped.isDone()) {
         this.wrapped.complete(value);
      }

      return super.complete(value);
   }

   public boolean completeExceptionally(Throwable ex) {
      if(!this.wrapped.isDone()) {
         this.wrapped.completeExceptionally(ex);
      }

      return super.completeExceptionally(ex);
   }
}
