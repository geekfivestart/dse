package org.apache.cassandra.net;

import com.google.common.base.Throwables;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.cassandra.db.monitoring.AbortedOperationException;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VerbHandlers {
   private static final Logger logger = LoggerFactory.getLogger(VerbHandlers.class);

   private VerbHandlers() {
   }

   private static <P, Q> FailureResponse<Q> handleFailure(Request<P, Q> request, Throwable t) {
      if(t.getCause() != null && (t instanceof CompletionException || t instanceof NullPointerException)) {
         t = t.getCause();
      }

      Throwables.propagateIfInstanceOf(t, AbortedOperationException.class);
      Throwables.propagateIfInstanceOf(t, DroppingResponseException.class);
      RequestFailureReason reason;
      if(t instanceof InternalRequestExecutionException) {
         InternalRequestExecutionException err = (InternalRequestExecutionException)t;
         reason = err.reason;
         request.verb().errorHandler().handleError(err);
      } else {
         reason = RequestFailureReason.UNKNOWN;
         JVMStabilityInspector.inspectThrowable(t);
         logger.error("Unexpected error during execution of request " + request, t);
      }

      return request.verb().isOneWay()?null:request.respondWithFailure(reason);
   }

   public interface SyncMonitoredRequestResponse<P extends Monitorable, Q> extends VerbHandlers.MonitoredRequestResponse<P, Q> {
      default CompletableFuture<Q> handle(InetAddress from, P message, Monitor monitor) {
         return CompletableFuture.completedFuture(this.handleSync(from, message, monitor));
      }

      Q handleSync(InetAddress var1, P var2, Monitor var3);
   }

   public interface MonitoredRequestResponse<P extends Monitorable, Q> extends VerbHandlers.RequestResponse<P, Q> {
      default CompletableFuture<Response<Q>> handleMayThrow(Request<P, Q> request) {
         Monitor monitor = Monitor.createAndStart((Monitorable)request.payload(), request.operationStartMillis(), request.timeoutMillis(), request.isLocal());
         return this.handle(request.from(), (Monitorable)request.payload(), monitor).thenApply((v) -> {
            monitor.complete();
            return request.respond(v);
         });
      }

      default CompletableFuture<Q> handle(InetAddress from, P message) {
         throw new UnsupportedOperationException();
      }

      CompletableFuture<Q> handle(InetAddress var1, P var2, Monitor var3);
   }

   public interface SyncAckedRequest<P> extends VerbHandlers.AckedRequest<P> {
      default CompletableFuture<?> handle2(InetAddress from, P message) {
         this.handleSync(from, message);
         return CompletableFuture.completedFuture((Object)null);
      }

      void handleSync(InetAddress var1, P var2);
   }

   public interface AckedRequest<P> extends VerbHandlers.RequestResponse<P, EmptyPayload> {
      default CompletableFuture<EmptyPayload> handle(InetAddress from, P message) {
         CompletableFuture<?> f = this.handle2(from, message);
         return f == null?CompletableFuture.completedFuture(EmptyPayload.instance):f.thenApply((x) -> {
            return EmptyPayload.instance;
         });
      }

      CompletableFuture<?> handle2(InetAddress var1, P var2);
   }

   public interface SyncRequestResponse<P, Q> extends VerbHandlers.RequestResponse<P, Q> {
      default CompletableFuture<Q> handle(InetAddress from, P message) {
         return CompletableFuture.completedFuture(this.handleSync(from, message));
      }

      Q handleSync(InetAddress var1, P var2);
   }

   public interface RequestResponse<P, Q> extends VerbHandler<P, Q> {
      default CompletableFuture<Response<Q>> handle(Request<P, Q> request) {
         try {
            return this.handleMayThrow(request).exceptionally((t) -> {
               return VerbHandlers.handleFailure(request, t);
            });
         } catch (Throwable var3) {
            return CompletableFuture.completedFuture(VerbHandlers.handleFailure(request, var3));
         }
      }

      default CompletableFuture<Response<Q>> handleMayThrow(Request<P, Q> request) {
         CompletableFuture var10000 = this.handle(request.from(), request.payload());
         request.getClass();
         return var10000.thenApply(request::respond);
      }

      CompletableFuture<Q> handle(InetAddress var1, P var2);
   }

   public interface OneWay<P> extends VerbHandler<P, NoResponse> {
      default CompletableFuture<Response<NoResponse>> handle(Request<P, NoResponse> request) {
         try {
            this.handle(request.from(), request.payload());
         } catch (Throwable var3) {
            VerbHandlers.handleFailure(request, var3);
         }

         return null;
      }

      void handle(InetAddress var1, P var2);
   }
}
