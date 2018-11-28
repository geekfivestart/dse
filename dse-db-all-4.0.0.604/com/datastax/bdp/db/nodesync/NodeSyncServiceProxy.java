package com.datastax.bdp.db.nodesync;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeSyncServiceProxy implements NodeSyncServiceProxyMBean {
   private static final CassandraVersion MIN_VERSION = new CassandraVersion("4.0.0.603");
   public static final NodeSyncServiceProxy instance = new NodeSyncServiceProxy();
   private static final Logger logger = LoggerFactory.getLogger(NodeSyncServiceProxy.class);

   private NodeSyncServiceProxy() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName jmxName = new ObjectName(MBEAN_NAME);
         mbs.registerMBean(this, jmxName);
      } catch (InstanceAlreadyExistsException var3) {
         logger.error("Cannot register NodeSync service proxy through JMX as a prior instance already exists: this shouldn't happen and should be reported to support. It won't prevent NodeSync from running, but it will prevent controlling remote instances through JMX");
      } catch (Exception var4) {
         logger.error("Cannot register NodeSync service proxy through JMX due to unexpected error: this shouldn't happen and should be reported to support. It won't prevent NodeSync from running, but it will prevent controlling remote instances through JMX", var4);
      }

   }

   public static void init() {
   }

   @Override
   public void startUserValidation(InetAddress address, Map<String, String> options) {
      this.send(address, Verbs.NODESYNC.SUBMIT_VALIDATION, UserValidationOptions.fromMap(options), reason -> {
         switch (reason) {
            case NODESYNC_NOT_RUNNING: {
               return new NodeSyncService.NodeSyncNotRunningException("Cannot start user validation, NodeSync is not currently running.");
            }
         }
         return new MethodExecutionException((RequestFailureReason)reason);
      });
   }

   @Override
   public void cancelUserValidation(InetAddress address, String idStr) {
      UserValidationID id = UserValidationID.from(idStr);
      this.send(address, Verbs.NODESYNC.CANCEL_VALIDATION, id, reason -> {
         switch (reason) {
            case NODESYNC_NOT_RUNNING: {
               return new NodeSyncService.NodeSyncNotRunningException("Cannot cancel user validation, NodeSync is not currently running.");
            }
            case UNKNOWN_NODESYNC_USER_VALIDATION: {
               return new NodeSyncService.NotFoundValidationException(id);
            }
            case CANCELLED_NODESYNC_USER_VALIDATION: {
               return new NodeSyncService.CancelledValidationException(id);
            }
         }
         return new MethodExecutionException((RequestFailureReason)reason);
      });
   }

   @Override
   public void enableTracing(InetAddress address, Map<String, String> options) {
      this.send(address, Verbs.NODESYNC.ENABLE_TRACING, TracingOptions.fromMap(options), reason -> {
         switch (reason) {
            case NODESYNC_TRACING_ALREADY_ENABLED: {
               UUID sessionId = this.currentTracingSession(address);
               return new NodeSyncTracing.NodeSyncTracingAlreadyEnabledException(sessionId);
            }
         }
         return new MethodExecutionException((RequestFailureReason)reason);
      });
   }


   public void disableTracing(InetAddress address) {
      this.send(address, Verbs.NODESYNC.DISABLE_TRACING, EmptyPayload.instance, (x$0) -> {
         return new NodeSyncServiceProxy.MethodExecutionException(x$0);
      });
   }

   public UUID currentTracingSession(InetAddress address) {
      return (UUID)((Optional)this.send(address, Verbs.NODESYNC.TRACING_SESSION, EmptyPayload.instance, (x$0) -> {
         return new NodeSyncServiceProxy.MethodExecutionException(x$0);
      })).orElse((Object)null);
   }

   private <P, T> T send(InetAddress address, Verb<P, T> verb, P requestPayload, Function<RequestFailureReason, Exception> exceptionProvider) {
      checkSupportsProxying(address);
      NodeSyncServiceProxy.Callback<T> callback = new NodeSyncServiceProxy.Callback(exceptionProvider);
      MessagingService.instance().send((Request)verb.newRequest(address, requestPayload), callback);

      try {
         return callback.get();
      } catch (InterruptedException var8) {
         throw new RuntimeException(var8);
      } catch (ExecutionException var9) {
         Throwable cause = var9.getCause();
         if(cause instanceof RuntimeException) {
            throw (RuntimeException)cause;
         } else {
            throw new RuntimeException(var9);
         }
      }
   }

   private static void checkSupportsProxying(InetAddress address) {
      CassandraVersion version = SystemKeyspace.getReleaseVersion(address);
      if(version == null || version.compareTo(MIN_VERSION) < 0) {
         throw new NodeSyncServiceProxy.RemoteVersionException(version);
      }
   }

   public static final class RemoteVersionException extends NodeSyncServiceProxy.NodeSyncServiceProxyException {
      private RemoteVersionException(CassandraVersion version) {
         super("The remote node doesn't support proxying, expected DSE >= " + NodeSyncServiceProxy.MIN_VERSION + " but found " + version);
      }
   }

   public static final class MethodTimeoutException extends NodeSyncServiceProxy.NodeSyncServiceProxyException {
      private MethodTimeoutException() {
         super("Timeout while running the remote NodeSync method");
      }
   }

   public static final class MethodExecutionException extends NodeSyncServiceProxy.NodeSyncServiceProxyException {
      private MethodExecutionException(RequestFailureReason reason) {
         super("Failure while running the remote NodeSync method, reason: " + reason);
      }
   }

   private abstract static class NodeSyncServiceProxyException extends RuntimeException {
      private NodeSyncServiceProxyException(String message) {
         super(message);
      }
   }

   private static class Callback<T> extends CompletableFuture<T> implements MessageCallback<T> {
      private final Function<RequestFailureReason, Exception> exceptionProvider;

      public Callback(Function<RequestFailureReason, Exception> exceptionProvider) {
         this.exceptionProvider = exceptionProvider;
      }

      public void onResponse(Response<T> response) {
         this.complete(response.payload());
      }

      public void onFailure(FailureResponse<T> response) {
         this.completeExceptionally((Throwable)this.exceptionProvider.apply(response.reason()));
      }

      public void onTimeout(InetAddress host) {
         this.completeExceptionally(new NodeSyncServiceProxy.MethodTimeoutException());
      }
   }
}
