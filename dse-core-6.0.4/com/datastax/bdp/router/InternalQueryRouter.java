package com.datastax.bdp.router;

import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.util.Addresses;
import com.google.common.base.Preconditions;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalQueryRouter {
   private static final Logger LOGGER = LoggerFactory.getLogger(InternalQueryRouter.class);
   private final InternodeClient client;

   public InternalQueryRouter(InternodeClient client) {
      this.client = client;
   }

   public Future<ResultMessage> executeRpcRemote(InetAddress nodeBroadcastAddress, String objectName, String methodName, ClientState clientState, List<ByteBuffer> values) {
      Preconditions.checkArgument(!Addresses.Internode.isLocalEndpoint(nodeBroadcastAddress), "Cannot execute on the local endpoint with query router");
      LOGGER.debug("Request to route RPC to {}", nodeBroadcastAddress);
      RoutedRpcRequest request = new RoutedRpcRequest(objectName, methodName, clientState, values);
      Future<RoutedQueryResponse> future = this.client.sendAsync(nodeBroadcastAddress, (MessageType)InternalQueryRouterProtocol.RPC_REQUEST_MESSAGE_TYPE, (Object)request);
      CompletionStage<RoutedQueryResponse> completionStage = (CompletionStage)future;
      return (Future)completionStage.thenApply((resp) -> {
         return resp.resultMessage;
      });
   }

   public Future<ResultMessage> executeQueryRemote(InetAddress nodeBroadcastAddress, String query, QueryState queryState, QueryOptions queryOptions, Map<String, ByteBuffer> payload) {
      Preconditions.checkArgument(!Addresses.Internode.isLocalEndpoint(nodeBroadcastAddress), "Cannot execute on the local endpoint with query router");
      LOGGER.debug("Request to route query to {}", nodeBroadcastAddress);
      RoutedQueryRequest request = new RoutedQueryRequest(query, queryState, queryOptions, payload);
      Future<RoutedQueryResponse> future = this.client.sendAsync(nodeBroadcastAddress, (MessageType)InternalQueryRouterProtocol.REQUEST_MESSAGE_TYPE, (Object)request);
      CompletionStage<RoutedQueryResponse> completionStage = (CompletionStage)future;
      return (Future)completionStage.thenApply((resp) -> {
         return resp.resultMessage;
      });
   }

   public static ResultMessage withQueryExceptionsHandled(Callable<ResultMessage> getCall) {
      try {
         return (ResultMessage)getCall.call();
      } catch (InterruptedException var2) {
         Thread.currentThread().interrupt();
         throw new RuntimeException("Query execution has been interrupted", var2);
      } catch (ExecutionException var3) {
         if(var3.getCause() instanceof RequestExecutionException) {
            throw (RequestExecutionException)var3.getCause();
         } else if(var3.getCause() instanceof RequestValidationException) {
            throw (RequestValidationException)var3.getCause();
         } else if(var3.getCause() instanceof RuntimeException) {
            throw (RuntimeException)var3.getCause();
         } else {
            throw new RuntimeException("Failed to execute query", var3);
         }
      } catch (Exception var4) {
         throw new RuntimeException("Failed to execute query", var4);
      }
   }
}
