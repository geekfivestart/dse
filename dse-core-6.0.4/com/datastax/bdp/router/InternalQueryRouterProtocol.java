package com.datastax.bdp.router;

import com.datastax.bdp.node.transport.Message;
import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.node.transport.RequestContext;
import com.datastax.bdp.node.transport.ServerProcessor;
import com.datastax.bdp.node.transport.internode.InternodeProtocol;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.util.rpc.RpcMethod;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.inject.Singleton;
import java.util.Optional;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InternalQueryRouterProtocol implements InternodeProtocol {
   private static final Logger LOGGER = LoggerFactory.getLogger(InternalQueryRouterProtocol.class);
   public static final MessageType REQUEST_MESSAGE_TYPE;
   public static final MessageType RESPONSE_MESSAGE_TYPE;
   public static final MessageType RPC_REQUEST_MESSAGE_TYPE;
   private final QueryHandler queryHandler = ClientState.getCQLQueryHandler();
   private final InternalQueryRouterProtocol.RoutingServerProcessor queryProcessor = new InternalQueryRouterProtocol.RoutingServerProcessor();
   private final InternalQueryRouterProtocol.RpcRoutingServerProcessor rpcProcessor = new InternalQueryRouterProtocol.RpcRoutingServerProcessor();

   public InternalQueryRouterProtocol() {
   }

   public void register(InternodeProtocolRegistry registry) {
      registry.addSerializer(REQUEST_MESSAGE_TYPE, RoutedQueryRequest.SERIALIZER, new byte[]{1});
      registry.addSerializer(RESPONSE_MESSAGE_TYPE, RoutedQueryResponse.SERIALIZER, new byte[]{1});
      registry.addSerializer(RPC_REQUEST_MESSAGE_TYPE, RoutedRpcRequest.SERIALIZER, new byte[]{1});
      registry.addProcessor(REQUEST_MESSAGE_TYPE, this.queryProcessor);
      registry.addProcessor(RPC_REQUEST_MESSAGE_TYPE, this.rpcProcessor);
      LOGGER.info("Registered InternalQueryRouterProtocol");
   }

   static {
      REQUEST_MESSAGE_TYPE = MessageType.of(MessageType.Domain.GENERIC_QUERY_ROUTER, 1);
      RESPONSE_MESSAGE_TYPE = MessageType.of(MessageType.Domain.GENERIC_QUERY_ROUTER, 2);
      RPC_REQUEST_MESSAGE_TYPE = MessageType.of(MessageType.Domain.GENERIC_QUERY_ROUTER, 3);
   }

   private class RpcRoutingServerProcessor implements ServerProcessor<RoutedRpcRequest, RoutedQueryResponse> {
      private RpcRoutingServerProcessor() {
      }

      public Message<RoutedQueryResponse> process(RequestContext ctx, RoutedRpcRequest body) throws Exception {
         InternalQueryRouterProtocol.LOGGER.debug("Processing an RPC request {} from remote node {}", Long.valueOf(ctx.getId()), ctx.getRemoteAddress());
         Optional<RpcMethod> method = RpcRegistry.lookupMethod(body.rpcObjectName, body.rpcMethodName);
         ResultMessage result = ((RpcMethod)method.get()).execute(body.clientState, body.values);
         RoutedQueryResponse response = new RoutedQueryResponse(result);
         return new Message(ctx.getId(), InternalQueryRouterProtocol.RESPONSE_MESSAGE_TYPE, response);
      }

      public void onComplete(Message<RoutedQueryResponse> response) {
      }
   }

   private class RoutingServerProcessor implements ServerProcessor<RoutedQueryRequest, RoutedQueryResponse> {
      private RoutingServerProcessor() {
      }

      public Message<RoutedQueryResponse> process(RequestContext ctx, RoutedQueryRequest body) throws Exception {
         InternalQueryRouterProtocol.LOGGER.debug("Processing a request {} from remote node {}", Long.valueOf(ctx.getId()), ctx.getRemoteAddress());
         ResultMessage result = (ResultMessage)TPCUtils.blockingGet(InternalQueryRouterProtocol.this.queryHandler.process(body.query, body.queryState, body.queryOptions, body.payload, System.nanoTime()));
         RoutedQueryResponse response = new RoutedQueryResponse(result);
         return new Message(ctx.getId(), InternalQueryRouterProtocol.RESPONSE_MESSAGE_TYPE, response);
      }

      public void onComplete(Message<RoutedQueryResponse> response) {
      }
   }
}
