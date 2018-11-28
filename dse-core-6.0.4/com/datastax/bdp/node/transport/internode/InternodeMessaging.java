package com.datastax.bdp.node.transport.internode;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.node.transport.Message;
import com.datastax.bdp.node.transport.MessageBodySerializer;
import com.datastax.bdp.node.transport.MessageClient;
import com.datastax.bdp.node.transport.MessageCodec;
import com.datastax.bdp.node.transport.MessageServer;
import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.node.transport.RequestContext;
import com.datastax.bdp.node.transport.SSLOptions;
import com.datastax.bdp.node.transport.ServerProcessor;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.LambdaMayThrow;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import java.util.function.Supplier;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InternodeMessaging implements InternodeProtocolRegistry, Provider<InternodeClient> {
   public static final int SERVER_ACCEPTOR_THREADS = FBUtilities.getAvailableProcessors();
   public static final int SERVER_WORKER_THREADS = FBUtilities.getAvailableProcessors() * 8;
   public static final int CLIENT_WORKER_THREADS = FBUtilities.getAvailableProcessors() * 8;
   public static final int CLIENT_MAX_CONNECTIONS = 100;
   public static final int HANDSHAKE_TIMEOUT_SECONDS = 10;
   public static final int CLIENT_REQUEST_TIMEOUT_SECONDS = 60;
   private static final Logger LOGGER = LoggerFactory.getLogger(InternodeMessaging.class);
   private static final String SERVER_NAME = "internode-messaging";
   private volatile MessageServer remoteServer;
   private volatile MessageClient remoteClient;
   private volatile MessageServer localServer;
   private volatile MessageClient localClient;
   private final MessageServer.Builder serverBuilder = MessageServer.newBuilder();
   private final MessageClient.Builder clientBuilder = MessageClient.newBuilder();
   private final int MAX_FRAME_LENGTH = DseConfig.getInternodeMessagingFrameLength();
   private final int PORT = DseConfig.getInternodeMessagingPort();
   private final MessageCodec codec;
   private volatile boolean activationStarted;

   @Inject
   public InternodeMessaging() {
      this.codec = new MessageCodec((byte)2, this.MAX_FRAME_LENGTH);
      this.activationStarted = false;
      this.serverBuilder.withSSLOptions(SSLOptions.getDefaultForInterNode()).withAcceptorThreads(DseConfig.getInternodeMessagingServerAcceptorThreads()).withWorkerThreads(DseConfig.getInternodeMessagingServerWorkerThreads()).withMessageCodec(this.codec);
      this.clientBuilder.withSSLOptions(SSLOptions.getDefaultForInterNode()).withMaxConnections(DseConfig.getInternodeMessagingClientMaxConnections()).withWorkerThreads(DseConfig.getInternodeMessagingClientWorkerThreads()).withHandshakeTimeoutSecs(DseConfig.getInternodeMessagingClientHandshakeTimeout()).withMessageCodec(this.codec);
   }

   public synchronized <T> void addSerializer(MessageType type, MessageBodySerializer<T> bodySerializer, byte... versions) {
      Preconditions.checkState(!this.activationStarted, "The plugin is already active");
      this.codec.addSerializer(type, bodySerializer, versions);
   }

   public synchronized <I, O> void addProcessor(MessageType type, ServerProcessor<I, O> processor) {
      Preconditions.checkState(!this.activationStarted, "The plugin is already active");
      this.serverBuilder.withProcessor(type, processor);
   }

   public <I, O> void addProcessor(MessageType requestType, final MessageType responseType, final LambdaMayThrow.FunctionMayThrow<I, O> f) {
      ServerProcessor<I, O> processor = new ServerProcessor<I, O>() {
         public Message<O> process(RequestContext ctx, I body) throws Exception {
            return new Message(ctx.getId(), responseType, f.apply(body));
         }

         public void onComplete(Message<O> response) {
         }
      };
      this.addProcessor(requestType, processor);
   }

   public synchronized void activate() {
      this.activationStarted = true;
      this.remoteServer = this.serverBuilder.buildRemote("internode-messaging", Addresses.Internode.getListenAddresses(), this.PORT);
      this.localServer = this.serverBuilder.buildLocal("internode-messaging");
      this.remoteClient = this.clientBuilder.buildRemote();
      this.localClient = this.clientBuilder.buildLocal("internode-messaging");

      try {
         this.remoteServer.bind();
         this.localServer.bind();
         LOGGER.info("Internode messaging server has been bound");
      } catch (InterruptedException var6) {
         InterruptedException e = var6;

         try {
            LOGGER.info("Internode messaging server has been interrupted during bind. Shutting down");
            this.deactivate();
            throw new RuntimeException("Interrupted during activation", e);
         } finally {
            Thread.currentThread().interrupt();
         }
      }
   }

   public synchronized void deactivate() {
      try {
         this.remoteServer.shutdown();
      } catch (Exception var5) {
         ;
      }

      try {
         this.remoteClient.shutdown();
      } catch (Exception var4) {
         ;
      }

      try {
         this.localServer.shutdown();
      } catch (Exception var3) {
         ;
      }

      try {
         this.localClient.shutdown();
      } catch (Exception var2) {
         ;
      }

   }

   public InternodeClient get() {
      return new InternodeClient(() -> {
         return this.remoteClient;
      }, () -> {
         return this.localClient;
      }, "internode-messaging", this.PORT);
   }
}
