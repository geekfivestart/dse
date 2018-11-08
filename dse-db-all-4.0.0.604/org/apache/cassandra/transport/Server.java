package org.apache.cassandra.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.Version;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCEventLoop;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.SystemKeyspacesFilteringRestrictions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.ApproximateTimeSource;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements CassandraDaemon.Server {
   private static final Logger logger;
   public static final AttributeKey<ClientState> ATTR_KEY_CLIENT_STATE;
   public static final TimeSource TIME_SOURCE;
   private final Server.ConnectionTracker connectionTracker;
   private final Connection.Factory connectionFactory;
   public final InetSocketAddress socket;
   public boolean useSSL;
   private final AtomicBoolean isRunning;

   private Server(Server.Builder builder) {
      this.connectionTracker = new Server.ConnectionTracker();
      this.connectionFactory = new Connection.Factory() {
         public Connection newConnection(Channel channel, ProtocolVersion version) {
            return new ServerConnection(channel, version, Server.this.connectionTracker);
         }
      };
      this.useSSL = false;
      this.isRunning = new AtomicBoolean(false);
      this.socket = builder.getSocket();
      this.useSSL = builder.useSSL;
      Server.EventNotifier notifier = new Server.EventNotifier(this, null);
      StorageService.instance.register(notifier);
      Schema.instance.registerListener(notifier);
   }

   public CompletableFuture stop() {
      return this.isRunning.compareAndSet(true, false)?this.close():this.connectionTracker.closeFuture;
   }

   public boolean isRunning() {
      return this.isRunning.get();
   }

   public synchronized void start() {
      if(!this.isRunning()) {
         this.connectionTracker.reset();
         ServerBootstrap bootstrap = ((ServerBootstrap)(new ServerBootstrap()).group(TPC.eventLoopGroup()).channel(TPC.USE_EPOLL?EpollServerSocketChannel.class:NioServerSocketChannel.class)).childOption(ChannelOption.TCP_NODELAY, Boolean.valueOf(true)).childOption(ChannelOption.SO_LINGER, Integer.valueOf(0)).childOption(ChannelOption.SO_KEEPALIVE, Boolean.valueOf(DatabaseDescriptor.getNativeTransportKeepAlive())).childOption(ChannelOption.ALLOCATOR, CBUtil.allocator).childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, Integer.valueOf('耀')).childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, Integer.valueOf(8192));
         if(this.useSSL) {
            EncryptionOptions.ClientEncryptionOptions clientEnc = DatabaseDescriptor.getClientEncryptionOptions();
            if(clientEnc.optional) {
               logger.info("Enabling optionally encrypted CQL connections between client and server");
               bootstrap.childHandler(new Server.OptionalSecureInitializer(this, clientEnc));
            } else {
               logger.info("Enabling encrypted CQL connections between client and server");
               bootstrap.childHandler(new Server.SecureInitializer(this, clientEnc));
            }
         } else {
            bootstrap.childHandler(new Server.Initializer(this));
         }

         logger.info("Using Netty Version: {}", Version.identify().entrySet());
         logger.info("Effective settings: Netty Epoll = {}, AIO = {}, data directories on SSD = {}", new Object[]{Boolean.valueOf(TPC.USE_EPOLL), Boolean.valueOf(TPC.USE_AIO), Boolean.valueOf(DatabaseDescriptor.assumeDataDirectoriesOnSSD())});
         logger.info("Starting listening for CQL clients on {} ({})...", this.socket, this.useSSL?"encrypted":"unencrypted");
         ChannelFuture bindFuture = bootstrap.bind(this.socket);
         if(!bindFuture.awaitUninterruptibly().isSuccess()) {
            throw new IllegalStateException(String.format("Failed to bind port %d on %s.", new Object[]{Integer.valueOf(this.socket.getPort()), this.socket.getAddress().getHostAddress()}));
         } else {
            this.connectionTracker.allChannels.add(bindFuture.channel());
            this.isRunning.set(true);
         }
      }
   }

   public int getConnectedClients() {
      return this.connectionTracker.getConnectedClients();
   }

   private CompletableFuture close() {
      logger.info("Stop listening for CQL clients");
      return this.connectionTracker.closeAll();
   }

   static {
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
      logger = LoggerFactory.getLogger(Server.class);
      ATTR_KEY_CLIENT_STATE = AttributeKey.newInstance("clientState");
      TIME_SOURCE = new ApproximateTimeSource();
   }

   public interface ChannelFilter {
      Server.ChannelFilter NOOP_FILTER = Maybe::just;

      Maybe<Channel> accept(Channel var1);
   }

   private static class EventNotifier extends SchemaChangeListener implements IEndpointLifecycleSubscriber {
      private final Server server;
      private final Map<InetAddress, Server.LatestEvent> latestEvents;
      private final Set<InetAddress> endpointsPendingJoinedNotification;
      private static final InetAddress bindAll;

      private EventNotifier(Server server) {
         this.latestEvents = new ConcurrentHashMap();
         this.endpointsPendingJoinedNotification = ConcurrentHashMap.newKeySet();
         this.server = server;
      }

      private InetAddress getRpcAddress(InetAddress endpoint) {
         try {
            InetAddress rpcAddress = InetAddress.getByName(StorageService.instance.getRpcaddress(endpoint));
            return rpcAddress.equals(bindAll)?endpoint:rpcAddress;
         } catch (UnknownHostException var3) {
            Server.logger.error("Problem retrieving RPC address for {}", endpoint, var3);
            return endpoint;
         }
      }

      private void send(InetAddress endpoint, Event.NodeEvent event) {
         if(Server.logger.isTraceEnabled()) {
            Server.logger.trace("Sending event for endpoint {}, rpc address {}", endpoint, event.nodeAddress());
         }

         if(endpoint.equals(FBUtilities.getBroadcastAddress()) || !event.nodeAddress().equals(FBUtilities.getNativeTransportBroadcastAddress())) {
            this.send((Event)event, (Server.ChannelFilter)Server.ChannelFilter.NOOP_FILTER);
         }
      }

      private void send(Event event, Server.ChannelFilter filter) {
         this.server.connectionTracker.send(event, filter);
      }

      public void onJoinCluster(InetAddress endpoint) {
         if(!StorageService.instance.isRpcReady(endpoint)) {
            this.endpointsPendingJoinedNotification.add(endpoint);
         } else {
            this.onTopologyChange(endpoint, Event.TopologyChange.newNode(this.getRpcAddress(endpoint), this.server.socket.getPort()));
         }

      }

      public void onLeaveCluster(InetAddress endpoint) {
         this.onTopologyChange(endpoint, Event.TopologyChange.removedNode(this.getRpcAddress(endpoint), this.server.socket.getPort()));
      }

      public void onMove(InetAddress endpoint) {
         this.onTopologyChange(endpoint, Event.TopologyChange.movedNode(this.getRpcAddress(endpoint), this.server.socket.getPort()));
      }

      public void onUp(InetAddress endpoint) {
         if(this.endpointsPendingJoinedNotification.remove(endpoint)) {
            this.onJoinCluster(endpoint);
         }

         this.onStatusChange(endpoint, Event.StatusChange.nodeUp(this.getRpcAddress(endpoint), this.server.socket.getPort()));
      }

      public void onDown(InetAddress endpoint) {
         this.onStatusChange(endpoint, Event.StatusChange.nodeDown(this.getRpcAddress(endpoint), this.server.socket.getPort()));
      }

      private void onTopologyChange(InetAddress endpoint, Event.TopologyChange event) {
         if(Server.logger.isTraceEnabled()) {
            Server.logger.trace("Topology changed event : {}, {}", endpoint, event.change);
         }

         Server.LatestEvent prev = (Server.LatestEvent)this.latestEvents.get(endpoint);
         if(prev == null || prev.topology != event.change) {
            Server.LatestEvent ret = (Server.LatestEvent)this.latestEvents.put(endpoint, Server.LatestEvent.forTopologyChange(event.change, prev));
            if(ret == prev) {
               this.send((InetAddress)endpoint, (Event.NodeEvent)event);
            }
         }

      }

      private void onStatusChange(InetAddress endpoint, Event.StatusChange event) {
         if(Server.logger.isTraceEnabled()) {
            Server.logger.trace("Status changed event : {}, {}", endpoint, event.status);
         }

         Server.LatestEvent prev = (Server.LatestEvent)this.latestEvents.get(endpoint);
         if(prev == null || prev.status != event.status) {
            Server.LatestEvent ret = (Server.LatestEvent)this.latestEvents.put(endpoint, Server.LatestEvent.forStatusChange(event.status, (Server.LatestEvent)null));
            if(ret == prev) {
               this.send((InetAddress)endpoint, (Event.NodeEvent)event);
            }
         }

      }

      public void onCreateKeyspace(String ksName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
      }

      public void onCreateTable(String ksName, String cfName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
      }

      public void onCreateType(String ksName, String typeName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
      }

      public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION, ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE, ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      public void onAlterKeyspace(String ksName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName));
      }

      public void onAlterTable(String ksName, String cfName, boolean affectsStatements) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
      }

      public void onAlterType(String ksName, String typeName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
      }

      public void onAlterFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.FUNCTION, ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      public void onAlterAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.AGGREGATE, ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      public void onDropKeyspace(String ksName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
      }

      public void onDropTable(String ksName, String cfName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksName, cfName));
      }

      public void onDropType(String ksName, String typeName) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, ksName, typeName));
      }

      public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION, ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         this.sendSchemaChange(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.AGGREGATE, ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
      }

      private void sendSchemaChange(Event.SchemaChange event) {
         this.send((Event)event, (Server.ChannelFilter)SystemKeyspacesFilteringRestrictions.getChannelFilter(event));
      }

      static {
         try {
            bindAll = InetAddress.getByAddress(new byte[4]);
         } catch (UnknownHostException var1) {
            throw new AssertionError(var1);
         }
      }
   }

   private static class LatestEvent {
      public final Event.StatusChange.Status status;
      public final Event.TopologyChange.Change topology;

      private LatestEvent(Event.StatusChange.Status status, Event.TopologyChange.Change topology) {
         this.status = status;
         this.topology = topology;
      }

      public String toString() {
         return String.format("Status %s, Topology %s", new Object[]{this.status, this.topology});
      }

      public static Server.LatestEvent forStatusChange(Event.StatusChange.Status status, Server.LatestEvent prev) {
         return new Server.LatestEvent(status, prev == null?null:prev.topology);
      }

      public static Server.LatestEvent forTopologyChange(Event.TopologyChange.Change change, Server.LatestEvent prev) {
         return new Server.LatestEvent(prev == null?null:prev.status, change);
      }
   }

   private static class SecureInitializer extends Server.AbstractSecureIntializer {
      public SecureInitializer(Server server, EncryptionOptions encryptionOptions) {
         super(server, encryptionOptions);
      }

      protected void initChannel(Channel channel) throws Exception {
         SslHandler sslHandler = this.createSslHandler();
         super.initChannel(channel);
         channel.pipeline().addFirst("ssl", sslHandler);
      }
   }

   private static class OptionalSecureInitializer extends Server.AbstractSecureIntializer {
      public OptionalSecureInitializer(Server server, EncryptionOptions encryptionOptions) {
         super(server, encryptionOptions);
      }

      protected void initChannel(Channel channel) throws Exception {
         super.initChannel(channel);
         channel.pipeline().addFirst("sslDetectionHandler", new ByteToMessageDecoder() {
            protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
               if(byteBuf.readableBytes() >= 5) {
                  if(SslHandler.isEncrypted(byteBuf)) {
                     SslHandler sslHandler = OptionalSecureInitializer.this.createSslHandler();
                     channelHandlerContext.pipeline().replace(this, "ssl", sslHandler);
                  } else {
                     channelHandlerContext.pipeline().remove(this);
                  }

               }
            }
         });
      }
   }

   protected abstract static class AbstractSecureIntializer extends Server.Initializer {
      private final SSLContext sslContext;
      private final EncryptionOptions encryptionOptions;

      protected AbstractSecureIntializer(Server server, EncryptionOptions encryptionOptions) {
         super(server);
         this.encryptionOptions = encryptionOptions;

         try {
            this.sslContext = SSLFactory.createSSLContext(encryptionOptions, encryptionOptions.require_client_auth);
         } catch (IOException var4) {
            throw new RuntimeException("Failed to setup secure pipeline", var4);
         }
      }

      protected final SslHandler createSslHandler() {
         SSLEngine sslEngine = this.sslContext.createSSLEngine();
         sslEngine.setUseClientMode(false);
         String[] suites = SSLFactory.filterCipherSuites(sslEngine.getSupportedCipherSuites(), this.encryptionOptions.cipher_suites);
         sslEngine.setEnabledCipherSuites(suites);
         sslEngine.setNeedClientAuth(this.encryptionOptions.require_client_auth);
         return new SslHandler(sslEngine);
      }
   }

   private static class Initializer extends ChannelInitializer<Channel> {
      private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
      private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
      private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
      private static final Frame.Compressor frameCompressor = new Frame.Compressor();
      private static final Frame.Encoder frameEncoder = new Frame.Encoder();
      private static final Message.ExceptionHandler exceptionHandler = new Message.ExceptionHandler();
      private static final Message.Dispatcher dispatcher = new Message.Dispatcher();
      private static final ConnectionLimitHandler connectionLimitHandler = new ConnectionLimitHandler();
      private final Server server;
      private final Map<EventLoop, Frame.AsyncProcessor> asyncFrameProcessors;

      public Initializer(Server server) {
         this.server = server;
         this.asyncFrameProcessors = new HashMap();
      }

      protected void initChannel(Channel channel) throws Exception {
         ChannelPipeline pipeline = channel.pipeline();
         EventLoop eventLoop = channel.eventLoop();
         Frame.AsyncProcessor processor;
         synchronized(this) {
            processor = (Frame.AsyncProcessor)this.asyncFrameProcessors.get(eventLoop);
            if(processor == null && eventLoop instanceof TPCEventLoop) {
               processor = new Frame.AsyncProcessor((TPCEventLoop)eventLoop);
               this.asyncFrameProcessors.put(eventLoop, processor);
            }
         }

         assert !TPC.USE_EPOLL || !this.asyncFrameProcessors.isEmpty();

         if(DatabaseDescriptor.getNativeTransportMaxConcurrentConnections() > 0L || DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0L) {
            pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
         }

         pipeline.addLast("frameDecoder", new Frame.Decoder(Server.TIME_SOURCE, this.server.connectionFactory, processor));
         pipeline.addLast("frameEncoder", frameEncoder);
         pipeline.addLast("frameDecompressor", frameDecompressor);
         pipeline.addLast("frameCompressor", frameCompressor);
         pipeline.addLast("messageDecoder", messageDecoder);
         pipeline.addLast("messageEncoder", messageEncoder);
         pipeline.addLast("exceptionHandler", exceptionHandler);
         pipeline.addLast("executor", dispatcher);
      }
   }

   public static class ConnectionTracker implements Connection.Tracker {
      public final ChannelGroup allChannels;
      private final EnumMap<Event.Type, ChannelGroup> groups;
      private volatile BlockingQueue<CompletableFuture<?>> inFlightRequestsFutures;
      private volatile CompletableFuture closeFuture;

      public ConnectionTracker() {
         this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
         this.groups = new EnumMap(Event.Type.class);
         this.inFlightRequestsFutures = null;
         this.closeFuture = new CompletableFuture();
         Event.Type[] var1 = Event.Type.values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            Event.Type type = var1[var3];
            this.groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
         }

      }

      public void addConnection(Channel ch, Connection connection) {
         this.allChannels.add(ch);
      }

      public void removeConnection(Channel ch, Connection connection) {
         if(Server.logger.isTraceEnabled()) {
            Server.logger.trace("Removing connection {} from connection tracker", ch);
         }

         this.allChannels.remove(ch);

         assert connection instanceof ServerConnection : "Expected connection of type ServerConnection";

         ServerConnection serverConnection = (ServerConnection)connection;
         if(this.inFlightRequestsFutures != null) {
            this.inFlightRequestsFutures.offer(serverConnection.waitForInFlightRequests());
         }

      }

      public void register(Event.Type type, Channel ch) {
         ((ChannelGroup)this.groups.get(type)).add(ch);
      }

      public void send(Event event, Server.ChannelFilter filter) {
         if(Server.ChannelFilter.NOOP_FILTER.equals(filter)) {
            ((ChannelGroup)this.groups.get(event.type)).writeAndFlush(new EventMessage(event));
         } else {
            Observable var10000 = Observable.fromIterable((Iterable)this.groups.get(event.type));
            filter.getClass();
            var10000.flatMapMaybe(filter::accept).subscribe((channel) -> {
               channel.writeAndFlush(new EventMessage(event));
            });
         }

      }

      public CompletableFuture closeAll() {
         if(Server.logger.isTraceEnabled()) {
            Server.logger.trace("Closing all channels");
         }

         if(this.allChannels.isEmpty()) {
            this.closeFuture.complete((Object)null);
            return this.closeFuture;
         } else {
            assert this.inFlightRequestsFutures == null : "closeAll should only be called once";

            this.inFlightRequestsFutures = new LinkedBlockingDeque();
            this.allChannels.close().addListener((future) -> {
               CompletableFuture.allOf((CompletableFuture[])this.inFlightRequestsFutures.toArray(new CompletableFuture[0])).whenComplete((res, err) -> {
                  if(err == null) {
                     this.closeFuture.complete((Object)null);
                  } else {
                     this.closeFuture.completeExceptionally(err);
                  }

               });
            });
            return this.closeFuture;
         }
      }

      public void reset() {
         assert this.allChannels.isEmpty();

         this.closeFuture = new CompletableFuture();
         this.inFlightRequestsFutures = null;
      }

      int getConnectedClients() {
         return this.allChannels.size() != 0?this.allChannels.size() - 1:0;
      }
   }

   public static class Builder {
      private boolean useSSL = false;
      private InetAddress hostAddr;
      private int port = -1;
      private InetSocketAddress socket;

      public Builder() {
      }

      public Server.Builder withSSL(boolean useSSL) {
         this.useSSL = useSSL;
         return this;
      }

      public Server.Builder withHost(InetAddress host) {
         this.hostAddr = host;
         this.socket = null;
         return this;
      }

      public Server.Builder withPort(int port) {
         this.port = port;
         this.socket = null;
         return this;
      }

      public Server build() {
         return new Server(this, null);
      }

      private InetSocketAddress getSocket() {
         if(this.socket != null) {
            return this.socket;
         } else if(this.port == -1) {
            throw new IllegalStateException("Missing port number");
         } else if(this.hostAddr != null) {
            this.socket = new InetSocketAddress(this.hostAddr, this.port);
            return this.socket;
         } else {
            throw new IllegalStateException("Missing host");
         }
      }
   }
}
