package org.apache.cassandra.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.ApproximateTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleClient implements Closeable {
   private static final Logger logger;
   public static final TimeSource TIME_SOURCE;
   public final String host;
   public final int port;
   private final EncryptionOptions.ClientEncryptionOptions encryptionOptions;
   protected final SimpleClient.ResponseHandler responseHandler;
   protected final Connection.Tracker tracker;
   protected final ProtocolVersion version;
   protected Connection connection;
   protected Bootstrap bootstrap;
   protected Channel channel;
   protected ChannelFuture lastWriteFuture;
   private final Connection.Factory connectionFactory;
   private static final Message.ProtocolDecoder messageDecoder;
   private static final Message.ProtocolEncoder messageEncoder;
   private static final Frame.Decompressor frameDecompressor;
   private static final Frame.Compressor frameCompressor;
   private static final Frame.Encoder frameEncoder;

   public SimpleClient(String host, int port, ProtocolVersion version, EncryptionOptions.ClientEncryptionOptions encryptionOptions) {
      this(host, port, version, false, encryptionOptions);
   }

   public SimpleClient(String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions) {
      this(host, port, ProtocolVersion.CURRENT, encryptionOptions);
   }

   public SimpleClient(String host, int port, ProtocolVersion version) {
      this(host, port, version, new EncryptionOptions.ClientEncryptionOptions());
   }

   public SimpleClient(String host, int port, ProtocolVersion version, boolean useBeta, EncryptionOptions.ClientEncryptionOptions encryptionOptions) {
      this.responseHandler = new SimpleClient.ResponseHandler(null);
      this.tracker = new SimpleClient.ConnectionTracker(null);
      this.connectionFactory = new Connection.Factory() {
         public Connection newConnection(Channel channel, ProtocolVersion version) {
            return SimpleClient.this.connection;
         }
      };
      this.host = host;
      this.port = port;
      if(version.isBeta() && !useBeta) {
         throw new IllegalArgumentException(String.format("Beta version of server used (%s), but USE_BETA flag is not set", new Object[]{version}));
      } else {
         this.version = version;
         this.encryptionOptions = encryptionOptions;
      }
   }

   public SimpleClient(String host, int port) {
      this(host, port, new EncryptionOptions.ClientEncryptionOptions());
   }

   public SimpleClient connect(boolean useCompression) throws IOException {
      this.establishConnection();
      Map<String, String> options = new HashMap();
      options.put("CQL_VERSION", "3.0.0");
      if(useCompression) {
         options.put("COMPRESSION", "snappy");
         this.connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
      }

      this.execute(new StartupMessage(options));
      return this;
   }

   public void setEventHandler(SimpleClient.EventHandler eventHandler) {
      this.responseHandler.eventHandler = eventHandler;
   }

   protected void establishConnection() throws IOException {
      this.bootstrap = (Bootstrap)((Bootstrap)((Bootstrap)(new Bootstrap()).group(new NioEventLoopGroup())).channel(NioSocketChannel.class)).option(ChannelOption.TCP_NODELAY, Boolean.valueOf(true));
      if(this.encryptionOptions.enabled) {
         this.bootstrap.handler(new SimpleClient.SecureInitializer());
      } else {
         this.bootstrap.handler(new SimpleClient.Initializer(null));
      }

      ChannelFuture future = this.bootstrap.connect(new InetSocketAddress(this.host, this.port));
      this.channel = future.awaitUninterruptibly().channel();
      if(!future.isSuccess()) {
         this.bootstrap.group().shutdownGracefully();
         throw new IOException("Connection Error", future.cause());
      }
   }

   public void login(Map<String, String> credentials) {
      if(credentials.get("username") != null && credentials.get("password") != null) {
         AuthResponse msg = new AuthResponse(this.encodeCredentialsForSasl(credentials));
         this.execute(msg);
      } else {
         throw new AuthenticationException("Authentication requires both 'username' and 'password'");
      }
   }

   protected byte[] encodeCredentialsForSasl(Map<String, String> credentials) {
      byte[] username = ((String)credentials.get("username")).getBytes(StandardCharsets.UTF_8);
      byte[] password = ((String)credentials.get("password")).getBytes(StandardCharsets.UTF_8);
      byte[] initialResponse = new byte[username.length + password.length + 2];
      initialResponse[0] = 0;
      System.arraycopy(username, 0, initialResponse, 1, username.length);
      initialResponse[username.length + 1] = 0;
      System.arraycopy(password, 0, initialResponse, username.length + 2, password.length);
      return initialResponse;
   }

   public ResultMessage execute(String query, ConsistencyLevel consistency) {
      return this.execute(query, Collections.emptyList(), consistency);
   }

   public ResultMessage execute(String query, List<ByteBuffer> values, ConsistencyLevel consistencyLevel) {
      Message.Response msg = this.execute(new QueryMessage(query, QueryOptions.forInternalCalls(consistencyLevel, values)));

      assert msg instanceof ResultMessage;

      return (ResultMessage)msg;
   }

   public ResultMessage.Prepared prepare(String query) {
      Message.Response msg = this.execute(new PrepareMessage(query, (String)null));

      assert msg instanceof ResultMessage.Prepared;

      return (ResultMessage.Prepared)msg;
   }

   public ResultMessage executePrepared(ResultMessage.Prepared prepared, List<ByteBuffer> values, ConsistencyLevel consistency) {
      Message.Response msg = this.execute(new ExecuteMessage(prepared.statementId, prepared.resultMetadataId, QueryOptions.forInternalCalls(consistency, values)));

      assert msg instanceof ResultMessage;

      return (ResultMessage)msg;
   }

   public void close() {
      if(this.lastWriteFuture != null) {
         this.lastWriteFuture.awaitUninterruptibly();
      }

      this.channel.close().awaitUninterruptibly();
      this.bootstrap.group().shutdownGracefully();
   }

   public Message.Response execute(Message.Request request) {
      try {
         request.attach(this.connection);
         this.lastWriteFuture = this.channel.writeAndFlush(request);
         Message.Response msg = (Message.Response)this.responseHandler.responses.take();
         if(msg instanceof ErrorMessage) {
            throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
         } else {
            return msg;
         }
      } catch (InterruptedException var3) {
         throw new RuntimeException(var3);
      }
   }

   static {
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
      logger = LoggerFactory.getLogger(SimpleClient.class);
      TIME_SOURCE = new ApproximateTimeSource();
      messageDecoder = new Message.ProtocolDecoder();
      messageEncoder = new Message.ProtocolEncoder();
      frameDecompressor = new Frame.Decompressor();
      frameCompressor = new Frame.Compressor();
      frameEncoder = new Frame.Encoder();
   }

   @Sharable
   private static class ResponseHandler extends SimpleChannelInboundHandler<Message.Response> {
      public final BlockingQueue<Message.Response> responses;
      public SimpleClient.EventHandler eventHandler;

      private ResponseHandler() {
         this.responses = new SynchronousQueue(true);
      }

      public void channelRead0(ChannelHandlerContext ctx, Message.Response r) {
         try {
            if(r instanceof EventMessage) {
               if(this.eventHandler != null) {
                  this.eventHandler.onEvent(((EventMessage)r).event);
               }
            } else {
               this.responses.put(r);
            }

         } catch (InterruptedException var4) {
            throw new RuntimeException(var4);
         }
      }

      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         if(this == ctx.pipeline().last()) {
            SimpleClient.logger.error("Exception in response", cause);
         }

         ctx.fireExceptionCaught(cause);
      }
   }

   private class SecureInitializer extends SimpleClient.Initializer {
      private final SSLContext sslContext;

      public SecureInitializer() throws IOException {
         super(null);
         this.sslContext = SSLFactory.createSSLContext(SimpleClient.this.encryptionOptions, true);
      }

      protected void initChannel(Channel channel) throws Exception {
         super.initChannel(channel);
         SSLEngine sslEngine = this.sslContext.createSSLEngine();
         sslEngine.setUseClientMode(true);
         String[] suites = SSLFactory.filterCipherSuites(sslEngine.getSupportedCipherSuites(), SimpleClient.this.encryptionOptions.cipher_suites);
         sslEngine.setEnabledCipherSuites(suites);
         channel.pipeline().addFirst("ssl", new SslHandler(sslEngine));
      }
   }

   private class Initializer extends ChannelInitializer<Channel> {
      private Initializer() {
      }

      protected void initChannel(Channel channel) throws Exception {
         SimpleClient.this.connection = new Connection(channel, SimpleClient.this.version, SimpleClient.this.tracker);
         channel.attr(Connection.attributeKey).set(SimpleClient.this.connection);
         ChannelPipeline pipeline = channel.pipeline();
         pipeline.addLast("frameDecoder", new Frame.Decoder(SimpleClient.TIME_SOURCE, SimpleClient.this.connectionFactory));
         pipeline.addLast("frameEncoder", SimpleClient.frameEncoder);
         pipeline.addLast("frameDecompressor", SimpleClient.frameDecompressor);
         pipeline.addLast("frameCompressor", SimpleClient.frameCompressor);
         pipeline.addLast("messageDecoder", SimpleClient.messageDecoder);
         pipeline.addLast("messageEncoder", SimpleClient.messageEncoder);
         pipeline.addLast("handler", SimpleClient.this.responseHandler);
      }
   }

   private static class ConnectionTracker implements Connection.Tracker {
      private ConnectionTracker() {
      }

      public void addConnection(Channel ch, Connection connection) {
      }

      public void removeConnection(Channel ch, Connection connection) {
      }

      public boolean isRegistered(Event.Type type, Channel ch) {
         return false;
      }
   }

   public static class SimpleEventHandler implements SimpleClient.EventHandler {
      public final LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue();

      public SimpleEventHandler() {
      }

      public void onEvent(Event event) {
         this.queue.add(event);
      }
   }

   public interface EventHandler {
      void onEvent(Event var1);
   }
}
