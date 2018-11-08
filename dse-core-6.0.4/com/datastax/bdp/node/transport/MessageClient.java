package com.datastax.bdp.node.transport;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.gms.EndpointStateChangeAdapter;
import com.datastax.bdp.server.DseDaemon;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageClient {
   private static final int CONNECTION_MAX_WAIT = 250;
   private static final int CONNECTION_MAX_AGE = 600000;
   private static final int CONNECTION_LIMITER_CHECK_TIME = 300000;
   private static final int CONNECTION_LIMITER_SCHEDULER_TIMEOUT = 30;
   protected final List<EventExecutorGroup> groups = new CopyOnWriteArrayList();
   protected final int maxConnections;
   protected final int workerThreads;
   protected final int handshakeTimeoutSecs;
   protected final Optional<SSLOptions> sslOptions;
   private final Logger logger = LoggerFactory.getLogger(this.getClass());
   private final AtomicInteger totalConnections = new AtomicInteger(0);
   private final ConcurrentMap<String, BlockingQueue<ClientServerConnection>> liveConnectionsQueue = new ConcurrentHashMap();
   private final ConcurrentMap<String, Set<ClientServerConnection>> liveConnectionsTracker = new ConcurrentHashMap();
   private final ScheduledExecutorService limitCheckScheduler = Executors.newSingleThreadScheduledExecutor((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(this.getClass().getSimpleName() + " connection limiter - %s").build());
   private final MessageCodec codec;
   private final Supplier<Bootstrap> bootstrap = Suppliers.memoize(new Supplier<Bootstrap>() {
      public Bootstrap get() {
         Bootstrap _bootstrap = MessageClient.this.doSetup();
         _bootstrap.option(ChannelOption.ALLOCATOR, CBUtil.allocator);
         _bootstrap.handler(new ChannelInitializer() {
            protected void initChannel(Channel channel) throws Exception {
               MessageClient.this.sslOptions.ifPresent((sslOptions) -> {
                  channel.pipeline().addFirst(new ChannelHandler[]{sslOptions.createClientSslHandler()});
               });
            }
         });
         MessageClient.this.limitCheckScheduler.scheduleAtFixedRate(MessageClient.this.new ConnectionLimiter(null), 300000L, 300000L, TimeUnit.MILLISECONDS);
         return _bootstrap;
      }
   });

   public static MessageClient.Builder newBuilder() {
      return new MessageClient.Builder(null);
   }

   public MessageClient(int maxConnections, int workerThreads, int handshakeTimeoutSecs, Optional<SSLOptions> sslOptions, MessageCodec codec) {
      this.maxConnections = maxConnections;
      this.workerThreads = workerThreads;
      this.handshakeTimeoutSecs = handshakeTimeoutSecs;
      this.sslOptions = sslOptions;
      this.codec = codec;
   }

   public void sendTo(String address, int port, ClientContext context, Message message) {
      BlockingQueue<ClientServerConnection> hostConnectionsQueue = null;
      ClientServerConnection connection = null;

      while(connection == null) {
         hostConnectionsQueue = (BlockingQueue)this.liveConnectionsQueue.get(address);
         if(hostConnectionsQueue == null) {
            this.liveConnectionsQueue.putIfAbsent(address, new PriorityBlockingQueue(10, new MessageClient.ConnectionComparator(null)));
            this.liveConnectionsTracker.putIfAbsent(address, new ConcurrentSkipListSet());
            hostConnectionsQueue = (BlockingQueue)this.liveConnectionsQueue.get(address);
            connection = this.tryNewConnection(address, port);
         }

         if(connection == null) {
            try {
               connection = (ClientServerConnection)hostConnectionsQueue.poll(250L, TimeUnit.MILLISECONDS);
               if(connection != null) {
                  if(connection.isFaulty()) {
                     if(connection.shutdown()) {
                        this.totalConnections.decrementAndGet();
                        ((Set)this.liveConnectionsTracker.get(address)).remove(connection);
                     }

                     connection = this.tryNewConnection(address, port);
                  }
               } else {
                  connection = this.tryNewConnection(address, port);
               }
            } catch (InterruptedException var9) {
               context.onError(var9);
               return;
            }
         }
      }

      int order = connection.acquire();
      hostConnectionsQueue.offer(connection);
      if(order > 1) {
         ClientServerConnection newConnection = this.tryNewConnection(address, port);
         if(newConnection != null) {
            hostConnectionsQueue.offer(newConnection);
         }
      }

      connection.send(context, message);
   }

   public void shutdown() {
      this.limitCheckScheduler.shutdownNow();

      try {
         if(!this.limitCheckScheduler.awaitTermination(30L, TimeUnit.SECONDS)) {
            this.logger.warn("The connection limiter scheduler could not be shutdown within {} seconds.", Integer.valueOf(30));
         }
      } catch (InterruptedException var3) {
         Thread.currentThread().interrupt();
      }

      Iterator var1 = this.groups.iterator();

      while(var1.hasNext()) {
         EventExecutorGroup group = (EventExecutorGroup)var1.next();
         group.shutdownGracefully().syncUninterruptibly();
      }

   }

   protected abstract Bootstrap doSetup();

   protected abstract ChannelFuture doConnect(Bootstrap var1, String var2, int var3);

   private ClientServerConnection tryNewConnection(String address, int port) {
      int updatedTotal = this.totalConnections.incrementAndGet();
      if(updatedTotal > this.maxConnections && !((Set)this.liveConnectionsTracker.get(address)).isEmpty()) {
         this.totalConnections.decrementAndGet();
         return null;
      } else {
         if(updatedTotal > this.maxConnections) {
            this.logger.warn("Exceeded maximum number of connections due to new connection required for host {}:{}. New total {}. Maximum connections {}.", new Object[]{address, Integer.valueOf(port), Integer.valueOf(updatedTotal), Integer.valueOf(this.maxConnections)});
         }

         Channel channel;
         try {
            channel = this.doConnect((Bootstrap)this.bootstrap.get(), address, port).syncUninterruptibly().channel();
            this.logger.debug("Connected client channel with {}:{}", address, Integer.valueOf(port));
         } catch (Throwable var8) {
            this.logConnectionError(String.format("Failed to connect to client channel with %s:%d", new Object[]{address, Integer.valueOf(port)}), var8);
            this.totalConnections.decrementAndGet();
            throw new RuntimeException(var8.getMessage(), var8);
         }

         ClientServerConnection connection = new ClientServerConnection(channel, this.handshakeTimeoutSecs);

         try {
            connection.configure(this.codec);
            ((Set)this.liveConnectionsTracker.get(address)).add(connection);
            return connection;
         } catch (Throwable var7) {
            this.logConnectionError(String.format("Failed to create a new connection with %s:%d", new Object[]{address, Integer.valueOf(port)}), var7);
            this.totalConnections.decrementAndGet();
            connection.shutdown();
            throw new RuntimeException(var7.getMessage(), var7);
         }
      }
   }

   private void logConnectionError(String message, Throwable ex) {
      if(DseDaemon.isStopped()) {
         this.logger.debug(message, ex);
      } else {
         this.logger.warn(message, ex);
      }

   }

   private class ConnectionLimiter implements Runnable {
      private ConnectionLimiter() {
      }

      public void run() {
         long targetSize = (long)(MessageClient.this.maxConnections / MessageClient.this.liveConnectionsQueue.size());
         targetSize = targetSize > 0L?targetSize:1L;
         Iterator var3 = MessageClient.this.liveConnectionsQueue.entrySet().iterator();

         while(true) {
            label48:
            while(var3.hasNext()) {
               Entry<String, BlockingQueue<ClientServerConnection>> perAddressConnections = (Entry)var3.next();
               String address = (String)perAddressConnections.getKey();
               BlockingQueue<ClientServerConnection> hostConnections = (BlockingQueue)perAddressConnections.getValue();
               int currentSize = hostConnections.size();

               while(true) {
                  while(true) {
                     if(currentSize <= 0) {
                        continue label48;
                     }

                     ClientServerConnection candidate = (ClientServerConnection)hostConnections.poll();
                     if(candidate != null && (candidate.isFaulty() || (long)currentSize > targetSize && candidate.isOlderThan(600000L, TimeUnit.MILLISECONDS) && !candidate.isAcquired() && !candidate.isActive())) {
                        if(candidate.shutdown()) {
                           MessageClient.this.totalConnections.decrementAndGet();
                           ((Set)MessageClient.this.liveConnectionsTracker.get(address)).remove(candidate);
                        }

                        --currentSize;
                     } else if(candidate != null) {
                        hostConnections.offer(candidate);
                        continue label48;
                     }
                  }
               }
            }

            return;
         }
      }
   }

   protected class RequestTerminator extends EndpointStateChangeAdapter {
      protected RequestTerminator() {
      }

      public void onDead(InetAddress endpoint, EndpointState state) {
         String address = DseConfig.isKerberosEnabled()?endpoint.getCanonicalHostName():endpoint.getHostAddress();
         Set<ClientServerConnection> hostConnections = (Set)MessageClient.this.liveConnectionsTracker.get(address);
         if(hostConnections != null) {
            Iterator var5 = hostConnections.iterator();

            while(var5.hasNext()) {
               ClientServerConnection connection = (ClientServerConnection)var5.next();
               if(connection.shutdown()) {
                  MessageClient.this.totalConnections.decrementAndGet();
               }
            }

            hostConnections.clear();
         }

      }
   }

   private static class ConnectionComparator implements Comparator<ClientServerConnection> {
      private ConnectionComparator() {
      }

      public int compare(ClientServerConnection c1, ClientServerConnection c2) {
         return Integer.compare(c1.getAcquired(), c2.getAcquired());
      }
   }

   public static class Builder {
      private int maxConnections;
      private int workerThreads;
      private int handshakeTimeoutSecs;
      private Optional<SSLOptions> sslOptions;
      private MessageCodec codec;

      private Builder() {
         this.maxConnections = 100;
         this.workerThreads = FBUtilities.getAvailableProcessors() * 8;
         this.handshakeTimeoutSecs = 10;
         this.sslOptions = Optional.empty();
      }

      public MessageClient.Builder withMaxConnections(int maxConnections) {
         this.maxConnections = maxConnections;
         return this;
      }

      public MessageClient.Builder withWorkerThreads(int workerThreads) {
         this.workerThreads = workerThreads;
         return this;
      }

      public MessageClient.Builder withHandshakeTimeoutSecs(int handshakeTimeoutSecs) {
         this.handshakeTimeoutSecs = handshakeTimeoutSecs;
         return this;
      }

      public MessageClient.Builder withSSLOptions(Optional<SSLOptions> sslOptions) {
         this.sslOptions = sslOptions;
         return this;
      }

      public MessageClient.Builder withMessageCodec(MessageCodec codec) {
         this.codec = codec;
         return this;
      }

      public LocalMessageClient buildLocal(String name) {
         Preconditions.checkState(name != null, "Address cannot be null");
         return new LocalMessageClient(this.maxConnections, this.workerThreads, this.handshakeTimeoutSecs, this.sslOptions, this.codec, name);
      }

      public RemoteMessageClient buildRemote() {
         return new RemoteMessageClient(this.maxConnections, this.workerThreads, this.handshakeTimeoutSecs, this.sslOptions, this.codec);
      }
   }
}
