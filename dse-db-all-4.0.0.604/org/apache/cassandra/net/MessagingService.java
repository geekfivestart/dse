package org.apache.cassandra.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.net.ssl.SSLHandshakeException;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCEventLoop;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.net.interceptors.Interceptors;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessagingService implements MessagingServiceMBean {
   private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);
   private static final NoSpamLogger noSpamLogger;
   public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";
   public static final MessagingVersion current_version;
   public static final int PROTOCOL_MAGIC = -900387334;
   private static final AtomicInteger idGen;
   private final Interceptors messageInterceptors;
   public final MessagingMetrics metrics;
   public static final long STARTUP_TIME;
   private final ExpiringMap<Integer, CallbackInfo<?>> callbacks;
   @VisibleForTesting
   final ConcurrentMap<InetAddress, OutboundTcpConnectionPool> connectionManagers;
   private final List<MessagingService.SocketThread> socketThreads;
   private final SimpleCondition listenGate;
   private final DroppedMessages droppedMessages;
   private final List<ILatencySubscriber> subscribers;
   private final ConcurrentMap<InetAddress, MessagingVersion> versions;
   private final BackPressureStrategy backPressure;

   public static MessagingService instance() {
      return MessagingService.MSHandle.instance;
   }

   static MessagingService test() {
      return MessagingService.MSTestHandle.instance;
   }

   private MessagingService(boolean testOnly) {
      this.messageInterceptors = new Interceptors();
      this.metrics = new MessagingMetrics();
      this.connectionManagers = new NonBlockingHashMap();
      this.socketThreads = Lists.newArrayList();
      this.droppedMessages = new DroppedMessages();
      this.subscribers = new CopyOnWriteArrayList();
      this.versions = new NonBlockingHashMap();
      this.backPressure = DatabaseDescriptor.getBackPressureStrategy();
      this.listenGate = new SimpleCondition();
      this.versions.put(FBUtilities.getBroadcastAddress(), current_version);
      if(!testOnly) {
         this.droppedMessages.scheduleLogging();
      }

      Consumer<ExpiringMap.ExpiringObject<Integer, CallbackInfo<?>>> timeoutReporter = (expiring) -> {
         CallbackInfo expiredCallbackInfo = (CallbackInfo)expiring.getValue();
         MessageCallback<?> callback = expiredCallbackInfo.callback;
         InetAddress target = expiredCallbackInfo.target;
         this.addLatency(expiredCallbackInfo.verb, target, expiring.timeoutMillis());
         ConnectionMetrics.totalTimeouts.mark();
         OutboundTcpConnectionPool cp = (OutboundTcpConnectionPool)this.getConnectionPool(expiredCallbackInfo.target).join();
         if(cp != null) {
            cp.incrementTimeout();
         }

         this.updateBackPressureOnReceive(target, expiredCallbackInfo.verb, true).join();
         expiredCallbackInfo.responseExecutor.execute(() -> {
            callback.onTimeout(target);
         }, (ExecutorLocals)null);
      };
      this.callbacks = new ExpiringMap(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);
      if(!testOnly) {
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

         try {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.net:type=MessagingService"));
         } catch (Exception var5) {
            throw new RuntimeException(var5);
         }
      }

      this.messageInterceptors.load();
   }

   public void addInterceptor(Interceptor interceptor) {
      this.messageInterceptors.add(interceptor);
   }

   public void removeInterceptor(Interceptor interceptor) {
      this.messageInterceptors.remove(interceptor);
   }

   public void clearInterceptors() {
      this.messageInterceptors.clear();
   }

   static int newMessageId() {
      return idGen.incrementAndGet();
   }

   public <Q> void send(Request<?, Q> request, MessageCallback<Q> callback) {
      if(request.isLocal()) {
         this.deliverLocally(request, callback);
      } else {
         this.registerCallback(request, callback);
         ClientWarn.instance.storeForRequest(request.id());
         this.updateBackPressureOnSend(request);
         this.sendRequest(request, callback);
      }

   }

   public <Q> void send(Request.Dispatcher<?, Q> dispatcher, MessageCallback<Q> callback) {
      assert callback != null && !dispatcher.verb().isOneWay();

      Iterator var3 = dispatcher.remoteRequests().iterator();

      while(var3.hasNext()) {
         Request<?, Q> request = (Request)var3.next();
         this.send(request, callback);
      }

      if(dispatcher.hasLocalRequest()) {
         this.deliverLocally(dispatcher.localRequest(), callback);
      }

   }

   private <P, Q> void deliverLocally(Request<P, Q> request, MessageCallback<Q> callback) {
      Consumer<Response<Q>> handleResponse = (response) -> {
         request.responseExecutor().execute(() -> {
            this.deliverLocalResponse(request, response, callback);
         }, ExecutorLocals.create());
      };
      Consumer<Response<Q>> onResponse = this.messageInterceptors.isEmpty()?handleResponse:(r) -> {
         this.messageInterceptors.intercept(r, handleResponse, (Consumer)null);
      };
      Runnable onAborted = () -> {
         request.responseExecutor().execute(() -> {
            Tracing.trace("Discarding partial local response (timed out)");
            instance().incrementDroppedMessages(request);
            callback.onTimeout(FBUtilities.getBroadcastAddress());
         }, ExecutorLocals.create());
      };
      Consumer<Request<P, Q>> consumer = (rq) -> {
         if(rq.isTimedOut(ApproximateTime.currentTimeMillis())) {
            onAborted.run();
         } else {
            rq.execute(onResponse, onAborted);
         }
      };
      this.deliverLocallyInternal(request, consumer, callback);
   }

   private <P, Q> void deliverLocalResponse(Request<P, Q> request, Response<Q> response, MessageCallback<Q> callback) {
      this.addLatency(request.verb(), request.to(), request.lifetimeMillis());
      response.deliverTo(callback);
   }

   private <P, Q> void registerCallback(Request<P, Q> request, MessageCallback<Q> callback) {
      long startTime = request.operationStartMillis();
      long timeout = request.timeoutMillis();
      TracingAwareExecutor executor = request.responseExecutor();
      Iterator var8 = request.forwards().iterator();

      while(var8.hasNext()) {
         Request.Forward forward = (Request.Forward)var8.next();
         this.registerCallback(forward.id, forward.to, request.verb(), callback, startTime, timeout, executor);
      }

      this.registerCallback(request.id(), request.to(), request.verb(), callback, startTime, timeout, executor);
   }

   private <Q> void registerCallback(int id, InetAddress to, Verb<?, Q> type, MessageCallback<Q> callback, long startTimeMillis, long timeout, TracingAwareExecutor executor) {
      timeout += DatabaseDescriptor.getEndpointSnitch().getCrossDcRttLatency(to);
      CallbackInfo previous = (CallbackInfo)this.callbacks.put(Integer.valueOf(id), new CallbackInfo(to, callback, type, executor, startTimeMillis), timeout);

      assert previous == null : String.format("Callback already exists for id %d! (%s)", new Object[]{Integer.valueOf(id), previous});

   }

   public <Q> CompletableFuture<Q> sendSingleTarget(Request<?, Q> request) {
      SingleTargetCallback<Q> callback = new SingleTargetCallback();
      this.send((Request)request, callback);
      return callback;
   }

   void forward(ForwardRequest<?, ?> request) {
      this.sendInternal(request);
   }

   public void send(OneWayRequest<?> request) {
      if(request.isLocal()) {
         this.deliverLocallyOneWay(request);
      } else {
         this.sendRequest(request, (MessageCallback)null);
      }

   }

   public void send(OneWayRequest.Dispatcher<?> dispatcher) {
      Iterator var2 = dispatcher.remoteRequests().iterator();

      while(var2.hasNext()) {
         OneWayRequest<?> request = (OneWayRequest)var2.next();
         this.sendRequest(request, (MessageCallback)null);
      }

      if(dispatcher.hasLocalRequest()) {
         this.deliverLocallyOneWay(dispatcher.localRequest());
      }

   }

   private void deliverLocallyOneWay(OneWayRequest<?> request) {
      this.deliverLocallyInternal(request, (r) -> {
         r.execute(r.verb().EMPTY_RESPONSE_CONSUMER, () -> {
         });
      }, (MessageCallback)null);
   }

   void reply(Response<?> response) {
      Tracing.trace("Enqueuing {} response to {}", response.verb(), response.from());
      this.sendResponse(response);
   }

   private <P, Q> void sendRequest(Request<P, Q> request, MessageCallback<Q> callback) {
      this.messageInterceptors.interceptRequest(request, this::sendInternal, callback);
   }

   private <Q> void sendResponse(Response<Q> response) {
      this.messageInterceptors.intercept(response, this::sendInternal, (Consumer)null);
   }

   private CompletableFuture<Void> sendInternal(Message<?> message) {
      if(logger.isTraceEnabled()) {
         logger.trace("Sending {}", message);
      }

      return this.getConnection(message).thenAccept((cp) -> {
         if(!cp.enqueue(message)) {
            noSpamLogger.debug("Failed to send message to {}, the outbound queue rejected it.", new Object[]{message.to()});
            if(message.isRequest()) {
               CallbackInfo info = this.getRegisteredCallback(message.id(), true, message.to());
               if(info != null) {
                  info.callback.onFailure(((Request)message).respondWithFailure(RequestFailureReason.UNKNOWN));
               }
            }
         }

      });
   }

   private <P, Q, M extends Request<P, Q>> void deliverLocallyInternal(M request, Consumer<M> consumer, MessageCallback<Q> callback) {
      try {
         Consumer<M> delivery = (rq) -> {
            rq.requestExecutor().execute(() -> {
               consumer.accept(rq);
            }, ExecutorLocals.create());
         };
         this.messageInterceptors.interceptRequest(request, delivery, callback);
      } catch (Throwable var5) {
         logger.error("{} while locally processing {} request.", var5.getClass().getCanonicalName(), request.verb());
         logger.trace("Stacktrace: ", var5);
         if(!request.verb().isOneWay()) {
            instance().incrementDroppedMessages(request);
            callback.onFailure(request.respondWithFailure(RequestFailureReason.UNKNOWN));
         }
      }

   }

   <Q> CompletableFuture<Void> updateBackPressureOnSend(Request<?, Q> request) {
      return request.verb().supportsBackPressure() && DatabaseDescriptor.backPressureEnabled()?this.getConnectionPool(request.to()).thenAccept((cp) -> {
         if(cp != null) {
            BackPressureState backPressureState = cp.getBackPressureState();
            backPressureState.onRequestSent(request);
         }

      }):CompletableFuture.completedFuture(null);
   }

   CompletableFuture<Void> updateBackPressureOnReceive(InetAddress host, Verb<?, ?> verb, boolean timeout) {
      return verb.supportsBackPressure() && DatabaseDescriptor.backPressureEnabled()?this.getConnectionPool(host).thenAccept((cp) -> {
         if(cp != null) {
            BackPressureState backPressureState = cp.getBackPressureState();
            if(!timeout) {
               backPressureState.onResponseReceived();
            } else {
               backPressureState.onResponseTimeout();
            }
         }

      }):CompletableFuture.completedFuture(null);
   }

   public CompletableFuture<Void> applyBackPressure(Iterable<InetAddress> hosts, long timeoutInNanos) {
      if(DatabaseDescriptor.backPressureEnabled()) {
         Set<BackPressureState> states = new HashSet();
         CompletableFuture<Void> future = null;
         Iterator var6 = hosts.iterator();

         while(var6.hasNext()) {
            InetAddress host = (InetAddress)var6.next();
            if(!host.equals(FBUtilities.getBroadcastAddress())) {
               CompletableFuture<Void> next = this.getConnectionPool(host).thenAccept((cp) -> {
                  if(cp != null) {
                     states.add(cp.getBackPressureState());
                  }

               });
               if(future == null) {
                  future = next;
               } else {
                  future = future.thenAcceptBoth(next, (a, b) -> {
                  });
               }
            }
         }

         if(future != null) {
            return future.thenCompose((ignored) -> {
               return this.backPressure.apply(states, timeoutInNanos, TimeUnit.NANOSECONDS);
            });
         }
      }

      return CompletableFuture.completedFuture(null);
   }

   void addLatency(Verb<?, ?> verb, InetAddress address, long latency) {
      Iterator var5 = this.subscribers.iterator();

      while(var5.hasNext()) {
         ILatencySubscriber subscriber = (ILatencySubscriber)var5.next();
         subscriber.receiveTiming(verb, address, latency);
      }

   }

   public CompletableFuture<Void> convict(InetAddress ep) {
      return this.getConnectionPool(ep).thenAccept((cp) -> {
         if(cp != null) {
            logger.debug("Resetting pool for " + ep);
            cp.reset();
         } else {
            logger.debug("Not resetting pool for {} because internode authenticator said not to connect", ep);
         }

      });
   }

   public void listen() {
      this.callbacks.reset();
      this.listen(FBUtilities.getLocalAddress());
      if(DatabaseDescriptor.shouldListenOnBroadcastAddress() && !FBUtilities.getLocalAddress().equals(FBUtilities.getBroadcastAddress())) {
         this.listen(FBUtilities.getBroadcastAddress());
      }

      this.listenGate.signalAll();
   }

   private void listen(InetAddress localEp) throws ConfigurationException {
      Iterator var2 = this.getServerSockets(localEp).iterator();

      while(var2.hasNext()) {
         ServerSocket ss = (ServerSocket)var2.next();
         MessagingService.SocketThread th = new MessagingService.SocketThread(ss, "ACCEPT-" + localEp);
         th.start();
         this.socketThreads.add(th);
      }

   }

   private List<ServerSocket> getServerSockets(InetAddress localEp) throws ConfigurationException {
      List<ServerSocket> ss = new ArrayList(2);
      if(DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none) {
         try {
            ss.add(SSLFactory.getServerSocket(DatabaseDescriptor.getServerEncryptionOptions(), localEp, DatabaseDescriptor.getSSLStoragePort()));
         } catch (IOException var9) {
            throw new ConfigurationException("Unable to create ssl socket", var9);
         }

         logger.info("Starting Encrypted Messaging Service on SSL port {}", Integer.valueOf(DatabaseDescriptor.getSSLStoragePort()));
      }

      if(DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all) {
         ServerSocketChannel serverChannel = null;

         try {
            serverChannel = ServerSocketChannel.open();
         } catch (IOException var8) {
            throw new RuntimeException(var8);
         }

         ServerSocket socket = serverChannel.socket();

         try {
            socket.setReuseAddress(true);
         } catch (SocketException var7) {
            FileUtils.closeQuietly((Closeable)socket);
            throw new ConfigurationException("Insufficient permissions to setReuseAddress", var7);
         }

         InetSocketAddress address = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());

         try {
            socket.bind(address, 500);
         } catch (BindException var10) {
            FileUtils.closeQuietly((Closeable)socket);
            if(var10.getMessage().contains("in use")) {
               throw new ConfigurationException(address + " is in use by another process.  Change listen_address:storage_port in cassandra.yaml to values that do not conflict with other services");
            }

            if(var10.getMessage().contains("Cannot assign requested address")) {
               throw new ConfigurationException("Unable to bind to address " + address + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }

            throw new RuntimeException(var10);
         } catch (IOException var11) {
            FileUtils.closeQuietly((Closeable)socket);
            throw new RuntimeException(var11);
         }

         String nic = FBUtilities.getNetworkInterface(localEp);
         logger.info("Starting Messaging Service on {}:{}{}", new Object[]{localEp, Integer.valueOf(DatabaseDescriptor.getStoragePort()), nic == null?"":String.format(" (%s)", new Object[]{nic})});
         ss.add(socket);
      }

      return ss;
   }

   public void waitUntilListening() {
      try {
         this.listenGate.await();
      } catch (InterruptedException var2) {
         logger.trace("await interrupted");
      }

   }

   public boolean isListening() {
      return this.listenGate.isSignaled();
   }

   public void destroyConnectionPool(InetAddress to) {
      logger.trace("Destroy pool {}", to);
      OutboundTcpConnectionPool cp = (OutboundTcpConnectionPool)this.connectionManagers.get(to);
      if(cp != null) {
         cp.close();
         this.connectionManagers.remove(to);
      }
   }

   public CompletableFuture<OutboundTcpConnectionPool> getConnectionPool(InetAddress to) {
      OutboundTcpConnectionPool cp = (OutboundTcpConnectionPool)this.connectionManagers.get(to);
      return cp != null?(cp.isStarted()?CompletableFuture.completedFuture(cp):CompletableFuture.supplyAsync(() -> {
         cp.waitForStarted();
         return cp;
      })):CompletableFuture.supplyAsync(() -> {
         if(!DatabaseDescriptor.getInternodeAuthenticator().authenticate(to, OutboundTcpConnectionPool.portFor(to))) {
            return null;
         } else {
            InetAddress preferredIp = SystemKeyspace.getPreferredIP(to);
            OutboundTcpConnectionPool np = new OutboundTcpConnectionPool(to, preferredIp, this.backPressure.newState(to));
            OutboundTcpConnectionPool existingPool = (OutboundTcpConnectionPool)this.connectionManagers.putIfAbsent(to, np);
            if(existingPool != null) {
               np = existingPool;
            } else {
               np.start();
            }

            np.waitForStarted();
            return np;
         }
      });
   }

   public boolean hasValidIncomingConnections(InetAddress from, int minAgeInSeconds) {
      long now = System.nanoTime();
      Iterator var5 = this.socketThreads.iterator();

      while(true) {
         Collection sockets;
         do {
            if(!var5.hasNext()) {
               return false;
            }

            MessagingService.SocketThread socketThread = (MessagingService.SocketThread)var5.next();
            sockets = socketThread.connections.get(from);
         } while(sockets == null);

         Iterator var8 = sockets.iterator();

         while(var8.hasNext()) {
            Closeable socket = (Closeable)var8.next();
            if(socket instanceof IncomingTcpConnection && (now - ((IncomingTcpConnection)socket).getConnectTime() > TimeUnit.SECONDS.toNanos((long)minAgeInSeconds) || TimeUnit.NANOSECONDS.toSeconds(now - STARTUP_TIME) < (long)(minAgeInSeconds * 2))) {
               return true;
            }
         }
      }
   }

   public CompletableFuture<OutboundTcpConnection> getConnection(Message msg) {
      return this.getConnectionPool(msg.to()).thenApply((cp) -> {
         return cp == null?null:cp.getConnection(msg);
      });
   }

   public void register(ILatencySubscriber subcriber) {
      this.subscribers.add(subcriber);
   }

   public void clearCallbacksUnsafe() {
      this.callbacks.reset();
   }

   public void shutdown() {
      logger.info("Waiting for messaging service to quiesce");
      if(!this.callbacks.shutdownBlocking(DatabaseDescriptor.getMinRpcTimeout() * 2L)) {
         logger.warn("Failed to wait for messaging service callbacks shutdown");
      }

      try {
         Iterator var1 = this.socketThreads.iterator();

         while(var1.hasNext()) {
            MessagingService.SocketThread th = (MessagingService.SocketThread)var1.next();

            try {
               th.close();
            } catch (IOException var4) {
               handleIOExceptionOnClose(var4);
            }
         }

      } catch (IOException var5) {
         throw new IOError(var5);
      }
   }

   public void receive(Message<?> message) {
      this.messageInterceptors.intercept(message, this::receiveInternal, this::reply);
   }

   private void receiveInternal(Message<?> message) {
      TraceState state = Tracing.instance.initializeFromMessage(message);
      if(state != null) {
         state.trace("{} message received from {}", message.verb(), message.from());
      }

      ExecutorLocals locals = ExecutorLocals.create(state, ClientWarn.instance.getForMessage(message.id()));

      try {
         if(message.isRequest()) {
            this.receiveRequestInternal((Request)message, locals);
         } else {
            this.receiveResponseInternal((Response)message, locals);
         }
      } catch (Throwable var6) {
         logger.error("{} while receiving {} from {}, caused by: {}", new Object[]{var6.getClass().getCanonicalName(), message.verb(), message.from(), var6.getMessage()});
         logger.trace("Stacktrace: ", var6);
         if(message.isRequest() && !message.verb().isOneWay()) {
            instance().incrementDroppedMessages(message);
            Request<?, ?> request = (Request)message;
            this.reply(request.respondWithFailure(RequestFailureReason.UNKNOWN));
         }
      }

   }

   private <P, Q> void receiveRequestInternal(Request<P, Q> request, ExecutorLocals locals) {
      assert !TPC.isTPCThread();

      TracingAwareExecutor executor = request.requestExecutor();
      int coreId = executor.coreId();
      if(request.verb.supportsBackPressure() && TPC.isValidCoreId(coreId)) {
         try {
            TPCEventLoop eventLoop = (TPCEventLoop)TPC.eventLoopGroup().eventLoops().get(coreId);
            long retryTimeCapInMillis = request.operationStartMillis() + request.timeoutMillis() / 4L;
            CompletableFuture<RejectedExecutionException> backpressure = new CompletableFuture();
            this.checkTPCBackpressure(eventLoop, backpressure, retryTimeCapInMillis);
            RejectedExecutionException rejected = (RejectedExecutionException)backpressure.get();
            if(rejected != null) {
               throw rejected;
            }
         } catch (RejectedExecutionException var10) {
            throw var10;
         } catch (InterruptedException var11) {
            Thread.currentThread().interrupt();
         } catch (Throwable var12) {
            throw new IllegalStateException(var12);
         }
      }

      executor.execute(MessageDeliveryTask.forRequest(request), locals);
   }

   private <Q> void receiveResponseInternal(Response<Q> response, ExecutorLocals locals) {
      CallbackInfo<Q> info = this.getRegisteredCallback(response, false);
      if(info != null) {
         info.responseExecutor.execute(MessageDeliveryTask.forResponse(response), locals);
      }

   }

   private void checkTPCBackpressure(TPCEventLoop eventLoop, CompletableFuture<RejectedExecutionException> backpressure, long retryTimeCapInMillis) {
      boolean shouldBackpressure = eventLoop.shouldBackpressure(true);
      if(shouldBackpressure && ApproximateTime.currentTimeMillis() < retryTimeCapInMillis - 100L) {
         TPC.bestTPCTimer().onTimeout(() -> {
            this.checkTPCBackpressure(eventLoop, backpressure, retryTimeCapInMillis);
         }, 100L, TimeUnit.MILLISECONDS);
      } else if(shouldBackpressure) {
         backpressure.complete(new RejectedExecutionException("Too many pending remote requests!"));
      } else {
         backpressure.complete(null);
      }

   }

   CallbackInfo<?> getRegisteredCallback(int id, boolean remove, InetAddress from) {
      CallbackInfo<?> callback = remove?(CallbackInfo)this.callbacks.remove(Integer.valueOf(id)):(CallbackInfo)this.callbacks.get(Integer.valueOf(id));
      if(callback == null) {
         String msg = "Callback already removed for message {} from {}, ignoring response";
         logger.trace(msg, Integer.valueOf(id), from);
         Tracing.trace(msg, Integer.valueOf(id), from);
         return null;
      } else {
         return callback;
      }
   }

   <Q> CallbackInfo<Q> getRegisteredCallback(Response<Q> response, boolean remove) {
      return (CallbackInfo<Q>) this.getRegisteredCallback(response.id(), remove, response.from());
   }

   public static void validateMagic(int magic) throws IOException {
      if(magic != -900387334) {
         throw new IOException("invalid protocol header");
      }
   }

   public static int getBits(int packed, int start, int count) {
      return packed >>> start + 1 - count & ~(-1 << count);
   }

   public MessagingVersion setVersion(InetAddress endpoint, MessagingVersion version) {
      logger.trace("Setting version {} for {}", version, endpoint);
      MessagingVersion v = (MessagingVersion)this.versions.put(endpoint, version);
      return v == null?version:v;
   }

   public void resetVersion(InetAddress endpoint) {
      logger.trace("Resetting version for {}", endpoint);
      this.versions.remove(endpoint);
   }

   public Optional<MessagingVersion> getVersion(InetAddress endpoint) {
      return Optional.ofNullable(this.versions.get(endpoint));
   }

   public boolean versionAtLeast(InetAddress endpoint, MessagingVersion minVersion) {
      MessagingVersion v = (MessagingVersion)this.versions.get(endpoint);
      return v != null && v.compareTo(minVersion) >= 0;
   }

   /** @deprecated */
   @Deprecated
   public int getVersion(String endpoint) throws UnknownHostException {
      return ((MessagingVersion)this.getVersion(InetAddress.getByName(endpoint)).orElse(current_version)).protocolVersion().handshakeVersion;
   }

   void incrementDroppedMessages(Message<?> message) {
      Verb<?, ?> definition = message.verb();

      assert !definition.isOneWay() : "Shouldn't drop a one-way message";

      if(message.isRequest()) {
         Object payload = message.payload();
         if(payload instanceof IMutation) {
            this.updateDroppedMutationCount((IMutation)payload);
         }
      }

      this.droppedMessages.onDroppedMessage(message);
   }

   private void updateDroppedMutationCount(IMutation mutation) {
      assert mutation != null : "Mutation should not be null when updating dropped mutations count";

      Iterator var2 = mutation.getTableIds().iterator();

      while(var2.hasNext()) {
         TableId tableId = (TableId)var2.next();
         ColumnFamilyStore cfs = Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(tableId);
         if(cfs != null) {
            cfs.metric.droppedMutations.inc();
         }
      }

   }

   private static void handleIOExceptionOnClose(IOException e) throws IOException {
      if(FBUtilities.isMacOSX) {
         String var1 = e.getMessage();
         byte var2 = -1;
         switch(var1.hashCode()) {
         case -369062560:
            if(var1.equals("Unknown error: 316")) {
               var2 = 0;
            }
            break;
         case 641576986:
            if(var1.equals("No such file or directory")) {
               var2 = 1;
            }
         }

         switch(var2) {
         case 0:
         case 1:
            return;
         }
      }

      throw e;
   }

   public Map<String, Integer> getLargeMessagePendingTasks() {
      Map<String, Integer> pendingTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         pendingTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Integer.valueOf(((OutboundTcpConnectionPool)entry.getValue()).large().getPendingMessages()));
      }

      return pendingTasks;
   }

   public Map<String, Long> getLargeMessageCompletedTasks() {
      Map<String, Long> completedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         completedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).large().getCompletedMesssages()));
      }

      return completedTasks;
   }

   public Map<String, Long> getLargeMessageDroppedTasks() {
      Map<String, Long> droppedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         droppedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).large().getDroppedMessages()));
      }

      return droppedTasks;
   }

   public Map<String, Integer> getSmallMessagePendingTasks() {
      Map<String, Integer> pendingTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         pendingTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Integer.valueOf(((OutboundTcpConnectionPool)entry.getValue()).small().getPendingMessages()));
      }

      return pendingTasks;
   }

   public Map<String, Long> getSmallMessageCompletedTasks() {
      Map<String, Long> completedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         completedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).small().getCompletedMesssages()));
      }

      return completedTasks;
   }

   public Map<String, Long> getSmallMessageDroppedTasks() {
      Map<String, Long> droppedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         droppedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).small().getDroppedMessages()));
      }

      return droppedTasks;
   }

   public Map<String, Integer> getGossipMessagePendingTasks() {
      Map<String, Integer> pendingTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         pendingTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Integer.valueOf(((OutboundTcpConnectionPool)entry.getValue()).gossip().getPendingMessages()));
      }

      return pendingTasks;
   }

   public Map<String, Long> getGossipMessageCompletedTasks() {
      Map<String, Long> completedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         completedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).gossip().getCompletedMesssages()));
      }

      return completedTasks;
   }

   public Map<String, Long> getGossipMessageDroppedTasks() {
      Map<String, Long> droppedTasks = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         droppedTasks.put(((InetAddress)entry.getKey()).getHostAddress(), Long.valueOf(((OutboundTcpConnectionPool)entry.getValue()).gossip().getDroppedMessages()));
      }

      return droppedTasks;
   }

   public Map<String, Integer> getDroppedMessages() {
      return this.droppedMessages.getSnapshot();
   }

   public Map<DroppedMessages.Group, DroppedMessageMetrics> getDroppedMessagesWithAllMetrics() {
      return this.droppedMessages.getAllMetrics();
   }

   public long getTotalTimeouts() {
      return ConnectionMetrics.totalTimeouts.getCount();
   }

   public Map<String, Long> getTimeoutsPerHost() {
      Map<String, Long> result = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         String ip = ((InetAddress)entry.getKey()).getHostAddress();
         long recent = ((OutboundTcpConnectionPool)entry.getValue()).getTimeouts();
         result.put(ip, Long.valueOf(recent));
      }

      return result;
   }

   public Map<String, Double> getBackPressurePerHost() {
      Map<String, Double> map = new HashMap(this.connectionManagers.size());
      Iterator var2 = this.connectionManagers.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, OutboundTcpConnectionPool> entry = (Entry)var2.next();
         map.put(((InetAddress)entry.getKey()).getHostAddress(), Double.valueOf(((OutboundTcpConnectionPool)entry.getValue()).getBackPressureState().getBackPressureRateLimit()));
      }

      return map;
   }

   public void setBackPressureEnabled(boolean enabled) {
      DatabaseDescriptor.setBackPressureEnabled(enabled);
   }

   public boolean isBackPressureEnabled() {
      return DatabaseDescriptor.backPressureEnabled();
   }

   public static IPartitioner globalPartitioner() {
      return StorageService.instance.getTokenMetadata().partitioner;
   }

   public static void validatePartitioner(Collection<? extends AbstractBounds<?>> allBounds) {
      Iterator var1 = allBounds.iterator();

      while(var1.hasNext()) {
         AbstractBounds<?> bounds = (AbstractBounds)var1.next();
         validatePartitioner(bounds);
      }

   }

   public static void validatePartitioner(AbstractBounds<?> bounds) {
      if(globalPartitioner() != bounds.left.getPartitioner()) {
         throw new AssertionError(String.format("Partitioner in bounds serialization. Expected %s, was %s.", new Object[]{globalPartitioner().getClass().getName(), bounds.left.getPartitioner().getClass().getName()}));
      }
   }

   @VisibleForTesting
   public List<MessagingService.SocketThread> getSocketThreads() {
      return this.socketThreads;
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
      current_version = MessagingVersion.DSE_603;
      idGen = new AtomicInteger(0);
      STARTUP_TIME = System.nanoTime();
   }

   @VisibleForTesting
   public static class SocketThread extends Thread {
      private final ServerSocket server;
      @VisibleForTesting
      public final Multimap<InetAddress, Closeable> connections = Multimaps.synchronizedMultimap(HashMultimap.create());

      SocketThread(ServerSocket server, String name) {
         super(name);
         this.server = server;
      }

      public void run() {
         while(true) {
            if(!this.server.isClosed()) {
               Socket socket = null;

               try {
                  socket = this.server.accept();
                  if(!this.authenticate(socket)) {
                     MessagingService.logger.trace("remote failed to authenticate");
                     socket.close();
                     continue;
                  }

                  socket.setKeepAlive(true);
                  socket.setSoTimeout(10000);
                  DataInputStream in = new DataInputStream(socket.getInputStream());
                  MessagingService.validateMagic(in.readInt());
                  int header = in.readInt();
                  boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
                  ProtocolVersion version = ProtocolVersion.fromProtocolHeader(header);
                  MessagingService.logger.trace("Connection version {} from {}", version, socket.getInetAddress());
                  socket.setSoTimeout(0);
                  Thread thread = isStream?new IncomingStreamingConnection(version, socket, this.connections):new IncomingTcpConnection(version, MessagingService.getBits(header, 2, 1) == 1, socket, this.connections);
                  ((Thread)thread).start();
                  this.connections.put(socket.getInetAddress(), (Closeable)thread);
                  continue;
               } catch (AsynchronousCloseException var7) {
                  MessagingService.logger.trace("Asynchronous close seen by server thread");
               } catch (ClosedChannelException var8) {
                  MessagingService.logger.trace("MessagingService server thread already closed");
               } catch (SSLHandshakeException var9) {
                  MessagingService.logger.error("SSL handshake error for inbound connection from " + socket, var9);
                  FileUtils.closeQuietly((Closeable)socket);
                  continue;
               } catch (Throwable var10) {
                  MessagingService.logger.trace("Error reading the socket {}", socket, var10);
                  FileUtils.closeQuietly((Closeable)socket);
                  continue;
               }
            }

            MessagingService.logger.info("MessagingService has terminated the accept() thread");
            return;
         }
      }

      void close() throws IOException {
         MessagingService.logger.trace("Closing accept() thread");

         try {
            this.server.close();
         } catch (IOException var5) {
            MessagingService.handleIOExceptionOnClose(var5);
         }

         Multimap var1 = this.connections;
         synchronized(this.connections) {
            Iterator var2 = Lists.newArrayList(this.connections.values()).iterator();

            while(var2.hasNext()) {
               Closeable connection = (Closeable)var2.next();
               connection.close();
            }

         }
      }

      private boolean authenticate(Socket socket) {
         return DatabaseDescriptor.getInternodeAuthenticator().authenticate(socket.getInetAddress(), socket.getPort());
      }
   }

   private static class MSTestHandle {
      public static final MessagingService instance = new MessagingService(true);

      private MSTestHandle() {
      }
   }

   private static class MSHandle {
      public static final MessagingService instance = new MessagingService(false);

      private MSHandle() {
      }
   }
}
