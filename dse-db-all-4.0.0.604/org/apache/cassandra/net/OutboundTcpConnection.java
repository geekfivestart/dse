package org.apache.cassandra.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.Checksum;
import javax.net.ssl.SSLHandshakeException;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscGrowableArrayQueue;
import org.jctools.queues.MessagePassingQueue.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundTcpConnection extends FastThreadLocalThread implements ParkedThreadsMonitor.MonitorableThread {
   private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);
   private static final NoSpamLogger nospamLogger;
   private static final String PREFIX = "cassandra.";
   private static final String INTRADC_TCP_NODELAY_PROPERTY = "cassandra.otc_intradc_tcp_nodelay";
   private static final boolean INTRADC_TCP_NODELAY;
   private static final String BUFFER_SIZE_PROPERTY = "cassandra.otc_buffer_size";
   private static final int BUFFER_SIZE;
   public static final int MAX_COALESCED_MESSAGES = 128;
   private static final int SOFT_MAX_QUEUE_SIZE_SMALL;
   private static final int SOFT_MAX_QUEUE_SIZE_LARGE;
   private volatile boolean isStopped;
   private volatile ParkedThreadsMonitor.MonitorableThread.ThreadState state;
   private Thread thread;
   private static final int OPEN_RETRY_DELAY = 100;
   public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;
   static final int LZ4_HASH_SEED = -1756908916;
   private final MessagePassingQueue<OutboundTcpConnection.QueuedMessage> backlog;
   private final int maxBackLogSize;
   private final AtomicInteger numBacklogMessages;
   private final OutboundTcpConnectionPool poolReference;
   private final CoalescingStrategies.CoalescingStrategy cs;
   private DataOutputStreamPlus out;
   private Socket socket;
   private volatile long completed;
   private final AtomicLong dropped;
   private volatile int currentMsgBufferCount;
   private final boolean isGossip;
   private volatile MessagingVersion targetVersion;
   private volatile Message.Serializer messageSerializer;

   private static int getMaxBackLogSize(Message.Kind kind) {
      switch(null.$SwitchMap$org$apache$cassandra$net$Message$Kind[kind.ordinal()]) {
      case 1:
         return 2147483647;
      case 2:
         return SOFT_MAX_QUEUE_SIZE_SMALL;
      case 3:
         return SOFT_MAX_QUEUE_SIZE_LARGE;
      default:
         throw new IllegalStateException("Unsupported message kind: " + kind);
      }
   }

   private static CoalescingStrategies.CoalescingStrategy newCoalescingStrategy(String displayName, OutboundTcpConnection owner) {
      return CoalescingStrategies.newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(), DatabaseDescriptor.getOtcCoalescingWindow(), owner, logger, displayName);
   }

   public OutboundTcpConnection(OutboundTcpConnectionPool pool, Message.Kind kind) {
      this(pool, kind.toString(), kind == Message.Kind.GOSSIP, getMaxBackLogSize(kind));
   }

   @VisibleForTesting
   OutboundTcpConnection(OutboundTcpConnectionPool pool, String name, boolean isGossip, int maxBackLogSize) {
      super("MessagingService-Outgoing-" + pool.endPoint() + "-" + name);
      this.isStopped = false;
      this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      this.backlog = new MpscGrowableArrayQueue(4096, 1073741824);
      this.numBacklogMessages = new AtomicInteger(0);
      this.dropped = new AtomicLong();
      this.currentMsgBufferCount = 0;
      this.poolReference = pool;
      this.isGossip = isGossip;
      this.maxBackLogSize = maxBackLogSize;
      this.cs = newCoalescingStrategy(pool.endPoint().getHostAddress(), this);
      this.targetVersion = (MessagingVersion)MessagingService.instance().getVersion(pool.endPoint()).orElse(MessagingService.current_version);
   }

   private static boolean isLocalDC(InetAddress targetHost) {
      return isLocalDC(DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost));
   }

   private static boolean isLocalDC(String remoteDC) {
      String localDC = DatabaseDescriptor.getLocalDataCenter();
      return remoteDC.equals(localDC) || DatabaseDescriptor.getEndpointSnitch().isDefaultDC(remoteDC);
   }

   public boolean enqueue(Message message) {
      if(message.isDroppable() && this.numBacklogMessages.get() >= this.maxBackLogSize) {
         return false;
      } else {
         boolean ret = this.backlog.relaxedOffer(new OutboundTcpConnection.QueuedMessage(message));

         assert ret : String.format("Dropped a message that should not have been dropped: %s, kind: %s", new Object[]{message.toString(), message.kind()});

         if(ret) {
            this.numBacklogMessages.incrementAndGet();
         }

         return ret;
      }
   }

   void closeSocket(boolean destroyThread) {
      logger.debug("Enqueuing socket close for {} with backlog size: {}", this.poolReference.endPoint(), this.numBacklogMessages);
      this.isStopped = destroyThread;
      int drained = this.backlog.drain((msg) -> {
      });
      this.numBacklogMessages.addAndGet(-drained);
      this.enqueue(Message.CLOSE_SENTINEL);
   }

   void softCloseSocket() {
      this.enqueue(Message.CLOSE_SENTINEL);
   }

   public ParkedThreadsMonitor.MonitorableThread.ThreadState getThreadState() {
      return this.state;
   }

   public void park() {
      this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED;
      LockSupport.park();
   }

   public void unpark() {
      assert this.thread != null;

      this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      LockSupport.unpark(this.thread);
   }

   public boolean shouldUnpark(long now) {
      return this.state == ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED && !this.backlog.isEmpty();
   }

   public void run() {
      if(this.thread == null) {
         this.thread = Thread.currentThread();
      }

      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addThreadToMonitor(this);
      int drainedMessageSize = true;
      ArrayList drainedMessages = new ArrayList(128);

      label69:
      while(!this.isStopped) {
         try {
            this.cs.coalesce(this.backlog, drainedMessages, 128);
         } catch (InterruptedException var8) {
            throw new AssertionError(var8);
         }

         int count = this.currentMsgBufferCount = drainedMessages.size();
         this.numBacklogMessages.addAndGet(-count);
         Iterator var4 = drainedMessages.iterator();

         while(var4.hasNext()) {
            OutboundTcpConnection.QueuedMessage qm = (OutboundTcpConnection.QueuedMessage)var4.next();

            try {
               Message m = qm.message;
               if(m == Message.CLOSE_SENTINEL) {
                  logger.trace("Disconnecting because CLOSE_SENTINEL detected");
                  this.disconnect();
                  if(this.isStopped) {
                     break label69;
                  }
                  continue;
               }

               if(m.isTimedOut(ApproximateTime.currentTimeMillis())) {
                  this.dropped.incrementAndGet();
               } else {
                  if(this.socket == null && !this.connect()) {
                     int drained = this.backlog.drain((msg) -> {
                     });
                     this.dropped.addAndGet((long)drained);
                     this.numBacklogMessages.addAndGet(-drained);
                     break;
                  }

                  this.writeConnected(qm, count == 1 && this.backlog.isEmpty());
               }
            } catch (OutboundTcpConnection.InternodeAuthFailed var9) {
               logger.warn("Internode auth failed connecting to {}", this.poolReference.endPoint());
               MessagingService.instance().destroyConnectionPool(this.poolReference.endPoint());
            } catch (Exception var10) {
               JVMStabilityInspector.inspectThrowable(var10);
               logger.error("error processing a message intended for {}", this.poolReference.endPoint(), var10);
            }

            --count;
            this.currentMsgBufferCount = count;
         }

         this.dropped.addAndGet((long)this.currentMsgBufferCount);
         drainedMessages.clear();
      }

      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).removeThreadToMonitor(this);
   }

   public int getPendingMessages() {
      return this.numBacklogMessages.get() + this.currentMsgBufferCount;
   }

   public long getCompletedMesssages() {
      return this.completed;
   }

   public long getDroppedMessages() {
      return this.dropped.get();
   }

   private static boolean shouldCompressConnection(InetAddress endpoint) {
      switch(null.$SwitchMap$org$apache$cassandra$config$Config$InternodeCompression[DatabaseDescriptor.internodeCompression().ordinal()]) {
      case 1:
         return false;
      case 2:
         return true;
      case 3:
         return !isLocalDC(endpoint);
      default:
         throw new AssertionError("internode-compression " + DatabaseDescriptor.internodeCompression());
      }
   }

   public static boolean shouldCompressConnection(String dc) {
      switch(null.$SwitchMap$org$apache$cassandra$config$Config$InternodeCompression[DatabaseDescriptor.internodeCompression().ordinal()]) {
      case 1:
         return false;
      case 2:
         return true;
      case 3:
         return !isLocalDC(dc);
      default:
         throw new AssertionError("internode-compression " + DatabaseDescriptor.internodeCompression());
      }
   }

   private void writeConnected(OutboundTcpConnection.QueuedMessage qm, boolean flush) {
      try {
         long serializedSize = this.messageSerializer.serializedSize(qm.message);

         assert serializedSize <= 2147483647L : "Invalid message, too large: " + serializedSize;

         int messageSize = (int)serializedSize;
         Tracing.instance.onMessageSend(qm.message, messageSize);
         this.messageSerializer.writeSerializedSize(messageSize, this.out);
         this.messageSerializer.serialize(qm.message, this.out);
         ++this.completed;
         if(flush || qm.message.verb() == Verbs.GOSSIP.ECHO) {
            this.out.flush();
         }
      } catch (Throwable var6) {
         JVMStabilityInspector.inspectThrowable(var6);
         this.disconnect();
         if(!(var6 instanceof IOException) && !(var6.getCause() instanceof IOException)) {
            logger.error("error writing to {}", this.poolReference.endPoint(), var6);
         } else {
            logger.debug("Error writing to {}", this.poolReference.endPoint(), var6);
            if(qm.shouldRetry()) {
               boolean accepted = this.backlog.relaxedOffer(new OutboundTcpConnection.RetriedQueuedMessage(qm));

               assert accepted;

               this.numBacklogMessages.incrementAndGet();
            }
         }
      }

   }

   public boolean isSocketOpen() {
      return this.socket != null && this.socket.isConnected();
   }

   private void disconnect() {
      if(this.socket != null) {
         try {
            if(this.out != null) {
               this.out.flush();
            }
         } catch (IOException var3) {
            if(logger.isTraceEnabled()) {
               logger.trace("exception flushing output stream before closing connection to " + this.poolReference.endPoint(), var3);
            }
         }

         try {
            this.socket.close();
            logger.debug("Socket to {} closed", this.poolReference.endPoint());
         } catch (IOException var2) {
            logger.debug("Exception closing connection to {}", this.poolReference.endPoint(), var2);
         }

         this.out = null;
         this.socket = null;
      }

   }

   private boolean connect() throws OutboundTcpConnection.InternodeAuthFailed {
      InetAddress endpoint = this.poolReference.endPoint();
      IInternodeAuthenticator var10000 = DatabaseDescriptor.getInternodeAuthenticator();
      OutboundTcpConnectionPool var10002 = this.poolReference;
      if(!var10000.authenticate(endpoint, OutboundTcpConnectionPool.portFor(endpoint))) {
         throw new OutboundTcpConnection.InternodeAuthFailed(null);
      } else {
         logger.debug("Attempting to connect to {}", endpoint);
         long start = System.nanoTime();
         long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());

         while(true) {
            if(System.nanoTime() - start < timeout) {
               this.targetVersion = (MessagingVersion)MessagingService.instance().getVersion(endpoint).orElse(MessagingService.current_version);
               boolean success = false;

               boolean var32;
               try {
                  this.socket = this.poolReference.newSocket();
                  this.socket.setKeepAlive(true);
                  if(!isLocalDC(endpoint) && !this.isGossip) {
                     this.socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                  } else {
                     this.socket.setTcpNoDelay(INTRADC_TCP_NODELAY);
                  }

                  if(DatabaseDescriptor.getInternodeSendBufferSize() > 0) {
                     try {
                        this.socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                     } catch (SocketException var24) {
                        logger.warn("Failed to set send buffer size on internode socket.", var24);
                     }
                  }

                  WritableByteChannel ch = this.socket.getChannel();
                  this.out = new BufferedDataOutputStreamPlus((WritableByteChannel)(ch != null?ch:Channels.newChannel(this.socket.getOutputStream())), BUFFER_SIZE);
                  ProtocolVersion targetProtocolVersion = this.targetVersion.protocolVersion();
                  boolean compress = shouldCompressConnection(this.poolReference.endPoint());
                  this.out.writeInt(-900387334);
                  this.out.writeInt(targetProtocolVersion.makeProtocolHeader(compress, false));
                  this.out.flush();
                  DataInputStream in = new DataInputStream(this.socket.getInputStream());
                  ProtocolVersion maxTargetVersion = this.handshakeVersion(this.socket, in);
                  if(maxTargetVersion == null) {
                     logger.trace("Target max version is {}; no version information yet, will retry", maxTargetVersion);
                     this.disconnect();
                     continue;
                  }

                  MessagingService.instance().setVersion(endpoint, MessagingVersion.from(maxTargetVersion));
                  if(targetProtocolVersion.compareTo(maxTargetVersion) > 0) {
                     logger.trace("Target max version is {}; will reconnect with that version", maxTargetVersion);

                     try {
                        if(DatabaseDescriptor.getSeeds().contains(endpoint)) {
                           logger.warn("Seed gossip version is {}; will not connect with that version", maxTargetVersion);
                        }
                     } catch (Throwable var23) {
                        JVMStabilityInspector.inspectThrowable(var23);
                        logger.warn("Configuration error prevented outbound connection: {}", var23.getLocalizedMessage());
                     }

                     boolean var30 = false;
                     return var30;
                  }

                  if(targetProtocolVersion.compareTo(maxTargetVersion) < 0 && this.targetVersion != MessagingService.current_version) {
                     logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done", maxTargetVersion, targetProtocolVersion);
                     this.softCloseSocket();
                  }

                  long baseTimestampMillis = System.currentTimeMillis();
                  this.messageSerializer = Message.createSerializer(this.targetVersion, baseTimestampMillis);
                  this.out.writeInt(MessagingService.current_version.protocolVersion().handshakeVersion);
                  CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), this.out);
                  if(this.targetVersion.isDSE()) {
                     MessageParameters connectionParameters = MessageParameters.builder().putLong("BASE_TIMESTAMP", baseTimestampMillis).build();
                     MessageParameters.serializer().serialize(connectionParameters, this.out);
                  }

                  if(compress) {
                     this.out.flush();
                     logger.trace("Upgrading OutputStream to {} to be compressed", endpoint);
                     LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                     Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(-1756908916).asChecksum();
                     this.out = new WrappedDataOutputStreamPlus(new LZ4BlockOutputStream(this.socket.getOutputStream(), 16384, compressor, checksum, true));
                  }

                  logger.debug("Done connecting to {}", endpoint);
                  success = true;
                  var32 = true;
               } catch (SSLHandshakeException var25) {
                  logger.error("SSL handshake error for outbound connection to " + this.socket, var25);
                  this.disconnect();
                  boolean var8 = false;
                  return var8;
               } catch (ConnectException var26) {
                  this.disconnect();
                  nospamLogger.debug(String.format("Unable to connect to %s (%s)", new Object[]{this.poolReference.endPoint(), var26.toString()}), new Object[0]);
                  Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
                  continue;
               } catch (IOException var27) {
                  this.disconnect();
                  logger.debug("unable to connect to " + this.poolReference.endPoint(), var27);
                  Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
                  continue;
               } finally {
                  if(!success) {
                     this.disconnect();
                  }

               }

               return var32;
            }

            return false;
         }
      }
   }

   private ProtocolVersion handshakeVersion(Socket socket, DataInputStream in) throws IOException {
      int oldTimeout = socket.getSoTimeout();

      ProtocolVersion var4;
      try {
         socket.setSoTimeout(5000);
         var4 = ProtocolVersion.fromHandshakeVersion(in.readInt());
      } finally {
         socket.setSoTimeout(oldTimeout);
      }

      return var4;
   }

   @VisibleForTesting
   MessagePassingQueue<OutboundTcpConnection.QueuedMessage> backlog() {
      return this.backlog;
   }

   @VisibleForTesting
   void drain(Consumer<OutboundTcpConnection.QueuedMessage> consumer, int length) {
      int drained = this.backlog.drain(consumer, length);
      this.numBacklogMessages.addAndGet(-drained);
   }

   static {
      nospamLogger = NoSpamLogger.getLogger(logger, 10L, TimeUnit.SECONDS);
      INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty("cassandra.otc_intradc_tcp_nodelay", "true"));
      BUFFER_SIZE = Integer.getInteger("cassandra.otc_buffer_size", 65536).intValue();
      SOFT_MAX_QUEUE_SIZE_SMALL = Integer.parseInt(System.getProperty("dse.outbound.connection.small.queue.max", "32768"));
      SOFT_MAX_QUEUE_SIZE_LARGE = Integer.parseInt(System.getProperty("dse.outbound.connection.large.queue.max", "4096"));
      if(SOFT_MAX_QUEUE_SIZE_SMALL <= 128) {
         throw new IllegalStateException("SOFT_MAX_QUEUE_SIZE_SMALL should be at least 128, got: " + Integer.toString(SOFT_MAX_QUEUE_SIZE_SMALL));
      } else if(SOFT_MAX_QUEUE_SIZE_LARGE <= 32) {
         throw new IllegalStateException("SOFT_MAX_QUEUE_SIZE_LARGE should be at least 32, got: " + Integer.toString(SOFT_MAX_QUEUE_SIZE_LARGE));
      } else {
         String strategy = DatabaseDescriptor.getOtcCoalescingStrategy();
         byte var2 = -1;
         switch(strategy.hashCode()) {
         case -2005403122:
            if(strategy.equals("TIMEHORIZON")) {
               var2 = 0;
            }
            break;
         case -864683537:
            if(strategy.equals("MOVINGAVERAGE")) {
               var2 = 1;
            }
            break;
         case 66907988:
            if(strategy.equals("FIXED")) {
               var2 = 2;
            }
            break;
         case 1053567612:
            if(strategy.equals("DISABLED")) {
               var2 = 3;
            }
         }

         switch(var2) {
         case 0:
            break;
         case 1:
         case 2:
         case 3:
            logger.info("OutboundTcpConnection using coalescing strategy {}", strategy);
            break;
         default:
            newCoalescingStrategy("dummy", (OutboundTcpConnection)null);
         }

         int coalescingWindow = DatabaseDescriptor.getOtcCoalescingWindow();
         if(coalescingWindow != 200) {
            logger.info("OutboundTcpConnection coalescing window set to {}μs", Integer.valueOf(coalescingWindow));
         }

         if(coalescingWindow < 0) {
            throw new ExceptionInInitializerError("Value provided for coalescing window must be greater than 0: " + coalescingWindow);
         }
      }
   }

   private static class InternodeAuthFailed extends Exception {
      private InternodeAuthFailed() {
      }
   }

   private static class RetriedQueuedMessage extends OutboundTcpConnection.QueuedMessage {
      RetriedQueuedMessage(OutboundTcpConnection.QueuedMessage msg) {
         super(msg.message);
      }

      boolean shouldRetry() {
         return false;
      }
   }

   static class QueuedMessage implements CoalescingStrategies.Coalescable {
      final Message message;
      final long timestampNanos;

      QueuedMessage(Message message) {
         this.message = message;
         this.timestampNanos = ApproximateTime.nanoTime();
      }

      boolean shouldRetry() {
         return true;
      }

      public long timestampNanos() {
         return this.timestampNanos;
      }
   }
}
