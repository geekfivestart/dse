package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundTcpConnectionPool {
   private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnectionPool.class);
   public static final long LARGE_MESSAGE_THRESHOLD = Long.getLong("cassandra.otcp_large_message_threshold", 65536L).longValue();
   private final InetAddress id;
   private final CountDownLatch started;
   private final EnumMap<Message.Kind, OutboundTcpConnection> connectionByKind;
   private InetAddress resetEndpoint;
   private ConnectionMetrics metrics;
   private final BackPressureState backPressureState;

   OutboundTcpConnectionPool(InetAddress remoteEp, InetAddress preferredIp, BackPressureState backPressureState) {
      this.id = remoteEp;
      this.resetEndpoint = preferredIp;
      this.started = new CountDownLatch(1);
      this.connectionByKind = new EnumMap(Message.Kind.class);
      Message.Kind[] var4 = Message.Kind.values();
      int var5 = var4.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         Message.Kind kind = var4[var6];
         this.connectionByKind.put(kind, new OutboundTcpConnection(this, kind));
      }

      this.backPressureState = backPressureState;
   }

   OutboundTcpConnection getConnection(Message msg) {
      return (OutboundTcpConnection)this.connectionByKind.get(msg.kind());
   }

   public BackPressureState getBackPressureState() {
      return this.backPressureState;
   }

   public OutboundTcpConnection large() {
      return (OutboundTcpConnection)this.connectionByKind.get(Message.Kind.LARGE);
   }

   public OutboundTcpConnection small() {
      return (OutboundTcpConnection)this.connectionByKind.get(Message.Kind.SMALL);
   }

   public OutboundTcpConnection gossip() {
      return (OutboundTcpConnection)this.connectionByKind.get(Message.Kind.GOSSIP);
   }

   void reset() {
      logger.trace("Reset called for {}", this.id);
      Iterator var1 = this.connectionByKind.values().iterator();

      while(var1.hasNext()) {
         OutboundTcpConnection conn = (OutboundTcpConnection)var1.next();
         conn.closeSocket(false);
      }

   }

   public void softReset() {
      logger.trace("Soft reset called for {}", this.id);
      Iterator var1 = this.connectionByKind.values().iterator();

      while(var1.hasNext()) {
         OutboundTcpConnection conn = (OutboundTcpConnection)var1.next();
         conn.softCloseSocket();
      }

   }

   public void reset(InetAddress remoteEP) {
      logger.trace("Reset called for {} remoteEP {}", this.id, remoteEP);
      this.resetEndpoint = remoteEP;
      Iterator var2 = this.connectionByKind.values().iterator();

      while(var2.hasNext()) {
         OutboundTcpConnection conn = (OutboundTcpConnection)var2.next();
         conn.softCloseSocket();
      }

      this.metrics.release();
      this.metrics = new ConnectionMetrics(this.resetEndpoint, this);
   }

   public long getTimeouts() {
      return this.metrics.timeouts.getCount();
   }

   public void incrementTimeout() {
      this.metrics.timeouts.mark();
   }

   public Socket newSocket() throws IOException {
      return newSocket(this.endPoint());
   }

   public static Socket newSocket(InetAddress endpoint) throws IOException {
      if(isEncryptedChannel(endpoint)) {
         return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint, DatabaseDescriptor.getSSLStoragePort());
      } else {
         SocketChannel channel = SocketChannel.open();
         channel.connect(new InetSocketAddress(endpoint, DatabaseDescriptor.getStoragePort()));
         return channel.socket();
      }
   }

   public static int portFor(InetAddress endpoint) {
      return isEncryptedChannel(endpoint)?DatabaseDescriptor.getSSLStoragePort():DatabaseDescriptor.getStoragePort();
   }

   public InetAddress endPoint() {
      return this.id.equals(FBUtilities.getBroadcastAddress())?FBUtilities.getLocalAddress():this.resetEndpoint;
   }

   public static boolean isEncryptedChannel(InetAddress address) {
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      switch(null.$SwitchMap$org$apache$cassandra$config$EncryptionOptions$ServerEncryptionOptions$InternodeEncryption[DatabaseDescriptor.getServerEncryptionOptions().internode_encryption.ordinal()]) {
      case 1:
         return false;
      case 2:
      default:
         break;
      case 3:
         if(snitch.isInLocalDatacenter(address)) {
            return false;
         }
         break;
      case 4:
         if(snitch.isInLocalRack(address)) {
            return false;
         }
      }

      return true;
   }

   public void start() {
      Iterator var1 = this.connectionByKind.values().iterator();

      while(var1.hasNext()) {
         OutboundTcpConnection connection = (OutboundTcpConnection)var1.next();
         connection.start();
      }

      this.metrics = new ConnectionMetrics(this.id, this);
      this.started.countDown();
   }

   public boolean isStarted() {
      return this.started.getCount() == 0L;
   }

   public void waitForStarted() {
      if(!this.isStarted()) {
         boolean error = false;

         try {
            if(!this.started.await(1L, TimeUnit.MINUTES)) {
               error = true;
            }
         } catch (InterruptedException var3) {
            Thread.currentThread().interrupt();
            error = true;
         }

         if(error) {
            throw new IllegalStateException(String.format("Connections to %s are not started!", new Object[]{this.id.getHostAddress()}));
         }
      }
   }

   public void close() {
      Iterator var1 = this.connectionByKind.values().iterator();

      while(var1.hasNext()) {
         OutboundTcpConnection connection = (OutboundTcpConnection)var1.next();
         connection.closeSocket(true);
      }

      if(this.metrics != null) {
         this.metrics.release();
      }

   }
}
