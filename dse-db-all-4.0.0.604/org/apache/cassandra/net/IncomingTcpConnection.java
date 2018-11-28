package org.apache.cassandra.net;

import com.google.common.collect.Multimap;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.Checksum;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.NIODataInputStream;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingTcpConnection extends FastThreadLocalThread implements Closeable {
   private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);
   private static final int BUFFER_SIZE = Integer.getInteger("cassandra..itc_buffer_size", 65536).intValue();
   private long connectTime = 0L;
   private final ProtocolVersion protocolVersion;
   private final boolean compressed;
   private final Socket socket;
   private final Multimap<InetAddress, Closeable> group;
   public final InetAddress socketFrom;
   private Message.Serializer messageSerializer;
   public InetAddress snitchFrom;

   public IncomingTcpConnection(ProtocolVersion version, boolean compressed, Socket socket, Multimap<InetAddress, Closeable> group) {
      super("MessagingService-Incoming-" + socket.getInetAddress());
      this.protocolVersion = version;
      this.compressed = compressed;
      this.socket = socket;
      this.socketFrom = socket.getInetAddress();
      this.group = group;
      if(DatabaseDescriptor.getInternodeRecvBufferSize() > 0) {
         try {
            this.socket.setReceiveBufferSize(DatabaseDescriptor.getInternodeRecvBufferSize());
         } catch (SocketException var6) {
            logger.warn("Failed to set receive buffer size on internode socket.", var6);
         }
      }

   }

   public void run() {
      try {
         if(MessagingVersion.from(this.protocolVersion) == null) {
            throw new UnsupportedOperationException(String.format("Unable to read obsolete message version %s; The earliest version supported is %s", new Object[]{Integer.valueOf(this.protocolVersion.handshakeVersion), MessagingVersion.values()[0]}));
         }

         if(MessagingVersion.from(this.protocolVersion) == MessagingVersion.OSS_40) {
            throw new UnsupportedOperationException(String.format("Received messaging version %d (OSS C* 4.0) is not supported. Terminating connection.", new Object[]{Integer.valueOf(this.protocolVersion.handshakeVersion)}));
         }

         this.receiveMessages();
      } catch (IOError | Exception var5) {
         logger.trace("{} reading from socket (cause: {}); closing", new Object[]{var5.getClass().getSimpleName(), var5.getCause(), var5});
      } finally {
         this.close();
      }

   }

   public void close() {
      try {
         if(logger.isTraceEnabled()) {
            logger.trace("Closing socket {} - isclosed: {}", this.socket, Boolean.valueOf(this.socket.isClosed()));
         }

         if(!this.socket.isClosed()) {
            this.socket.close();
         }
      } catch (IOException var5) {
         logger.trace("Error closing socket", var5);
      } finally {
         this.group.remove(this.socketFrom, this);
         if(this.snitchFrom != null) {
            this.group.remove(this.snitchFrom, this);
         }

      }

   }

   private void receiveMessages() throws IOException {
      DataOutputStream out = new DataOutputStream(this.socket.getOutputStream());
      out.writeInt(MessagingService.current_version.protocolVersion().handshakeVersion);
      out.flush();
      DataInputPlus in = new DataInputPlus.DataInputStreamPlus(this.socket.getInputStream());
      ProtocolVersion maxVersion = ProtocolVersion.fromHandshakeVersion(in.readInt());

      assert this.protocolVersion.compareTo(MessagingService.current_version.protocolVersion()) <= 0;

      this.snitchFrom = CompactEndpointSerializationHelper.deserialize(in);
      MessagingService.instance().setVersion(this.snitchFrom, MessagingVersion.from(maxVersion));
      logger.trace("Set version for {} to {} (will use {})", new Object[]{this.snitchFrom, maxVersion, MessagingService.instance().getVersion(this.snitchFrom)});
      this.group.put(this.snitchFrom, this);
      MessagingVersion version = MessagingVersion.from(this.protocolVersion);
      long baseTimestampMillis = -1L;
      if(version.isDSE()) {
         MessageParameters connectionParameters = MessageParameters.serializer().deserialize(in);

         assert connectionParameters.has("BASE_TIMESTAMP");

         baseTimestampMillis = connectionParameters.getLong("BASE_TIMESTAMP").longValue();
      }

      this.messageSerializer = Message.createSerializer(version, baseTimestampMillis);
      if(this.compressed) {
         logger.trace("Upgrading incoming connection to be compressed");
         LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
         Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(-1756908916).asChecksum();
         in = new DataInputPlus.DataInputStreamPlus(new LZ4BlockInputStream(this.socket.getInputStream(), decompressor, checksum));
      } else {
         ReadableByteChannel channel = this.socket.getChannel();
         in = new NIODataInputStream((ReadableByteChannel)(channel != null?channel:Channels.newChannel(this.socket.getInputStream())), BUFFER_SIZE);
      }

      this.connectTime = System.nanoTime();

      while(true) {
         this.receiveMessage(new TrackedDataInputPlus((DataInput)in));
      }
   }

   private void receiveMessage(TrackedDataInputPlus input) throws IOException {
      int size = this.messageSerializer.readSerializedSize(input);

      try {
         Message message = this.messageSerializer.deserialize(input, size, this.snitchFrom);
         if(message == null) {
            return;
         }

         MessagingService.instance().receive(message);
      } catch (MessageDeserializationException var4) {
         var4.maybeSendErrorResponse();
         if(!var4.isRecoverable()) {
            throw var4;
         }

         var4.recover(input);
      }

   }

   public long getConnectTime() {
      return this.connectTime;
   }
}
