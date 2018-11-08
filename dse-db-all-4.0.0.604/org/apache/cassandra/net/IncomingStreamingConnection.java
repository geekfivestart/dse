package org.apache.cassandra.net;

import com.google.common.collect.Multimap;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingStreamingConnection extends Thread implements Closeable {
   private static final Logger logger = LoggerFactory.getLogger(IncomingStreamingConnection.class);
   private final ProtocolVersion version;
   public final Socket socket;
   private final InetAddress from;
   private final Multimap<InetAddress, Closeable> group;

   public IncomingStreamingConnection(ProtocolVersion version, Socket socket, Multimap<InetAddress, Closeable> group) {
      super("STREAM-INIT-" + socket.getRemoteSocketAddress());
      this.version = version;
      this.socket = socket;
      this.from = socket.getInetAddress();
      this.group = group;
   }

   public void run() {
      try {
         StreamMessage.StreamVersion streamVersion = StreamMessage.StreamVersion.of(this.version);
         if(streamVersion == null) {
            throw new IOException(String.format("Received stream using protocol version %d (my version %d). Terminating connection", new Object[]{Integer.valueOf(this.version.handshakeVersion), Integer.valueOf(StreamMessage.CURRENT_VERSION.protocolVersion.handshakeVersion)}));
         }

         if(streamVersion == StreamMessage.StreamVersion.OSS_40) {
            throw new IOException(String.format("Received stream using protocol version %d (OSS C* 4.0) is not supported. Terminating connection.", new Object[]{Integer.valueOf(this.version.handshakeVersion)}));
         }

         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(this.socket.getInputStream());
         StreamInitMessage init = (StreamInitMessage)((Serializer)StreamInitMessage.serializers.get(streamVersion)).deserialize(input);
         StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.streamOperation, init.from, this, init.isForOutgoing, streamVersion, init.keepSSTableLevel, init.pendingRepair, init.previewKind);
      } catch (Throwable var4) {
         logger.error("Error while reading from socket from {}.", this.socket.getRemoteSocketAddress(), var4);
         this.close();
      }

   }

   public void close() {
      try {
         if(!this.socket.isClosed()) {
            this.socket.close();
         }
      } catch (IOException var5) {
         logger.debug("Error closing socket", var5);
      } finally {
         this.group.remove(this.from, this);
      }

   }
}
