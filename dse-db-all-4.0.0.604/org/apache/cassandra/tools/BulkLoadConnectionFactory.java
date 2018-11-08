package org.apache.cassandra.tools;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class BulkLoadConnectionFactory implements StreamConnectionFactory {
   private final boolean outboundBindAny;
   private final int storagePort;
   private final int secureStoragePort;
   private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

   public BulkLoadConnectionFactory(int storagePort, int secureStoragePort, EncryptionOptions.ServerEncryptionOptions encryptionOptions, boolean outboundBindAny) {
      this.storagePort = storagePort;
      this.secureStoragePort = secureStoragePort;
      this.encryptionOptions = encryptionOptions;
      this.outboundBindAny = outboundBindAny;
   }

   public Socket createConnection(InetAddress peer) throws IOException {
      if(this.encryptionOptions != null && this.encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none) {
         return this.outboundBindAny?SSLFactory.getSocket(this.encryptionOptions, peer, this.secureStoragePort):SSLFactory.getSocket(this.encryptionOptions, peer, this.secureStoragePort, FBUtilities.getLocalAddress(), 0);
      } else {
         Socket socket = SocketChannel.open(new InetSocketAddress(peer, this.storagePort)).socket();
         if(this.outboundBindAny && !socket.isBound()) {
            socket.bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
         }

         return socket;
      }
   }
}
