package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConnectionFactory implements StreamConnectionFactory {
   private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionFactory.class);
   private static final int MAX_CONNECT_ATTEMPTS = 3;

   public DefaultConnectionFactory() {
   }

   public Socket createConnection(InetAddress peer) throws IOException {
      int attempts = 0;

      while(true) {
         Socket socket = null;

         try {
            socket = OutboundTcpConnectionPool.newSocket(peer);
            socket.setKeepAlive(true);
            return socket;
         } catch (IOException var9) {
            if(socket != null) {
               socket.close();
            }

            ++attempts;
            if(attempts >= 3) {
               throw var9;
            }

            long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2.0D, (double)attempts);
            logger.warn("Failed attempt {} to connect to {}. Retrying in {} ms. ({})", new Object[]{Integer.valueOf(attempts), peer, Long.valueOf(waitms), var9.getMessage()});

            try {
               Thread.sleep(waitms);
            } catch (InterruptedException var8) {
               throw new IOException("interrupted", var8);
            }
         }
      }
   }
}
