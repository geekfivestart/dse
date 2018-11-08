package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.rmi.server.RMIServerSocketFactory;
import javax.net.ServerSocketFactory;

public class RMIServerSocketFactoryImpl implements RMIServerSocketFactory {
   private final InetAddress bindAddress;

   public RMIServerSocketFactoryImpl(InetAddress bindAddress) {
      this.bindAddress = bindAddress;
   }

   public ServerSocket createServerSocket(int pPort) throws IOException {
      ServerSocket socket = ServerSocketFactory.getDefault().createServerSocket(pPort, 0, this.bindAddress);

      try {
         socket.setReuseAddress(true);
         return socket;
      } catch (SocketException var4) {
         socket.close();
         throw var4;
      }
   }

   public boolean equals(Object obj) {
      return obj == null?false:(obj == this?true:obj.getClass().equals(this.getClass()));
   }

   public int hashCode() {
      return RMIServerSocketFactoryImpl.class.hashCode();
   }
}
