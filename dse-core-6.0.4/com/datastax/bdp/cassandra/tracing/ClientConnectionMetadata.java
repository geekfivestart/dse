package com.datastax.bdp.cassandra.tracing;

import java.net.InetAddress;
import java.util.Objects;

public class ClientConnectionMetadata {
   public final String userId;
   public final InetAddress clientAddress;
   public final int clientPort;
   public final String connectionId;
   private final int hash;

   public ClientConnectionMetadata(InetAddress clientAddress, int clientPort, String userId) {
      this.clientAddress = clientAddress;
      this.clientPort = clientPort;
      this.userId = userId;
      this.connectionId = clientAddress.getHostAddress() + ":" + clientPort;
      this.hash = Objects.hash(new Object[]{this.connectionId, userId});
   }

   public int hashCode() {
      return this.hash;
   }

   public boolean equals(Object other) {
      if(other == this) {
         return true;
      } else if(!(other instanceof ClientConnectionMetadata)) {
         return false;
      } else {
         ClientConnectionMetadata otherCCM = (ClientConnectionMetadata)other;
         return Objects.equals(this.connectionId, otherCCM.connectionId) && Objects.equals(this.userId, otherCCM.userId);
      }
   }

   public String toString() {
      return String.format("ClientConnectionMetadata { 'connectionId':'%s', 'userId':'%s'}", new Object[]{this.connectionId, this.userId});
   }
}
