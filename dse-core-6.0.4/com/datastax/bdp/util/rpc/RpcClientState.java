package com.datastax.bdp.util.rpc;

import java.net.InetSocketAddress;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

public class RpcClientState {
   public final AuthenticatedUser user;
   public final InetSocketAddress remoteAddress;

   public RpcClientState(AuthenticatedUser user, InetSocketAddress remoteAddress) {
      this.user = user;
      this.remoteAddress = remoteAddress;
   }

   public static RpcClientState fromClientState(ClientState clientState) {
      return new RpcClientState(clientState.getUser(), clientState.getRemoteAddress());
   }
}
