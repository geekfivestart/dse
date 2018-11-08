package com.datastax.bdp.cassandra.auth;

import java.security.Principal;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

public class DsePrincipal implements Principal {
   private final ClientState clientState;

   public DsePrincipal(ClientState clientState) {
      this.clientState = clientState;
   }

   public String getName() {
      AuthenticatedUser user = this.clientState.getUser();
      return user != null?user.getName():null;
   }

   public ClientState getClientState() {
      return this.clientState;
   }
}
