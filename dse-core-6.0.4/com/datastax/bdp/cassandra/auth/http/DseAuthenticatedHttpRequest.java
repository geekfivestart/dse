package com.datastax.bdp.cassandra.auth.http;

import com.datastax.bdp.cassandra.auth.DsePrincipal;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.apache.cassandra.service.ClientState;

class DseAuthenticatedHttpRequest extends HttpServletRequestWrapper {
   private ClientState clientState;

   public DseAuthenticatedHttpRequest(HttpServletRequest request, ClientState clientState) {
      super(request);
      this.clientState = clientState;
   }

   public String getRemoteUser() {
      return this.clientState.getUser().getName();
   }

   public Principal getUserPrincipal() {
      return new DsePrincipal(this.clientState);
   }

   public String getAuthType() {
      return "BASIC";
   }
}
