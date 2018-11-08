package com.datastax.bdp.cassandra.auth.http;

import com.cloudera.alfredo.client.KerberosAuthenticator;
import com.cloudera.alfredo.server.AuthenticationFilter;
import com.datastax.bdp.transport.server.KerberosServerUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseHttpKerberosAuthenticationFilter extends AuthenticationFilter {
   private static final Logger logger = LoggerFactory.getLogger(DseHttpKerberosAuthenticationFilter.class);

   public DseHttpKerberosAuthenticationFilter() {
   }

   public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
      super.doFilter(request, response, new DseHttpKerberosAuthenticationFilter.FilterChainWrapper(filterChain));
   }

   private static class FilterChainWrapper implements FilterChain {
      private final FilterChain underlyingFilterChain;

      private FilterChainWrapper(FilterChain underlyingFilterChain) {
         this.underlyingFilterChain = underlyingFilterChain;
      }

      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse) throws IOException, ServletException {
         HttpServletRequest request = (HttpServletRequest)servletRequest;
         HttpServletResponse response = (HttpServletResponse)servletResponse;
         if(!request.getMethod().toUpperCase().equals(KerberosAuthenticator.getAuthHttpMethod())) {
            if(!DseAuthenticationFilter.isTrustedDseNode(request)) {
               try {
                  AuthenticatedUser user = KerberosServerUtils.getUserFromAuthzId(request.getUserPrincipal().getName());
                  ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved(request.getRemoteAddr(), request.getRemotePort()), (Connection)null);
                  TPCUtils.blockingGet(clientState.login(user));
                  servletRequest = new DseAuthenticatedHttpRequest(request, clientState);
               } catch (AuthenticationException var7) {
                  DseHttpKerberosAuthenticationFilter.logger.info("Authentication error", var7);
                  response.sendError(401, "Failed to login. Please re-try.");
                  return;
               }
            }

            this.underlyingFilterChain.doFilter((ServletRequest)servletRequest, servletResponse);
         }
      }
   }
}
