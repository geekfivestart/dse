package com.datastax.bdp.cassandra.auth.http;

import com.datastax.bdp.cassandra.auth.Credentials;
import com.datastax.bdp.cassandra.auth.DseAuthenticationException;
import com.datastax.bdp.cassandra.auth.DseAuthenticator;
import com.datastax.bdp.util.Addresses;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseHttpBasicAuthenticationFilter implements Filter {
   Logger logger = LoggerFactory.getLogger(DseHttpBasicAuthenticationFilter.class);
   public static final String AUTHENTICATION_SCHEME = "Basic";

   public DseHttpBasicAuthenticationFilter() {
   }

   public void init(FilterConfig filterConfig) throws ServletException {
   }

   public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      HttpServletRequest request = (HttpServletRequest)servletRequest;
      HttpServletResponse response = (HttpServletResponse)servletResponse;

      try {
         if(DatabaseDescriptor.getAuthenticator().requireAuthentication()) {
            request = this.authenticateRequest(request);
         }

         filterChain.doFilter(request, response);
      } catch (DseAuthenticationException var9) {
         String fullURL = request.getRequestURL().append("?").append(request.getQueryString()).toString();
         this.logger.debug("Request to " + fullURL + " was not authenticated");
         String realm = Addresses.Internode.getBroadcastAddress().getHostName();
         response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm + "\"");
         response.sendError(401, "Failed to login. Please re-try.");
      }

   }

   private HttpServletRequest authenticateRequest(HttpServletRequest request) throws DseAuthenticationException, IOException {
      String authHeader = this.authorizationHeader(request);
      Credentials credentials = this.decodeCredentials(authHeader);
      ClientState clientState = this.authenticate(request, credentials.authenticationUser, credentials.password);
      return new DseAuthenticatedHttpRequest(request, clientState);
   }

   private String authorizationHeader(HttpServletRequest request) throws DseAuthenticationException {
      String authHeader = request.getHeader("Authorization");
      if(authHeader != null && authHeader.startsWith("Basic")) {
         return authHeader;
      } else {
         throw new DseAuthenticationException();
      }
   }

   private Credentials decodeCredentials(String authHeader) throws IOException, DseAuthenticationException {
      String[] authHeaderElements = authHeader.split(" ");
      if(authHeaderElements.length != 2) {
         throw new DseAuthenticationException();
      } else {
         return DseAuthenticator.decodeHttpBasicCredentials(authHeaderElements[1]);
      }
   }

   private ClientState authenticate(HttpServletRequest request, String userName, String password) throws DseAuthenticationException {
      Map<String, String> credentials = Maps.newHashMap();
      credentials.put("username", userName);
      credentials.put("password", password);
      ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved(request.getRemoteAddr(), request.getRemotePort()), (Connection)null);

      try {
         AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().legacyAuthenticate(credentials);
         TPCUtils.blockingGet(clientState.login(user));
         return clientState;
      } catch (AuthenticationException var7) {
         throw new DseAuthenticationException(userName);
      }
   }

   public void destroy() {
   }
}
