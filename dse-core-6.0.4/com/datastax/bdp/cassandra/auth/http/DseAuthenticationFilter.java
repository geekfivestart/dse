package com.datastax.bdp.cassandra.auth.http;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.util.Addresses;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.commons.codec.binary.Base64;

public class DseAuthenticationFilter implements Filter {
   private static final Pattern SERVICE_PRINCIPAL_PATTERN = Pattern.compile("(.+)/(.+)@(.+)");
   private Filter gssapiFilter;
   private Filter plainFilter;

   public DseAuthenticationFilter() {
   }

   public void init(FilterConfig filterConfig) throws ServletException {
      if(DseConfig.isKerberosEnabled()) {
         this.gssapiFilter = new DseHttpKerberosAuthenticationFilter();
         this.gssapiFilter.init(this.kerberosAuthenticationFilterConfig(filterConfig));
      }

      if(DseConfig.isPlainTextAuthEnabled()) {
         this.plainFilter = new DseHttpBasicAuthenticationFilter();
         this.plainFilter.init(this.passwordAuthenticationFilterConfig(filterConfig));
      }

      if(this.gssapiFilter == null && this.plainFilter == null && DatabaseDescriptor.getAuthenticator().requireAuthentication()) {
         throw new ServletException("DseAuthenticationFilter can be used only with one of: DseAuthenticator, KerberosAuthenticator, LdapAuthenticator, PasswordAuthenticator, AllowAllAuthenticator");
      }
   }

   private FilterConfig passwordAuthenticationFilterConfig(FilterConfig filterConfig) {
      Map<String, String> defaultConfig = Collections.emptyMap();
      return new DseAuthenticationFilter.FilterConfigWrapper("PasswordAuthenticationFilter", filterConfig, defaultConfig, null);
   }

   private FilterConfig kerberosAuthenticationFilterConfig(FilterConfig filterConfig) {
      Map<String, String> defaultConfig = new HashMap<String, String>() {
         {
            this.put("type", "kerberos");
            this.put("token.validity", "3600");
            this.put("cookie.path", "/");
            if(DseConfig.isKerberosEnabled()) {
               this.put("kerberos.principal", DseConfig.getHttpKrbprincipal().asLocal());
               this.put("kerberos.keytab", DseConfig.getDseServiceKeytab());
            }

         }
      };
      return new DseAuthenticationFilter.FilterConfigWrapper("KerberosAuthenticationFilter", filterConfig, defaultConfig, null);
   }

   public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      if(this.plainFilter != null && this.hasPlainTextCredentials(servletRequest)) {
         this.plainFilter.doFilter(servletRequest, servletResponse, filterChain);
      } else if(this.gssapiFilter != null) {
         this.gssapiFilter.doFilter(servletRequest, servletResponse, filterChain);
      } else if(this.plainFilter == null && this.gssapiFilter == null) {
         filterChain.doFilter(servletRequest, servletResponse);
      } else {
         String realm = Addresses.Internode.getBroadcastAddress().getHostName();
         HttpServletResponse response = (HttpServletResponse)servletResponse;
         response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm + "\"");
         response.sendError(401, "This node requires authentication");
      }

   }

   public void destroy() {
      if(this.plainFilter != null) {
         this.plainFilter.destroy();
      }

      if(this.gssapiFilter != null) {
         this.gssapiFilter.destroy();
      }

   }

   public static boolean isTrustedDseNode(HttpServletRequest request) throws UnknownHostException {
      if(!DseConfig.isKerberosEnabled()) {
         return false;
      } else {
         ServicePrincipal dseServicePrincipal = DseConfig.getDseServicePrincipal();
         String userPrincipal = request.getUserPrincipal().getName();
         Matcher matcher = SERVICE_PRINCIPAL_PATTERN.matcher(userPrincipal);
         return matcher.matches() && matcher.group(1).equals(dseServicePrincipal.service) && matcher.group(3).equals(dseServicePrincipal.realm) && Gossiper.instance.isKnownEndpoint(InetAddress.getByName(request.getRemoteAddr()));
      }
   }

   private boolean hasPlainTextCredentials(ServletRequest request) {
      String authHeader = ((HttpServletRequest)request).getHeader("Authorization");
      if(authHeader != null && authHeader.startsWith("Basic")) {
         String[] authHeaderElements = authHeader.split(" ");
         if(authHeaderElements.length == 2) {
            String credentialsStr = new String(Base64.decodeBase64(authHeaderElements[1]));
            return credentialsStr.split(":").length == 2;
         }
      }

      return false;
   }

   private static class FilterConfigWrapper implements FilterConfig {
      private final String filterName;
      private final FilterConfig wrappedConfig;
      private final Map<String, String> defaultConfig;

      private FilterConfigWrapper(String filterName, FilterConfig wrappedConfig, Map<String, String> defaultConfig) {
         this.filterName = filterName;
         this.wrappedConfig = wrappedConfig;
         this.defaultConfig = defaultConfig;
      }

      public String getFilterName() {
         return this.filterName;
      }

      public ServletContext getServletContext() {
         return this.wrappedConfig.getServletContext();
      }

      public String getInitParameter(String s) {
         String value = this.wrappedConfig.getInitParameter(s);
         return value != null?value:(String)this.defaultConfig.get(s);
      }

      public Enumeration<String> getInitParameterNames() {
         Set<String> providedNames = Sets.newHashSet(Collections.list(this.wrappedConfig.getInitParameterNames()));
         Set<String> defaultNames = this.defaultConfig.keySet();
         return Collections.enumeration(Sets.union(providedNames, defaultNames));
      }
   }
}
