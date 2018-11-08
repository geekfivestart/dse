package com.datastax.bdp.transport.server;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.transport.common.ServicePrincipalFormatException;
import com.datastax.bdp.util.Addresses;
import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosServerUtils {
   private static final Logger logger = LoggerFactory.getLogger(KerberosServerUtils.class);
   private static final String DEFAULT_SUPERUSER_NAME = "cassandra";

   public KerberosServerUtils() {
   }

   public static Subject loginServer(String kerberosPrincipal) throws LoginException {
      Subject subject = new Subject();
      LoginContext loginCtx = new LoginContext(kerberosPrincipal, subject, (CallbackHandler)null, new KrbConfiguration());
      loginCtx.login();
      return subject;
   }

   public static void validateServicePrincipal(String servicePrincipal) throws ConfigurationException {
      try {
         String principalHostName = (new ServicePrincipal(servicePrincipal)).getLocalHostName();
         String myHostName = Addresses.Internode.getBroadcastAddress().getHostName();
         if(logger.isTraceEnabled()) {
            logger.trace("[validate-service-principal] using service-principal = {}, principal-host-name = {}, host-name = {}", new Object[]{servicePrincipal, principalHostName, myHostName});
         }

         if(!principalHostName.equals(myHostName)) {
            logger.warn("Service principal host does not match the name of the host running DSE (" + principalHostName + " != " + myHostName + "). This may pose problems for some clients.");
         }

      } catch (ServicePrincipalFormatException var3) {
         throw new ConfigurationException(var3.getMessage());
      }
   }

   public static String getUserNameFromAuthzId(String authzId) {
      String[] names = authzId.split("[/@]");
      if("cassandra".equals(names[0])) {
         if(logger.isTraceEnabled()) {
            logger.trace("[user-from-auth-id] using auth-id = {}", "cassandra");
         }

         return "cassandra";
      } else {
         if(logger.isTraceEnabled()) {
            logger.trace("[user-from-auth-id] using auth-id = {}", authzId);
         }

         return authzId;
      }
   }

   public static AuthenticatedUser getUserFromAuthzId(String authzId) {
      String userName = getUserNameFromAuthzId(authzId);
      if(logger.isTraceEnabled()) {
         logger.trace("[user-from-auth-id] using auth-id = {}", userName);
      }

      return new AuthenticatedUser(userName);
   }

   public static String getOrExtractServiceName(String name) {
      if(logger.isTraceEnabled()) {
         logger.trace("[extract-service-name] using name = {}", name);
      }

      if(name == null) {
         throw new NullPointerException("Name cannot be null");
      } else {
         try {
            Class<?> kerberosNameClass = Class.forName("org.apache.hadoop.security.HadoopKerberosName");
            Object kerberosName = kerberosNameClass.getConstructor(new Class[]{String.class}).newInstance(new Object[]{name});
            String serviceName = (String)kerberosName.getClass().getMethod("getShortName", new Class[0]).invoke(kerberosName, new Object[0]);
            if(logger.isTraceEnabled()) {
               logger.trace("[extract-service-name] using service-name = {}", serviceName);
            }

            return serviceName;
         } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException var4) {
            throw new AssertionError("Failed to obtain KerberosName or HadoopKerberosName - this should not happen", var4);
         } catch (Exception var5) {
            throw new IllegalArgumentException("Invalid Kerberos name provided: " + name, var5);
         }
      }
   }

   public static String getLocalHostName(ClientConfiguration clientConf) throws UnknownHostException {
      try {
         return clientConf.getCassandraHost().getHostName();
      } catch (Throwable var2) {
         logger.warn("Failed to get hostname from service principal in dse.yaml: " + var2.getMessage());
         return InetAddress.getLocalHost().getCanonicalHostName();
      }
   }

   public static String getCanonicalHostName(String host, ClientConfiguration clientConf) throws UnknownHostException {
      return !host.equals("127.0.0.1") && !host.equals("localhost")?InetAddress.getByName(host).getCanonicalHostName():getLocalHostName(clientConf);
   }

   public static String getServerPrincipal(String host, ClientConfiguration clientConf) throws UnknownHostException {
      return getServiceName(clientConf) + "/" + getCanonicalHostName(host, clientConf);
   }

   public static String getServiceName(ClientConfiguration clientConf) {
      if(clientConf.isKerberosEnabled()) {
         try {
            return clientConf.getDseServicePrincipal().service;
         } catch (Throwable var2) {
            if(clientConf.isKerberosDefaultScheme()) {
               logger.warn("Failed to get service name from service principal in dse.yaml: " + var2.getMessage());
            }
         }
      }

      return "dse";
   }
}
