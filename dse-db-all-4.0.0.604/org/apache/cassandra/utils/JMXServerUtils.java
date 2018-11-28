package org.apache.cassandra.utils;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.MBeanServerForwarder;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import javax.security.auth.Subject;
import org.apache.cassandra.auth.jmx.AuthenticationProxy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMXServerUtils {
   private static final Logger logger = LoggerFactory.getLogger(JMXServerUtils.class);

   public JMXServerUtils() {
   }

   public static JMXConnectorServer createJMXServer(int port, boolean local) throws IOException {
      InetAddress serverAddress = null;
      if(local) {
         serverAddress = InetAddress.getLoopbackAddress();
         System.setProperty("java.rmi.server.hostname", serverAddress.getHostAddress());
      }

      Map<String, Object> env = new HashMap(configureJmxSocketFactories(serverAddress, local));
      Registry registry = LocateRegistry.createRegistry(port, (RMIClientSocketFactory)env.get("jmx.remote.rmi.client.socket.factory"), (RMIServerSocketFactory)env.get("jmx.remote.rmi.server.socket.factory"));
      env.putAll(configureJmxAuthentication());
      MBeanServerForwarder authzProxy = configureJmxAuthorization(env);
      env.put("jmx.remote.x.daemon", "true");
      int rmiPort = Integer.getInteger("com.sun.management.jmxremote.rmi.port", 0).intValue();
      RMIJRMPServerImpl server = new RMIJRMPServerImpl(rmiPort, (RMIClientSocketFactory)env.get("jmx.remote.rmi.client.socket.factory"), (RMIServerSocketFactory)env.get("jmx.remote.rmi.server.socket.factory"), env);
      JMXServiceURL serviceURL = new JMXServiceURL("rmi", (String)null, rmiPort);
      RMIConnectorServer jmxServer = new RMIConnectorServer(serviceURL, env, server, ManagementFactory.getPlatformMBeanServer());
      if(authzProxy != null) {
         jmxServer.setMBeanServerForwarder(authzProxy);
      }

      jmxServer.start();
      registry.rebind("jmxrmi", server);
      logJmxServiceUrl(serverAddress, port);
      return jmxServer;
   }

   private static Map<String, Object> configureJmxAuthentication() {
      Map<String, Object> env = new HashMap();
      if(!Boolean.getBoolean("com.sun.management.jmxremote.authenticate")) {
         return env;
      } else {
         String configEntry = System.getProperty("cassandra.jmx.remote.login.config");
         if(configEntry != null) {
            env.put("jmx.remote.authenticator", new AuthenticationProxy(configEntry));
         } else {
            String passwordFile = System.getProperty("com.sun.management.jmxremote.password.file");
            if(passwordFile != null) {
               env.put("jmx.remote.x.password.file", passwordFile);
            }

            env.put("jmx.remote.authenticator", new JMXServerUtils.JMXPluggableAuthenticatorWrapper(env));
         }

         return env;
      }
   }

   private static MBeanServerForwarder configureJmxAuthorization(Map<String, Object> env) {
      String authzProxyClass = System.getProperty("cassandra.jmx.authorizer");
      if(authzProxyClass != null) {
         InvocationHandler handler = (InvocationHandler)FBUtilities.construct(authzProxyClass, "JMX authz proxy");
         Class[] interfaces = new Class[]{MBeanServerForwarder.class};
         Object proxy = Proxy.newProxyInstance(MBeanServerForwarder.class.getClassLoader(), interfaces, handler);
         return (MBeanServerForwarder)MBeanServerForwarder.class.cast(proxy);
      } else {
         String accessFile = System.getProperty("com.sun.management.jmxremote.access.file");
         if(accessFile != null) {
            env.put("jmx.remote.x.access.file", accessFile);
         }

         return null;
      }
   }

   private static Map<String, Object> configureJmxSocketFactories(InetAddress serverAddress, boolean localOnly) {
      Map<String, Object> env = new HashMap();
      if(Boolean.getBoolean("com.sun.management.jmxremote.ssl")) {
         boolean requireClientAuth = Boolean.getBoolean("com.sun.management.jmxremote.ssl.need.client.auth");
         String[] protocols = null;
         String protocolList = System.getProperty("com.sun.management.jmxremote.ssl.enabled.protocols");
         if(protocolList != null) {
            System.setProperty("javax.rmi.ssl.client.enabledProtocols", protocolList);
            protocols = StringUtils.split(protocolList, ',');
         }

         String[] ciphers = null;
         String cipherList = System.getProperty("com.sun.management.jmxremote.ssl.enabled.cipher.suites");
         if(cipherList != null) {
            System.setProperty("javax.rmi.ssl.client.enabledCipherSuites", cipherList);
            ciphers = StringUtils.split(cipherList, ',');
         }

         SslRMIClientSocketFactory clientFactory = new SslRMIClientSocketFactory();
         SslRMIServerSocketFactory serverFactory = new SslRMIServerSocketFactory(ciphers, protocols, requireClientAuth);
         env.put("jmx.remote.rmi.server.socket.factory", serverFactory);
         env.put("jmx.remote.rmi.client.socket.factory", clientFactory);
         env.put("com.sun.jndi.rmi.factory.socket", clientFactory);
         logJmxSslConfig(serverFactory);
      } else if(localOnly) {
         env.put("jmx.remote.rmi.server.socket.factory", new RMIServerSocketFactoryImpl(serverAddress));
      }

      return env;
   }

   private static void logJmxServiceUrl(InetAddress serverAddress, int port) {
      String urlTemplate = "service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$d/jmxrmi";
      String hostName;
      if(serverAddress == null) {
         hostName = FBUtilities.getBroadcastAddress() instanceof Inet6Address?"[::]":"0.0.0.0";
      } else {
         hostName = serverAddress instanceof Inet6Address?'[' + serverAddress.getHostAddress() + ']':serverAddress.getHostAddress();
      }

      String url = String.format(urlTemplate, new Object[]{hostName, Integer.valueOf(port)});
      logger.info("Configured JMX server at: {}", url);
   }

   private static void logJmxSslConfig(SslRMIServerSocketFactory serverFactory) {
      logger.debug("JMX SSL configuration. { protocols: [{}], cipher_suites: [{}], require_client_auth: {} }", new Object[]{serverFactory.getEnabledProtocols() == null?"'JVM defaults'":Arrays.stream(serverFactory.getEnabledProtocols()).collect(Collectors.joining("','", "'", "'")), serverFactory.getEnabledCipherSuites() == null?"'JVM defaults'":Arrays.stream(serverFactory.getEnabledCipherSuites()).collect(Collectors.joining("','", "'", "'")), Boolean.valueOf(serverFactory.getNeedClientAuth())});
   }

   private static class JMXPluggableAuthenticatorWrapper implements JMXAuthenticator {
      private static final MethodHandle ctorHandle;
      final Map<?, ?> env;

      private JMXPluggableAuthenticatorWrapper(Map<?, ?> env) {
         this.env = ImmutableMap.copyOf(env);
      }

      public Subject authenticate(Object credentials) {
         try {
            JMXAuthenticator authenticator = (JMXAuthenticator)ctorHandle.invoke(this.env);
            return authenticator.authenticate(credentials);
         } catch (Throwable var3) {
            throw new RuntimeException(var3);
         }
      }

      static {
         try {
            Class c = Class.forName("com.sun.jmx.remote.security.JMXPluggableAuthenticator");
            Constructor ctor = c.getDeclaredConstructor(new Class[]{Map.class});
            ctorHandle = MethodHandles.lookup().unreflectConstructor(ctor);
         } catch (Exception var2) {
            throw new RuntimeException(var2);
         }
      }
   }
}
