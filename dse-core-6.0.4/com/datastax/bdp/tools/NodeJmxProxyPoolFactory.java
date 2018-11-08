package com.datastax.bdp.tools;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

public class NodeJmxProxyPoolFactory implements Provider<NodeJmxProxyPool> {
   private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
   private final Set<ProxySource> proxySources;
   private String host;
   private int port = 7199;
   private String username;
   private String password;

   @Inject
   public NodeJmxProxyPoolFactory(Set<ProxySource> proxySources) {
      this.proxySources = ImmutableSet.copyOf(proxySources);
   }

   public Set<ProxySource> getProxySources() {
      return this.proxySources;
   }

   public NodeJmxProxyPool get() {
      try {
         return new NodeJmxProxyPool(this.getMbeanServerConnection(), this.proxySources, this.host);
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public String getHost() {
      return this.host;
   }

   public void setHost(String host) {
      this.host = host;
   }

   public String getPassword() {
      return this.password;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public int getPort() {
      return this.port;
   }

   public void setPort(int port) {
      this.port = port;
   }

   public String getUsername() {
      return this.username;
   }

   public void setUsername(String username) {
      this.username = username;
   }

   protected MBeanServerConnection getMbeanServerConnection() throws IOException {
      if(this.host == null) {
         return ManagementFactory.getPlatformMBeanServer();
      } else {
         JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", new Object[]{this.host, Integer.valueOf(this.port)}));
         Map<String, Object> env = new HashMap();
         if(this.username != null) {
            String[] creds = new String[]{this.username, this.password};
            env.put("jmx.remote.credentials", creds);
         }

         return jmxConnect(jmxUrl, env).getMBeanServerConnection();
      }
   }

   public static JMXConnector jmxConnect(JMXServiceURL jmxUrl, Map<String, Object> env) throws IOException {
      Map<String, Object> localEnv = new HashMap();
      localEnv.putAll(env);
      localEnv.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());
      return JMXConnectorFactory.connect(jmxUrl, localEnv);
   }

   private static RMIClientSocketFactory getRMIClientSocketFactory() {
      return (RMIClientSocketFactory)(Boolean.parseBoolean(System.getProperty("ssl.enable"))?new SslRMIClientSocketFactory():RMISocketFactory.getDefaultSocketFactory());
   }
}
