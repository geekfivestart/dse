package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

class JMXConnection {
   private static final String FMT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
   private final String host;
   private final String username;
   private final String password;
   private final int port;
   private JMXConnector jmxc;
   private MBeanServerConnection mbeanServerConn;

   JMXConnection(String host, int port, String username, String password) throws IOException {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.connect();
   }

   private void connect() throws IOException {
      JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", new Object[]{this.host, Integer.valueOf(this.port)}));
      Map<String, Object> env = new HashMap();
      if(this.username != null) {
         env.put("jmx.remote.credentials", new String[]{this.username, this.password});
      }

      this.jmxc = JMXConnectorFactory.connect(jmxUrl, env);
      this.mbeanServerConn = this.jmxc.getMBeanServerConnection();
   }

   public void close() throws IOException {
      this.jmxc.close();
   }

   public MBeanServerConnection getMbeanServerConn() {
      return this.mbeanServerConn;
   }
}
