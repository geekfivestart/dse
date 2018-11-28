package com.datastax.bdp.tools;

import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.bean.PluginBean;
import com.datastax.bdp.util.MapBuilder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class NodeJmxProxyPool {
   private final Map<Class, Object> proxyInterfaces = new HashMap();
   private final Map<String, Object> proxies = new HashMap();
   private final String dsehost;
   private final MBeanServerConnection connection;

   NodeJmxProxyPool(MBeanServerConnection connection, Iterable<ProxySource> proxySources, String host) {
      this.dsehost = host;
      this.connection = connection;

      try {
         Iterator var4 = proxySources.iterator();

         while(var4.hasNext()) {
            ProxySource proxySource = (ProxySource)var4.next();
            proxySource.makeProxies(this);
         }

      } catch (MalformedObjectNameException var6) {
         throw new RuntimeException("Unable to initialize proxies", var6);
      }
   }

   public String getDsehost() {
      return this.dsehost;
   }

   public <T> T makeProxy(JMX.Type type, Class<T> interfaceClass, MapBuilder.ImmutableMap<String, String> properties) throws MalformedObjectNameException {
      String mbeanName = JMX.buildMBeanName(type, properties);
      T proxy = javax.management.JMX.newMXBeanProxy(this.connection, new ObjectName(mbeanName), interfaceClass);
      this.proxyInterfaces.put(interfaceClass, proxy);
      this.proxies.put(properties.get("name"), proxy);
      return proxy;
   }

   public <T> T makeProxy(JMX.Type type, String name, Class<T> interfaceClass) throws MalformedObjectNameException {
      T proxy = javax.management.JMX.newMXBeanProxy(this.connection, JMX.getObjectName(type, name), interfaceClass);
      this.proxyInterfaces.put(interfaceClass, proxy);
      this.proxies.put(name, proxy);
      return proxy;
   }

   public <T> T makePerfProxy(Class<? extends PluginBean> pluginClass, Class<T> interfaceClass) throws MalformedObjectNameException {
      String name = PerformanceObjectsController.getPerfBeanName(pluginClass);
      T proxy = (T)this.proxies.get(name);
      if(proxy == null) {
         proxy = javax.management.JMX.newMXBeanProxy(this.connection, JMX.getObjectName(JMX.Type.PERF_OBJECTS, name), interfaceClass);
         this.proxies.put(name, proxy);
      }

      return proxy;
   }

   public <P> P getProxy(Class<P> proxiedClass) {
      return (P)this.proxyInterfaces.get(proxiedClass);
   }

   public Object getProxy(String name) {
      return this.proxies.get(name);
   }
}
