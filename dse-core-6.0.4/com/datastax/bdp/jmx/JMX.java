package com.datastax.bdp.jmx;

import com.datastax.bdp.util.MapBuilder;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map.Entry;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class JMX {
   public JMX() {
   }

   public static void registerMBean(Object o, JMX.Type type, MapBuilder.ImmutableMap<String, String> names) {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(o, new ObjectName(buildMBeanName(type, names)));
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }
   }

   public static void unregisterMBean(JMX.Type type, MapBuilder.ImmutableMap<String, String> names) {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.unregisterMBean(new ObjectName(buildMBeanName(type, names)));
      } catch (Exception var4) {
         throw new RuntimeException(var4);
      }
   }

   public static String buildMBeanName(JMX.Type type, MapBuilder.ImmutableMap<String, String> names) {
      StringBuilder nameBuilder = (new StringBuilder("com.datastax.bdp:type=")).append(type.getTypeString());
      Iterator var3 = names.entrySet().iterator();

      while(var3.hasNext()) {
         Entry namePair = (Entry)var3.next();
         nameBuilder.append(",").append(namePair.getKey()).append("=").append(namePair.getValue());
      }

      return nameBuilder.toString();
   }

   public static ObjectName getObjectName(JMX.Type type, String name) throws MalformedObjectNameException {
      return new ObjectName(buildMBeanName(type, MapBuilder.immutable().withKeys(new String[]{"name"}).withValues(new String[]{name}).build()));
   }

   public static enum Type {
      ANALYTICS("analytics"),
      CORE("core"),
      SEARCH("search"),
      PERF_OBJECTS("performance objects"),
      GRAPH("graph"),
      METRICS("metrics");

      private final String typeString;

      private Type(String typeString) {
         this.typeString = typeString;
      }

      public String getTypeString() {
         return this.typeString;
      }
   }
}
