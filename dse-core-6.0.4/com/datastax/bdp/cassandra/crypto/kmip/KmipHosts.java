package com.datastax.bdp.cassandra.crypto.kmip;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.config.KmipHostOptions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.exceptions.ConfigurationException;

public class KmipHosts implements KmipHostsMXBean {
   public static final KmipHosts instance = new KmipHosts();
   static Map<String, KmipHost> hosts = new HashMap();
   private static volatile boolean initialized = false;

   public KmipHosts() {
   }

   public static synchronized void init() throws ConfigurationException {
      if(!initialized) {
         Iterator var0 = DseConfig.getKmipHosts().entrySet().iterator();

         while(var0.hasNext()) {
            Entry<String, KmipHostOptions> entry = (Entry)var0.next();
            registerHost((String)entry.getKey(), (KmipHostOptions)entry.getValue());
         }

         initialized = true;
      }
   }

   public static KmipHost createHost(String name, KmipHostOptions options) throws ConfigurationException {
      try {
         return new KmipHost(name, options);
      } catch (IOException var3) {
         throw new ConfigurationException(var3.getMessage());
      }
   }

   public static synchronized KmipHost registerHost(String name, KmipHostOptions options) throws ConfigurationException {
      if(hosts.containsKey(name)) {
         throw new IllegalStateException("Kmip host '" + name + "' already exists");
      } else {
         KmipHost host = createHost(name, options);
         hosts.put(name, host);
         return host;
      }
   }

   public static KmipHost getHost(String name) {
      return (KmipHost)hosts.get(name);
   }

   public Map<String, List<String>> getConnectionErrors() {
      Map<String, List<String>> errorMap = new HashMap();
      Iterator var2 = hosts.values().iterator();

      while(var2.hasNext()) {
         KmipHost host = (KmipHost)var2.next();
         errorMap.put(host.getHostName(), host.getErrorMessages());
      }

      return errorMap;
   }
}
