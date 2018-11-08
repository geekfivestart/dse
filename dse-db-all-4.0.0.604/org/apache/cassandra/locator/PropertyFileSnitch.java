package org.apache.cassandra.locator;

import com.google.common.collect.Sets;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyFileSnitch extends AbstractNetworkTopologySnitch {
   private static final Logger logger = LoggerFactory.getLogger(PropertyFileSnitch.class);
   public static final String SNITCH_PROPERTIES_FILENAME = "cassandra-topology.properties";
   private static final int DEFAULT_REFRESH_PERIOD_IN_SECONDS = 5;
   private static volatile Map<InetAddress, String[]> endpointMap;
   private static volatile String[] defaultDCRack;
   private static volatile String[] localDCRack;
   private volatile boolean gossipStarted;

   public PropertyFileSnitch() throws ConfigurationException {
      this(5);
   }

   public PropertyFileSnitch(int refreshPeriodInSeconds) throws ConfigurationException {
      this.reloadConfiguration(false);

      try {
         FBUtilities.resourceToFile("cassandra-topology.properties");
         Runnable runnable = new WrappedRunnable() {
            protected void runMayThrow() throws ConfigurationException {
               PropertyFileSnitch.this.reloadConfiguration(true);
            }
         };
         ResourceWatcher.watch("cassandra-topology.properties", runnable, refreshPeriodInSeconds * 1000);
      } catch (ConfigurationException var3) {
         logger.error("{} found, but does not look like a plain file. Will not watch it for changes", "cassandra-topology.properties");
      }

   }

   public static String[] getEndpointInfo(InetAddress endpoint) {
      String[] rawEndpointInfo = getRawEndpointInfo(endpoint);
      if(rawEndpointInfo == null) {
         throw new RuntimeException("Unknown host " + endpoint + " with no default configured");
      } else {
         return rawEndpointInfo;
      }
   }

   private static String[] getRawEndpointInfo(InetAddress endpoint) {
      String[] value = (String[])endpointMap.get(endpoint);
      if(value == null) {
         logger.trace("Could not find end point information for {}, will use default", endpoint);
         return defaultDCRack;
      } else {
         return value;
      }
   }

   public String getDatacenter(InetAddress endpoint) {
      String[] info = getEndpointInfo(endpoint);

      assert info != null : "No location defined for endpoint " + endpoint;

      return info[0];
   }

   public String getRack(InetAddress endpoint) {
      String[] info = getEndpointInfo(endpoint);

      assert info != null : "No location defined for endpoint " + endpoint;

      return info[1];
   }

   public String getLocalDatacenter() {
      return (localDCRack != null?localDCRack:defaultDCRack)[0];
   }

   public String getLocalRack() {
      return (localDCRack != null?localDCRack:defaultDCRack)[1];
   }

   public void reloadConfiguration(boolean isUpdate) throws ConfigurationException {
      HashMap<InetAddress, String[]> reloadedMap = new HashMap();
      String[] reloadedDefaultDCRack = null;
      Properties properties = new Properties();

      try {
         InputStream stream = this.getClass().getClassLoader().getResourceAsStream("cassandra-topology.properties");
         Throwable var6 = null;

         try {
            properties.load(stream);
         } catch (Throwable var19) {
            var6 = var19;
            throw var19;
         } finally {
            if(stream != null) {
               if(var6 != null) {
                  try {
                     stream.close();
                  } catch (Throwable var18) {
                     var6.addSuppressed(var18);
                  }
               } else {
                  stream.close();
               }
            }

         }
      } catch (Exception var22) {
         throw new ConfigurationException("Unable to read cassandra-topology.properties", var22);
      }

      Iterator var23 = properties.entrySet().iterator();

      while(var23.hasNext()) {
         Entry<Object, Object> entry = (Entry)var23.next();
         String key = (String)entry.getKey();
         String value = (String)entry.getValue();
         if("default".equals(key)) {
            String[] newDefault = value.split(":");
            if(newDefault.length < 2) {
               reloadedDefaultDCRack = new String[]{"default", "default"};
            } else {
               reloadedDefaultDCRack = new String[]{newDefault[0].trim().intern(), newDefault[1].trim()};
            }
         } else {
            String hostString = StringUtils.remove(key, '/');

            InetAddress host;
            try {
               host = InetAddress.getByName(hostString);
            } catch (UnknownHostException var20) {
               throw new ConfigurationException("Unknown host " + hostString, var20);
            }

            String[] token = value.split(":");
            if(token.length < 2) {
               token = new String[]{"default", "default"};
            } else {
               token = new String[]{token[0].trim(), token[1].trim()};
            }

            reloadedMap.put(host, token);
         }
      }

      InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
      String[] localInfo = (String[])reloadedMap.get(broadcastAddress);
      if(reloadedDefaultDCRack == null && localInfo == null) {
         throw new ConfigurationException(String.format("Snitch definitions at %s do not define a location for this node's broadcast address %s, nor does it provides a default", new Object[]{"cassandra-topology.properties", broadcastAddress}));
      } else {
         InetAddress localAddress = FBUtilities.getLocalAddress();
         if(!localAddress.equals(broadcastAddress) && !reloadedMap.containsKey(localAddress)) {
            reloadedMap.put(localAddress, localInfo);
         }

         if(!isUpdate || livenessCheck(reloadedMap, reloadedDefaultDCRack)) {
            if(logger.isTraceEnabled()) {
               StringBuilder sb = new StringBuilder();
               Iterator var30 = reloadedMap.entrySet().iterator();

               while(var30.hasNext()) {
                  Entry<InetAddress, String[]> entry = (Entry)var30.next();
                  sb.append(entry.getKey()).append(':').append(Arrays.toString((Object[])entry.getValue())).append(", ");
               }

               logger.trace("Loaded network topology from property file: {}", StringUtils.removeEnd(sb.toString(), ", "));
            }

            defaultDCRack = reloadedDefaultDCRack;
            localDCRack = localInfo;
            endpointMap = reloadedMap;
            if(StorageService.instance != null) {
               if(isUpdate) {
                  StorageService.instance.updateTopology();
               } else {
                  StorageService.instance.getTokenMetadata().invalidateCachedRings();
               }
            }

            if(this.gossipStarted) {
               StorageService.instance.gossipSnitchInfo();
            }

         }
      }
   }

   private static boolean livenessCheck(HashMap<InetAddress, String[]> reloadedMap, String[] reloadedDefaultDCRack) {
      Set<InetAddress> hosts = Arrays.equals(defaultDCRack, reloadedDefaultDCRack)?Sets.intersection(StorageService.instance.getLiveRingMembers(), Sets.union(endpointMap.keySet(), reloadedMap.keySet())):StorageService.instance.getLiveRingMembers();
      Iterator var3 = ((Set)hosts).iterator();

      InetAddress host;
      String[] origValue;
      String[] updateValue;
      do {
         if(!var3.hasNext()) {
            return true;
         }

         host = (InetAddress)var3.next();
         origValue = endpointMap.containsKey(host)?(String[])endpointMap.get(host):defaultDCRack;
         updateValue = reloadedMap.containsKey(host)?(String[])reloadedMap.get(host):reloadedDefaultDCRack;
      } while(Arrays.equals(origValue, updateValue));

      logger.error("Cannot update data center or rack from {} to {} for live host {}, property file NOT RELOADED", new Object[]{origValue, updateValue, host});
      return false;
   }

   public void gossiperStarting() {
      this.gossipStarted = true;
   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return defaultDCRack == null?false:dc == defaultDCRack[0];
   }

   public String toString() {
      return "PropertyFileSnitch{gossipStarted=" + this.gossipStarted + '}';
   }
}
