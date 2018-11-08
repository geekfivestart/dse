package org.apache.cassandra.locator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudstackSnitch extends AbstractNetworkTopologySnitch {
   protected static final Logger logger = LoggerFactory.getLogger(CloudstackSnitch.class);
   protected static final String ZONE_NAME_QUERY_URI = "/latest/meta-data/availability-zone";
   private Map<InetAddress, Map<String, String>> savedEndpoints;
   private static final String DEFAULT_DC = "UNKNOWN-DC";
   private static final String DEFAULT_RACK = "UNKNOWN-RACK";
   private static final String[] LEASE_FILES = new String[]{"file:///var/lib/dhcp/dhclient.eth0.leases", "file:///var/lib/dhclient/dhclient.eth0.leases"};
   protected String csZoneDc;
   protected String csZoneRack;

   public CloudstackSnitch() throws IOException, ConfigurationException {
      String endpoint = this.csMetadataEndpoint();
      String zone = this.csQueryMetadata(endpoint + "/latest/meta-data/availability-zone");
      String[] zone_parts = zone.split("-");
      if(zone_parts.length != 3) {
         throw new ConfigurationException("CloudstackSnitch cannot handle invalid zone format: " + zone);
      } else {
         this.csZoneDc = zone_parts[0] + "-" + zone_parts[1];
         this.csZoneRack = zone_parts[2];
      }
   }

   public String getRack(InetAddress endpoint) {
      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         return this.csZoneRack;
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
         VersionedValue rack = epState != null?epState.getApplicationState(ApplicationState.RACK):null;
         if(rack != null) {
            return rack.value;
         } else {
            if(this.savedEndpoints == null) {
               this.savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }

            return this.savedEndpoints.containsKey(endpoint)?(String)((Map)this.savedEndpoints.get(endpoint)).get("rack"):"UNKNOWN-RACK";
         }
      }
   }

   public String getDatacenter(InetAddress endpoint) {
      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         return this.csZoneDc;
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
         VersionedValue dc = epState != null?epState.getApplicationState(ApplicationState.DC):null;
         if(dc != null) {
            return dc.value;
         } else {
            if(this.savedEndpoints == null) {
               this.savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }

            return this.savedEndpoints.containsKey(endpoint)?(String)((Map)this.savedEndpoints.get(endpoint)).get("data_center"):"UNKNOWN-DC";
         }
      }
   }

   public String getLocalDatacenter() {
      return this.csZoneDc;
   }

   public String getLocalRack() {
      return this.csZoneRack;
   }

   String csQueryMetadata(String url) throws ConfigurationException, IOException {
      HttpURLConnection conn = null;
      DataInputStream is = null;

      try {
         conn = (HttpURLConnection)(new URL(url)).openConnection();
      } catch (Exception var10) {
         throw new ConfigurationException("CloudstackSnitch cannot query wrong metadata URL: " + url);
      }

      String var6;
      try {
         conn.setRequestMethod("GET");
         if(conn.getResponseCode() != 200) {
            throw new ConfigurationException("CloudstackSnitch was unable to query metadata.");
         }

         int cl = conn.getContentLength();
         byte[] b = new byte[cl];
         is = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
         is.readFully(b);
         var6 = new String(b, StandardCharsets.UTF_8);
      } finally {
         FileUtils.close(new Closeable[]{is});
         conn.disconnect();
      }

      return var6;
   }

   String csMetadataEndpoint() throws ConfigurationException {
      String[] var1 = LEASE_FILES;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         String lease_uri = var1[var3];

         try {
            File lease_file = new File(new URI(lease_uri));
            if(lease_file.exists()) {
               return this.csEndpointFromLease(lease_file);
            }
         } catch (Exception var6) {
            JVMStabilityInspector.inspectThrowable(var6);
         }
      }

      throw new ConfigurationException("No valid DHCP lease file could be found.");
   }

   String csEndpointFromLease(File lease) throws ConfigurationException {
      String endpoint = null;
      Pattern identifierPattern = Pattern.compile("^[ \t]*option dhcp-server-identifier (.*);$");

      try {
         BufferedReader reader = new BufferedReader(new FileReader(lease));
         Throwable var6 = null;

         try {
            String line;
            try {
               while((line = reader.readLine()) != null) {
                  Matcher matcher = identifierPattern.matcher(line);
                  if(matcher.find()) {
                     endpoint = matcher.group(1);
                     break;
                  }
               }
            } catch (Throwable var16) {
               var6 = var16;
               throw var16;
            }
         } finally {
            if(reader != null) {
               if(var6 != null) {
                  try {
                     reader.close();
                  } catch (Throwable var15) {
                     var6.addSuppressed(var15);
                  }
               } else {
                  reader.close();
               }
            }

         }
      } catch (Exception var18) {
         throw new ConfigurationException("CloudstackSnitch cannot access lease file.");
      }

      if(endpoint == null) {
         throw new ConfigurationException("No metadata server could be found in lease file.");
      } else {
         return "http://" + endpoint;
      }
   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return dc == "UNKNOWN-DC";
   }

   public String toString() {
      return "CloudstackSnitch{myDC='" + this.csZoneDc + '\'' + ", myRack='" + this.csZoneRack + "'}";
   }
}
