package org.apache.cassandra.locator;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudSnitch extends AbstractNetworkTopologySnitch {
   protected static final Logger logger = LoggerFactory.getLogger(GoogleCloudSnitch.class);
   protected static final String ZONE_NAME_QUERY_URL = "http://metadata.google.internal/computeMetadata/v1/instance/zone";
   private static final String DEFAULT_DC = "UNKNOWN-DC";
   private static final String DEFAULT_RACK = "UNKNOWN-RACK";
   private Map<InetAddress, Map<String, String>> savedEndpoints;
   protected String gceZone;
   protected String gceRegion;
   protected String datacenterSuffix;

   public GoogleCloudSnitch() throws IOException, ConfigurationException {
      String response = this.gceApiCall("http://metadata.google.internal/computeMetadata/v1/instance/zone");
      String[] splits = response.split("/");
      String az = splits[splits.length - 1];
      splits = az.split("-");
      this.gceZone = splits[splits.length - 1];
      int lastRegionIndex = az.lastIndexOf("-");
      this.gceRegion = az.substring(0, lastRegionIndex);
      this.datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
      this.gceRegion = this.gceRegion.concat(this.datacenterSuffix);
      logger.info("GCESnitch using region: {}, zone: {}.", this.gceRegion, this.gceZone);
   }

   String gceApiCall(String url) throws IOException, ConfigurationException {
      HttpURLConnection conn = (HttpURLConnection)(new URL(url)).openConnection();
      DataInputStream d = null;

      String var6;
      try {
         conn.setRequestMethod("GET");
         conn.setRequestProperty("Metadata-Flavor", "Google");
         if(conn.getResponseCode() != 200) {
            throw new ConfigurationException("GoogleCloudSnitch was unable to execute the API call. Not a gce node?");
         }

         int cl = conn.getContentLength();
         byte[] b = new byte[cl];
         d = new DataInputStream((FilterInputStream)conn.getContent());
         d.readFully(b);
         var6 = new String(b, StandardCharsets.UTF_8);
      } finally {
         FileUtils.close(new Closeable[]{d});
         conn.disconnect();
      }

      return var6;
   }

   public String getRack(InetAddress endpoint) {
      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         return this.gceZone;
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
         return this.gceRegion;
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
      return this.gceRegion;
   }

   public String getLocalRack() {
      return this.gceZone;
   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return dc == "UNKNOWN-DC";
   }

   public String toString() {
      return "Ec2Snitch{, datacenterSuffix=" + this.datacenterSuffix + ", region(DC)='" + this.gceRegion + '\'' + ", zone(rack)='" + this.gceZone + "'}";
   }
}
