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

public class Ec2Snitch extends AbstractNetworkTopologySnitch {
   protected static final Logger logger = LoggerFactory.getLogger(Ec2Snitch.class);
   protected static final String ZONE_NAME_QUERY_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
   private static final String DEFAULT_DC = "UNKNOWN-DC";
   private static final String DEFAULT_RACK = "UNKNOWN-RACK";
   private Map<InetAddress, Map<String, String>> savedEndpoints;
   protected String ec2zone;
   protected String ec2region;
   protected String datacenterSuffix;

   public Ec2Snitch() throws IOException, ConfigurationException {
      String az = this.awsApiCall("http://169.254.169.254/latest/meta-data/placement/availability-zone");
      String[] splits = az.split("-");
      this.ec2zone = splits[splits.length - 1];
      this.ec2region = az.substring(0, az.length() - 1);
      if(this.ec2region.endsWith("1")) {
         this.ec2region = az.substring(0, az.length() - 3);
      }

      this.datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
      this.ec2region = this.ec2region.concat(this.datacenterSuffix);
      logger.info("EC2Snitch using region: {}, zone: {}.", this.ec2region, this.ec2zone);
   }

   String awsApiCall(String url) throws IOException, ConfigurationException {
      HttpURLConnection conn = (HttpURLConnection)(new URL(url)).openConnection();
      DataInputStream d = null;

      String var6;
      try {
         conn.setRequestMethod("GET");
         if(conn.getResponseCode() != 200) {
            throw new ConfigurationException("Ec2Snitch was unable to execute the API call. Not an ec2 node?");
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
         return this.ec2zone;
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
         return this.ec2region;
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
      return this.ec2region;
   }

   public String getLocalRack() {
      return this.ec2zone;
   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return dc == "UNKNOWN-DC";
   }

   public String toString() {
      return "Ec2Snitch{, datacenterSuffix=" + this.datacenterSuffix + ", region(DC)='" + this.ec2region + '\'' + ", zone(rack)='" + this.ec2zone + "'}";
   }
}
