package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch {
   private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);
   public static final String PFS_COMPAT_MODE_PROP = "cassandra.gpfs.enable_pfs_compatibility_mode";
   public static Boolean PFS_COMPATIBILITY_ENABLED = Boolean.valueOf(System.getProperty("cassandra.gpfs.enable_pfs_compatibility_mode", "false"));
   private PropertyFileSnitch psnitch;
   private final String myDC;
   private final String myRack;
   private final boolean preferLocal;
   private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;
   private Map<InetAddress, Map<String, String>> savedEndpoints;
   protected static final String DEFAULT_DC = "UNKNOWN_DC";
   protected static final String DEFAULT_RACK = "UNKNOWN_RACK";

   public GossipingPropertyFileSnitch() throws ConfigurationException {
      this(PFS_COMPATIBILITY_ENABLED.booleanValue());
   }

   public GossipingPropertyFileSnitch(boolean enablePfsCompatibilityMode) throws ConfigurationException {
      SnitchProperties properties = loadConfiguration();
      this.myDC = properties.get("dc", "UNKNOWN_DC").trim();
      this.myRack = properties.get("rack", "UNKNOWN_RACK").trim();
      this.preferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
      this.snitchHelperReference = new AtomicReference();
      if(enablePfsCompatibilityMode) {
         try {
            this.psnitch = new PropertyFileSnitch();
            logger.info("Loaded {} for compatibility", "cassandra-topology.properties");
         } catch (ConfigurationException var4) {
            logger.info("Unable to load {}; compatibility mode disabled", "cassandra-topology.properties");
         }
      } else {
         logger.debug("Property file snitch compatibility mode is disabled. Set startup property {}=true to enable.", "cassandra.gpfs.enable_pfs_compatibility_mode");
      }

   }

   private static SnitchProperties loadConfiguration() throws ConfigurationException {
      SnitchProperties properties = new SnitchProperties();
      if(properties.contains("dc") && properties.contains("rack")) {
         return properties;
      } else {
         throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: cassandra-rackdc.properties");
      }
   }

   public String getDatacenter(InetAddress endpoint) {
      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         return this.myDC;
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
         VersionedValue dc = epState != null?epState.getApplicationState(ApplicationState.DC):null;
         if(dc != null) {
            return dc.value;
         } else {
            if(this.savedEndpoints == null) {
               this.savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }

            return this.savedEndpoints.containsKey(endpoint)?(String)((Map)this.savedEndpoints.get(endpoint)).get("data_center"):(this.psnitch != null?this.psnitch.getDatacenter(endpoint):"UNKNOWN_DC");
         }
      }
   }

   public String getRack(InetAddress endpoint) {
      if(endpoint.equals(FBUtilities.getBroadcastAddress())) {
         return this.myRack;
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
         VersionedValue rack = epState != null?epState.getApplicationState(ApplicationState.RACK):null;
         if(rack != null) {
            return rack.value;
         } else {
            if(this.savedEndpoints == null) {
               this.savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }

            return this.savedEndpoints.containsKey(endpoint)?(String)((Map)this.savedEndpoints.get(endpoint)).get("rack"):(this.psnitch != null?this.psnitch.getRack(endpoint):"UNKNOWN_RACK");
         }
      }
   }

   public String getLocalDatacenter() {
      return this.myDC;
   }

   public String getLocalRack() {
      return this.myRack;
   }

   public void gossiperStarting() {
      super.gossiperStarting();
      Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(FBUtilities.getLocalAddress().getHostAddress()));
      this.loadGossiperState();
   }

   private void loadGossiperState() {
      assert Gossiper.instance != null;

      ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, this.myDC, this.preferLocal);
      Gossiper.instance.register(pendingHelper);
      pendingHelper = (ReconnectableSnitchHelper)this.snitchHelperReference.getAndSet(pendingHelper);
      if(pendingHelper != null) {
         Gossiper.instance.unregister(pendingHelper);
      }

   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return dc == "UNKNOWN_DC";
   }

   public String toString() {
      return "GossipingPropertyFileSnitch{psnitch=" + this.psnitch + ", myDC='" + this.myDC + '\'' + ", myRack='" + this.myRack + '\'' + ", preferLocal=" + this.preferLocal + ", snitchHelperReference=" + this.snitchHelperReference + '}';
   }
}
