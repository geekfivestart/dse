package com.datastax.bdp.snitch;

import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.util.Addresses;
import com.google.common.base.Joiner;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseSimpleSnitch extends AbstractNetworkTopologySnitch {
   protected static final Logger logger = LoggerFactory.getLogger(DseSimpleSnitch.class);
   public static final String ANALYTICS_DC = "Analytics";
   public static final String CASSANDRA_DC = "Cassandra";
   public static final String SEARCH_DC = "Solr";
   public static final String SEARCH_ANALYTICS_DC = "SearchAnalytics";
   public static final String GRAPH_DC = "Graph";
   public static final String UNKNOWN_DC = "Unknown";
   protected final String myDC;
   private volatile Map<InetAddress, Map<String, String>> savedEndpoints;

   public DseSimpleSnitch() {
      this(SystemInfo.isAnalyticsNode(), SystemInfo.isSearchNode(), SystemInfo.isGraphNode());
   }

   public DseSimpleSnitch(boolean analytics, boolean search, boolean graph) {
      Set<String> workloads = new HashSet();
      if(search) {
         workloads.add(!analytics && !graph?"Solr":"Search");
      }

      if(analytics) {
         workloads.add("Analytics");
      }

      if(graph) {
         workloads.add("Graph");
      }

      if(workloads.isEmpty()) {
         workloads.add("Cassandra");
      }

      List<String> workloadList = new ArrayList(workloads);
      Collections.sort(workloadList);
      Collections.reverse(workloadList);
      this.myDC = Joiner.on("").join(workloadList);
   }

   public String getDatacenter(InetAddress endpoint) {
      String dc = null;
      if(Addresses.Internode.isLocalEndpoint(endpoint)) {
         dc = this.myDC;
      } else {
         try {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if(epState != null && epState.getApplicationState(ApplicationState.DC) != null) {
               dc = epState.getApplicationState(ApplicationState.DC).value;
            } else {
               if(this.savedEndpoints == null) {
                  this.savedEndpoints = SystemKeyspace.loadDcRackInfo();
               }

               Map<String, String> endpointAttributes = (Map)this.savedEndpoints.get(endpoint);
               if(endpointAttributes != null) {
                  dc = (String)endpointAttributes.get("data_center");
               }
            }
         } catch (Exception var5) {
            logger.warn("Failed to find data center for " + endpoint.toString(), var5);
         }

         if(dc == null) {
            dc = "Unknown";
         }
      }

      if(logger.isTraceEnabled()) {
         logger.trace("Datacenter for endpoint " + endpoint.getHostAddress() + " is " + dc);
      }

      return dc;
   }

   public void gossiperStarting() {
      Gossiper.instance.addLocalApplicationState(ApplicationState.DC, StorageService.instance.valueFactory.datacenter(this.myDC));
   }

   public boolean isDefaultDC(String dc) {
      return "Unknown".equals(dc);
   }

   public String getRack(InetAddress arg0) {
      return "rack1";
   }

   public String toString() {
      return "DseSimpleSnitch{myDC=" + this.myDC + ", rack=rack1}";
   }
}
