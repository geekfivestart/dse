package com.datastax.bdp.snitch;

import com.datastax.bdp.gms.DseState;
import java.beans.ConstructorProperties;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.SortedMap;

public interface EndpointStateTrackerMXBean {
   String getDatacenter(String var1);

   String getWorkloads(String var1);

   boolean getIsGraphServer(String var1);

   Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(String var1);

   Boolean getActiveStatus(String var1);

   Boolean getBlacklistedStatus(String var1);

   boolean isBlacklisted();

   void setBlacklisted(boolean var1);

   Double getNodeHealth(String var1);

   boolean vnodesEnabled();

   SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> getRing(String var1) throws UnknownHostException;

   String getServerId(String var1);

   String getServerId();

   public static class NodeStatus {
      public final String address;
      public final String dc;
      public final String rack;
      public final String workload;
      public final String analyticsLabel;
      public final String status;
      public final String state;
      public final String load;
      public final String ownership;
      public final String token;
      public final boolean effective;
      public final String serverId;
      public final boolean isGraphNode;
      public final double health;

      @ConstructorProperties({"address", "dc", "rack", "workload", "status", "state", "load", "ownership", "token", "effective", "serverId", "isGraphNode", "health", "analyticsLabel"})
      public NodeStatus(String address, String dc, String rack, String workload, String status, String state, String load, String ownership, String token, boolean effective, String serverId, boolean isGraphNode, double health, String analyticsLabel) {
         this.address = address;
         this.dc = dc;
         this.rack = rack;
         this.workload = workload;
         this.status = status;
         this.state = state;
         this.load = load;
         this.ownership = ownership;
         this.token = token;
         this.effective = effective;
         this.serverId = serverId;
         this.isGraphNode = isGraphNode;
         this.health = health;
         this.analyticsLabel = analyticsLabel;
      }

      public String getAddress() {
         return this.address;
      }

      public String getDc() {
         return this.dc;
      }

      public String getRack() {
         return this.rack;
      }

      public String getWorkload() {
         return this.workload;
      }

      public String getStatus() {
         return this.status;
      }

      public String getState() {
         return this.state;
      }

      public String getLoad() {
         return this.load;
      }

      public String getOwnership() {
         return this.ownership;
      }

      public String getToken() {
         return this.token;
      }

      public boolean getEffective() {
         return this.effective;
      }

      public String getServerId() {
         return this.serverId;
      }

      public boolean getIsGraphNode() {
         return this.isGraphNode;
      }

      public double getHealth() {
         return this.health;
      }

      public String getAnalyticsLabel() {
         return this.analyticsLabel;
      }
   }
}
