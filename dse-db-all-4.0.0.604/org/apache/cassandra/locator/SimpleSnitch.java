package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.List;

public class SimpleSnitch extends AbstractEndpointSnitch {
   private static final String DEFAULT_DC = "datacenter1";
   private static final String DEFAULT_RACK = "rack1";

   public SimpleSnitch() {
   }

   public String getRack(InetAddress endpoint) {
      return "rack1";
   }

   public String getDatacenter(InetAddress endpoint) {
      return "datacenter1";
   }

   public boolean isInLocalDatacenter(InetAddress endpoint) {
      return true;
   }

   public boolean isInLocalRack(InetAddress endpoint) {
      return true;
   }

   public void sortByProximity(InetAddress address, List<InetAddress> addresses) {
   }

   public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) {
      return 0;
   }

   public boolean isDefaultDC(String dc) {
      assert dc != null;

      return dc == "datacenter1";
   }

   public String toString() {
      return "SimpleSnitch{, DC='" + this.getLocalDatacenter() + '\'' + ", rack='" + this.getLocalRack() + "'}";
   }
}
