package org.apache.cassandra.locator;

import java.net.InetAddress;

public class RackInferringSnitch extends AbstractNetworkTopologySnitch {
   public RackInferringSnitch() {
   }

   public String getRack(InetAddress endpoint) {
      return Integer.toString(endpoint.getAddress()[2] & 255, 10);
   }

   public String getDatacenter(InetAddress endpoint) {
      return Integer.toString(endpoint.getAddress()[1] & 255, 10);
   }

   public boolean isDefaultDC(String dc) {
      return false;
   }

   public String toString() {
      return "RackInferringSnitch{, DC='" + this.getLocalDatacenter() + '\'' + ", rack='" + this.getLocalRack() + "'}";
   }
}
