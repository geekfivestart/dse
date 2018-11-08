package org.apache.cassandra.locator;

import java.net.InetAddress;

public abstract class AbstractNetworkTopologySnitch extends AbstractEndpointSnitch {
   public AbstractNetworkTopologySnitch() {
   }

   public abstract String getRack(InetAddress var1);

   public abstract String getDatacenter(InetAddress var1);

   public int compareEndpoints(InetAddress address, InetAddress a1, InetAddress a2) {
      if(address.equals(a1) && !address.equals(a2)) {
         return -1;
      } else if(address.equals(a2) && !address.equals(a1)) {
         return 1;
      } else {
         String addressDatacenter = this.getDatacenter(address);
         String a1Datacenter = this.getDatacenter(a1);
         String a2Datacenter = this.getDatacenter(a2);
         if(addressDatacenter.equals(a1Datacenter) && !addressDatacenter.equals(a2Datacenter)) {
            return -1;
         } else if(addressDatacenter.equals(a2Datacenter) && !addressDatacenter.equals(a1Datacenter)) {
            return 1;
         } else {
            String addressRack = this.getRack(address);
            String a1Rack = this.getRack(a1);
            String a2Rack = this.getRack(a2);
            return addressRack.equals(a1Rack) && !addressRack.equals(a2Rack)?-1:(addressRack.equals(a2Rack) && !addressRack.equals(a1Rack)?1:0);
         }
      }
   }
}
