package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.utils.FBUtilities;

public interface IEndpointSnitch {
   String getRack(InetAddress var1);

   String getDatacenter(InetAddress var1);

   default String getLocalDatacenter() {
      return this.getDatacenter(FBUtilities.getBroadcastAddress());
   }

   default String getLocalRack() {
      return this.getRack(FBUtilities.getBroadcastAddress());
   }

   default String getDisplayName() {
      return this.getClass().getName();
   }

   default boolean isInLocalDatacenter(InetAddress endpoint) {
      return FBUtilities.getBroadcastAddress().equals(endpoint) || this.getLocalDatacenter().equals(this.getDatacenter(endpoint));
   }

   default boolean isInLocalRack(InetAddress endpoint) {
      return this.isInLocalDatacenter(endpoint) && this.getLocalRack().equals(this.getRack(endpoint));
   }

   List<InetAddress> getSortedListByProximity(InetAddress var1, Collection<InetAddress> var2);

   void sortByProximity(InetAddress var1, List<InetAddress> var2);

   int compareEndpoints(InetAddress var1, InetAddress var2, InetAddress var3);

   void gossiperStarting();

   boolean isWorthMergingForRangeQuery(List<InetAddress> var1, List<InetAddress> var2, List<InetAddress> var3);

   long getCrossDcRttLatency(InetAddress var1);

   boolean isDefaultDC(String var1);
}
