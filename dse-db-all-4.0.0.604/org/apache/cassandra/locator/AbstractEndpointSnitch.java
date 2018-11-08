package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;

public abstract class AbstractEndpointSnitch implements IEndpointSnitch {
   public AbstractEndpointSnitch() {
   }

   public abstract int compareEndpoints(InetAddress var1, InetAddress var2, InetAddress var3);

   public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress) {
      List<InetAddress> preferred = new ArrayList(unsortedAddress);
      this.sortByProximity(address, preferred);
      return preferred;
   }

   public void sortByProximity(final InetAddress address, List<InetAddress> addresses) {
      Collections.sort(addresses, new Comparator<InetAddress>() {
         public int compare(InetAddress a1, InetAddress a2) {
            return AbstractEndpointSnitch.this.compareEndpoints(address, a1, a2);
         }
      });
   }

   public void gossiperStarting() {
   }

   public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2) {
      boolean mergedHasRemote = this.hasRemoteNode(merged);
      return mergedHasRemote?this.hasRemoteNode(l1) || this.hasRemoteNode(l2):true;
   }

   private boolean hasRemoteNode(List<InetAddress> l) {
      Iterator var2 = l.iterator();

      InetAddress ep;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         ep = (InetAddress)var2.next();
      } while(this.isInLocalDatacenter(ep));

      return true;
   }

   public long getCrossDcRttLatency(InetAddress endpoint) {
      long crossDCLatency = DatabaseDescriptor.getCrossDCRttLatency();

      assert crossDCLatency >= 0L;

      return crossDCLatency != 0L && !this.isInLocalDatacenter(endpoint)?crossDCLatency:0L;
   }

   public boolean isDefaultDC(String dc) {
      return false;
   }
}
