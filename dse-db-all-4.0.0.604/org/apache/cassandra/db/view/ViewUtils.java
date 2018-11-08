package org.apache.cassandra.db.view;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.utils.FBUtilities;

public final class ViewUtils {
   private ViewUtils() {
   }

   public static Optional<InetAddress> getViewNaturalEndpoint(String keyspaceName, Token baseToken, Token viewToken) {
      AbstractReplicationStrategy replicationStrategy = Keyspace.open(keyspaceName).getReplicationStrategy();
      List<InetAddress> baseEndpoints = new ArrayList();
      List<InetAddress> viewEndpoints = new ArrayList();
      Iterator var6 = replicationStrategy.getNaturalEndpoints(baseToken).iterator();

      while(true) {
         InetAddress viewEndpoint;
         do {
            if(!var6.hasNext()) {
               var6 = replicationStrategy.getNaturalEndpoints(viewToken).iterator();

               while(true) {
                  while(var6.hasNext()) {
                     viewEndpoint = (InetAddress)var6.next();
                     if(viewEndpoint.equals(FBUtilities.getBroadcastAddress())) {
                        return Optional.of(viewEndpoint);
                     }

                     if(baseEndpoints.contains(viewEndpoint)) {
                        baseEndpoints.remove(viewEndpoint);
                     } else if(!(replicationStrategy instanceof NetworkTopologyStrategy) || DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(viewEndpoint)) {
                        viewEndpoints.add(viewEndpoint);
                     }
                  }

                  assert baseEndpoints.size() == viewEndpoints.size() : "Replication strategy should have the same number of endpoints for the base and the view";

                  int baseIdx = baseEndpoints.indexOf(FBUtilities.getBroadcastAddress());
                  if(baseIdx < 0) {
                     return Optional.empty();
                  }

                  return Optional.of(viewEndpoints.get(baseIdx));
               }
            }

            viewEndpoint = (InetAddress)var6.next();
         } while(replicationStrategy instanceof NetworkTopologyStrategy && !DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(viewEndpoint));

         baseEndpoints.add(viewEndpoint);
      }
   }
}
