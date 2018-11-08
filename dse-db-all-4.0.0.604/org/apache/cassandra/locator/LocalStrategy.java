package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public class LocalStrategy extends AbstractReplicationStrategy {
   private final List<InetAddress> naturalEndpoints = Collections.singletonList(FBUtilities.getBroadcastAddress());

   public LocalStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) {
      super(keyspaceName, tokenMetadata, snitch, configOptions);
   }

   public List<InetAddress> getCachedNaturalEndpoints(RingPosition searchPosition) {
      return this.naturalEndpoints;
   }

   public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata) {
      return this.naturalEndpoints;
   }

   public int getReplicationFactor() {
      return 1;
   }

   public void validateOptions() throws ConfigurationException {
   }

   public Collection<String> recognizedOptions() {
      return Collections.emptySet();
   }

   public boolean isPartitioned() {
      return false;
   }
}
