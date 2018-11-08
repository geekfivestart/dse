package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;

public class SimpleStrategy extends AbstractReplicationStrategy {
   public SimpleStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) {
      super(keyspaceName, tokenMetadata, snitch, configOptions);
   }

   public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata) {
      int replicas = this.getReplicationFactor();
      ArrayList<Token> tokens = metadata.sortedTokens();
      ArrayList<InetAddress> endpoints = new ArrayList(replicas);
      if(tokens.isEmpty()) {
         return endpoints;
      } else {
         Iterator iter = TokenMetadata.ringIterator(tokens, token, false);

         while(endpoints.size() < replicas && iter.hasNext()) {
            InetAddress ep = metadata.getEndpoint((Token)iter.next());
            if(!endpoints.contains(ep)) {
               endpoints.add(ep);
            }
         }

         return endpoints;
      }
   }

   public int getReplicationFactor() {
      return Integer.parseInt((String)this.configOptions.get("replication_factor"));
   }

   public void validateOptions() throws ConfigurationException {
      String rf = (String)this.configOptions.get("replication_factor");
      if(rf == null) {
         throw new ConfigurationException("SimpleStrategy requires a replication_factor strategy option.");
      } else {
         this.validateReplicationFactor(rf);
      }
   }

   public Collection<String> recognizedOptions() {
      return Collections.singleton("replication_factor");
   }
}
