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

public class OldNetworkTopologyStrategy extends AbstractReplicationStrategy {
   public OldNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) {
      super(keyspaceName, tokenMetadata, snitch, configOptions);
   }

   public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata) {
      int replicas = this.getReplicationFactor();
      ArrayList<InetAddress> endpoints = new ArrayList(replicas);
      ArrayList<Token> tokens = metadata.sortedTokens();
      if(tokens.isEmpty()) {
         return endpoints;
      } else {
         Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);
         Token primaryToken = (Token)iter.next();
         endpoints.add(metadata.getEndpoint(primaryToken));
         boolean bDataCenter = false;
         boolean bOtherRack = false;

         Token t;
         while(endpoints.size() < replicas && iter.hasNext()) {
            t = (Token)iter.next();
            if(!this.snitch.getDatacenter(metadata.getEndpoint(primaryToken)).equals(this.snitch.getDatacenter(metadata.getEndpoint(t)))) {
               if(!bDataCenter) {
                  endpoints.add(metadata.getEndpoint(t));
                  bDataCenter = true;
               }
            } else if(!this.snitch.getRack(metadata.getEndpoint(primaryToken)).equals(this.snitch.getRack(metadata.getEndpoint(t))) && this.snitch.getDatacenter(metadata.getEndpoint(primaryToken)).equals(this.snitch.getDatacenter(metadata.getEndpoint(t))) && !bOtherRack) {
               endpoints.add(metadata.getEndpoint(t));
               bOtherRack = true;
            }
         }

         if(endpoints.size() < replicas) {
            iter = TokenMetadata.ringIterator(tokens, token, false);

            while(endpoints.size() < replicas && iter.hasNext()) {
               t = (Token)iter.next();
               if(!endpoints.contains(metadata.getEndpoint(t))) {
                  endpoints.add(metadata.getEndpoint(t));
               }
            }
         }

         return endpoints;
      }
   }

   public int getReplicationFactor() {
      return Integer.parseInt((String)this.configOptions.get("replication_factor"));
   }

   public void validateOptions() throws ConfigurationException {
      if(this.configOptions != null && this.configOptions.get("replication_factor") != null) {
         this.validateReplicationFactor((String)this.configOptions.get("replication_factor"));
      } else {
         throw new ConfigurationException("SimpleStrategy requires a replication_factor strategy option.");
      }
   }

   public Collection<String> recognizedOptions() {
      return Collections.singleton("replication_factor");
   }
}
