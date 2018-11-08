package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class EverywhereStrategy extends NetworkTopologyStrategy {
   private IEndpointSnitch snitch;

   public EverywhereStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException {
      super(keyspaceName, tokenMetadata, snitch, configOptions);
      this.snitch = snitch;
   }

   public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata tokenMetadata) {
      Set<InetAddress> replicas = new LinkedHashSet();
      Iterator<Token> tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), token, false);
      if(tokenIter.hasNext()) {
         replicas.add(tokenMetadata.getEndpoint((Token)tokenIter.next()));
      }

      List<InetAddress> allEndpoints = new ArrayList(tokenMetadata.getAllEndpoints());
      Collections.sort(allEndpoints, new EverywhereStrategy.InetAddressComparator());
      Iterator var6 = allEndpoints.iterator();

      while(var6.hasNext()) {
         InetAddress endpoint = (InetAddress)var6.next();
         replicas.add(endpoint);
      }

      return new ArrayList(replicas);
   }

   public int getReplicationFactor() {
      return StorageService.instance.getTokenMetadata().getAllEndpoints().size();
   }

   public int getReplicationFactor(String dc) {
      int cnt = 0;
      TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
      Iterator var4 = tokenMetadata.getAllEndpoints().iterator();

      while(var4.hasNext()) {
         InetAddress address = (InetAddress)var4.next();
         if(Objects.equals(this.snitch.getDatacenter(address), dc)) {
            ++cnt;
         }
      }

      return cnt;
   }

   public Set<String> getDatacenters() {
      Set<String> dcs = new HashSet();
      TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
      Iterator var3 = tokenMetadata.getAllEndpoints().iterator();

      while(var3.hasNext()) {
         InetAddress address = (InetAddress)var3.next();
         dcs.add(this.snitch.getDatacenter(address));
      }

      return dcs;
   }

   public void validateOptions() throws ConfigurationException {
   }

   protected void validateReplicationFactor(String rf) throws ConfigurationException {
   }

   protected void validateExpectedOptions() throws ConfigurationException {
   }

   public boolean isPartitioned() {
      return false;
   }

   private class InetAddressComparator implements Comparator<InetAddress> {
      private InetAddressComparator() {
      }

      public int compare(InetAddress a1, InetAddress a2) {
         byte[] ba1 = a1.getAddress();
         byte[] ba2 = a2.getAddress();
         return ba1.length < ba2.length?-1:(ba1.length > ba2.length?1:FBUtilities.compareUnsigned(ba1, ba2));
      }
   }
}
