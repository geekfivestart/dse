package org.apache.cassandra.locator;

import com.google.common.collect.Multimap;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkTopologyStrategy extends AbstractReplicationStrategy {
   private final Map<String, Integer> datacenters;
   private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

   public NetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException {
      super(keyspaceName, tokenMetadata, snitch, configOptions);
      Map<String, Integer> newDatacenters = new HashMap();
      if(configOptions != null) {
         Iterator var6 = configOptions.entrySet().iterator();

         while(var6.hasNext()) {
            Entry<String, String> entry = (Entry)var6.next();
            String dc = (String)entry.getKey();
            if(dc.equalsIgnoreCase("replication_factor")) {
               throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
            }

            Integer replicas = Integer.valueOf((String)entry.getValue());
            newDatacenters.put(dc, replicas);
         }
      }

      this.datacenters = Collections.unmodifiableMap(newDatacenters);
      logger.trace("Configured datacenter replicas are {}", FBUtilities.toString(this.datacenters));
   }

   public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata) {
      Set<InetAddress> replicas = new LinkedHashSet();
      Set<Pair<String, String>> seenRacks = new HashSet();
      TokenMetadata.Topology topology = tokenMetadata.getTopology();
      Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
      Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();

      assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

      int dcsToFill = 0;
      Map<String, NetworkTopologyStrategy.DatacenterEndpoints> dcs = new HashMap(this.datacenters.size() * 2);
      Iterator tokenIter = this.datacenters.entrySet().iterator();

      while(tokenIter.hasNext()) {
         Entry<String, Integer> en = (Entry)tokenIter.next();
         String dc = (String)en.getKey();
         int rf = ((Integer)en.getValue()).intValue();
         int nodeCount = this.sizeOrZero(allEndpoints.get(dc));
         if(rf > 0 && nodeCount > 0) {
            NetworkTopologyStrategy.DatacenterEndpoints dcEndpoints = new NetworkTopologyStrategy.DatacenterEndpoints(rf, this.sizeOrZero((Multimap)racks.get(dc)), nodeCount, replicas, seenRacks);
            dcs.put(dc, dcEndpoints);
            ++dcsToFill;
         }
      }

      tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);

      while(dcsToFill > 0 && tokenIter.hasNext()) {
         Token next = (Token)tokenIter.next();
         InetAddress ep = tokenMetadata.getEndpoint(next);
         Pair<String, String> location = topology.getLocation(ep);
         NetworkTopologyStrategy.DatacenterEndpoints dcEndpoints = (NetworkTopologyStrategy.DatacenterEndpoints)dcs.get(location.left);
         if(dcEndpoints != null && dcEndpoints.addEndpointAndCheckIfDone(ep, location)) {
            --dcsToFill;
         }
      }

      return new ArrayList(replicas);
   }

   private int sizeOrZero(Multimap<?, ?> collection) {
      return collection != null?collection.asMap().size():0;
   }

   private int sizeOrZero(Collection<?> collection) {
      return collection != null?collection.size():0;
   }

   public int getReplicationFactor() {
      int total = 0;

      int repFactor;
      for(Iterator var2 = this.datacenters.values().iterator(); var2.hasNext(); total += repFactor) {
         repFactor = ((Integer)var2.next()).intValue();
      }

      return total;
   }

   public int getReplicationFactor(String dc) {
      Integer replicas = (Integer)this.datacenters.get(dc);
      return replicas == null?0:replicas.intValue();
   }

   public Set<String> getDatacenters() {
      return this.datacenters.keySet();
   }

   private static Set<String> buildValidDataCentersSet() {
      Set<String> validDataCenters = new HashSet();
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      validDataCenters.add(snitch.getDatacenter(FBUtilities.getBroadcastAddress()));
      Iterator var2 = SystemKeyspace.getHostIds().keySet().iterator();

      while(var2.hasNext()) {
         InetAddress peer = (InetAddress)var2.next();
         validDataCenters.add(snitch.getDatacenter(peer));
      }

      return validDataCenters;
   }

   public Collection<String> recognizedOptions() {
      return buildValidDataCentersSet();
   }

   protected void validateExpectedOptions() throws ConfigurationException {
      if(this.configOptions.isEmpty()) {
         throw new ConfigurationException("Configuration for at least one datacenter must be present");
      } else {
         super.validateExpectedOptions();
      }
   }

   public void validateOptions() throws ConfigurationException {
      Iterator var1 = this.configOptions.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<String, String> e = (Entry)var1.next();
         if(((String)e.getKey()).equalsIgnoreCase("replication_factor")) {
            throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
         }

         this.validateReplicationFactor((String)e.getValue());
      }

   }

   public boolean hasSameSettings(AbstractReplicationStrategy other) {
      return super.hasSameSettings(other) && ((NetworkTopologyStrategy)other).datacenters.equals(this.datacenters);
   }

   private static final class DatacenterEndpoints {
      Set<InetAddress> endpoints;
      Set<Pair<String, String>> racks;
      int rfLeft;
      int acceptableRackRepeats;

      DatacenterEndpoints(int rf, int rackCount, int nodeCount, Set<InetAddress> endpoints, Set<Pair<String, String>> racks) {
         this.endpoints = endpoints;
         this.racks = racks;
         this.rfLeft = Math.min(rf, nodeCount);
         this.acceptableRackRepeats = rf - rackCount;
      }

      boolean addEndpointAndCheckIfDone(InetAddress ep, Pair<String, String> location) {
         if(this.done()) {
            return false;
         } else if(this.racks.add(location)) {
            --this.rfLeft;
            boolean added = this.endpoints.add(ep);

            assert added;

            return this.done();
         } else if(this.acceptableRackRepeats <= 0) {
            return false;
         } else if(!this.endpoints.add(ep)) {
            return false;
         } else {
            --this.acceptableRackRepeats;
            --this.rfLeft;
            return this.done();
         }
      }

      boolean done() {
         assert this.rfLeft >= 0;

         return this.rfLeft == 0;
      }
   }
}
