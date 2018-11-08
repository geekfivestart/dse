package org.apache.cassandra.dht.tokenallocator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenAllocation {
   private static final Logger logger = LoggerFactory.getLogger(TokenAllocation.class);

   public TokenAllocation() {
   }

   public static Collection<Token> allocateTokens(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, InetAddress endpoint, int numTokens) {
      TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
      TokenAllocation.StrategyAdapter strategy = getStrategy(tokenMetadataCopy, rs, endpoint);
      Collection<Token> tokens = create(tokenMetadata, strategy).addUnit(endpoint, numTokens);
      tokens = adjustForCrossDatacenterClashes(tokenMetadata, strategy, tokens);
      if(logger.isWarnEnabled()) {
         logger.info("Selected tokens {}", tokens);
         SummaryStatistics os = replicatedOwnershipStats(tokenMetadataCopy, rs, endpoint);
         tokenMetadataCopy.updateNormalTokens(tokens, endpoint);
         SummaryStatistics ns = replicatedOwnershipStats(tokenMetadataCopy, rs, endpoint);
         logger.info("Replicated node load in datacenter before allocation {}", statToString(os));
         logger.info("Replicated node load in datacenter after allocation {}", statToString(ns));
         if(ns.getStandardDeviation() > os.getStandardDeviation()) {
            logger.warn("Unexpected growth in standard deviation after allocation.");
         }
      }

      return tokens;
   }

   public static Collection<Token> allocateTokens(TokenMetadata tokenMetadata, int localReplicationFactor, IEndpointSnitch snitch, InetAddress endpoint, int numTokens) {
      TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
      String dc = snitch.getDatacenter(endpoint);
      TokenAllocation.StrategyAdapter strategy = getStrategy(tokenMetadataCopy, dc, localReplicationFactor, snitch, endpoint);
      Collection<Token> tokens = create(tokenMetadata, strategy).addUnit(endpoint, numTokens);
      tokens = adjustForCrossDatacenterClashes(tokenMetadata, strategy, tokens);
      logger.info("Selected tokens {}", tokens);
      return tokens;
   }

   private static Collection<Token> adjustForCrossDatacenterClashes(TokenMetadata tokenMetadata, TokenAllocation.StrategyAdapter strategy, Collection<Token> tokens) {
      List<Token> filtered = Lists.newArrayListWithCapacity(tokens.size());
      Iterator var4 = tokens.iterator();

      while(var4.hasNext()) {
         Token t;
         for(t = (Token)var4.next(); tokenMetadata.getEndpoint(t) != null; t = t.increaseSlightly()) {
            InetAddress other = tokenMetadata.getEndpoint(t);
            if(strategy.inAllocationRing(other)) {
               throw new ConfigurationException(String.format("Allocated token %s already assigned to node %s. Is another node also allocating tokens?", new Object[]{t, other}));
            }
         }

         filtered.add(t);
      }

      return filtered;
   }

   public static Map<InetAddress, Double> evaluateReplicatedOwnership(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs) {
      Map<InetAddress, Double> ownership = Maps.newHashMap();
      List<Token> sortedTokens = tokenMetadata.sortedTokens();
      Iterator<Token> it = sortedTokens.iterator();

      Token current;
      Token next;
      for(current = (Token)it.next(); it.hasNext(); current = next) {
         next = (Token)it.next();
         addOwnership(tokenMetadata, rs, current, next, ownership);
      }

      addOwnership(tokenMetadata, rs, current, (Token)sortedTokens.get(0), ownership);
      return ownership;
   }

   static void addOwnership(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, Token current, Token next, Map<InetAddress, Double> ownership) {
      double size = current.size(next);
      Token representative = current.getPartitioner().midpoint(current, next);
      Iterator var8 = rs.calculateNaturalEndpoints(representative, tokenMetadata).iterator();

      while(var8.hasNext()) {
         InetAddress n = (InetAddress)var8.next();
         Double v = (Double)ownership.get(n);
         ownership.put(n, Double.valueOf(v != null?v.doubleValue() + size:size));
      }

   }

   public static String statToString(SummaryStatistics stat) {
      return String.format("max %.2f min %.2f stddev %.4f", new Object[]{Double.valueOf(stat.getMax() / stat.getMean()), Double.valueOf(stat.getMin() / stat.getMean()), Double.valueOf(stat.getStandardDeviation())});
   }

   public static SummaryStatistics replicatedOwnershipStats(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, InetAddress endpoint) {
      SummaryStatistics stat = new SummaryStatistics();
      TokenAllocation.StrategyAdapter strategy = getStrategy(tokenMetadata, rs, endpoint);
      Iterator var5 = evaluateReplicatedOwnership(tokenMetadata, rs).entrySet().iterator();

      while(var5.hasNext()) {
         Entry<InetAddress, Double> en = (Entry)var5.next();
         if(strategy.inAllocationRing((InetAddress)en.getKey())) {
            stat.addValue(((Double)en.getValue()).doubleValue() / (double)tokenMetadata.getTokens((InetAddress)en.getKey()).size());
         }
      }

      return stat;
   }

   static TokenAllocator<InetAddress> create(TokenMetadata tokenMetadata, TokenAllocation.StrategyAdapter strategy) {
      NavigableMap<Token, InetAddress> sortedTokens = new TreeMap();
      Iterator var3 = tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Token, InetAddress> en = (Entry)var3.next();
         if(strategy.inAllocationRing((InetAddress)en.getValue())) {
            sortedTokens.put(en.getKey(), en.getValue());
         }
      }

      return TokenAllocatorFactory.createTokenAllocator(sortedTokens, strategy, tokenMetadata.partitioner);
   }

   static TokenAllocation.StrategyAdapter getStrategy(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, InetAddress endpoint) {
      if(rs instanceof NetworkTopologyStrategy) {
         IEndpointSnitch snitch = rs.snitch;
         String dc = snitch.getDatacenter(endpoint);
         return getStrategy(tokenMetadata, dc, ((NetworkTopologyStrategy)rs).getReplicationFactor(dc), snitch, endpoint);
      } else if(rs instanceof SimpleStrategy) {
         return getStrategy(tokenMetadata, rs.getReplicationFactor(), endpoint);
      } else {
         throw new ConfigurationException("Token allocation does not support replication strategy " + rs.getClass().getSimpleName());
      }
   }

   static TokenAllocation.StrategyAdapter getStrategy(TokenMetadata tokenMetadata, final int replicas, InetAddress endpoint) {
      return new TokenAllocation.StrategyAdapter() {
         public int replicas() {
            return replicas;
         }

         public Object getGroup(InetAddress unit) {
            return unit;
         }

         public boolean inAllocationRing(InetAddress other) {
            return true;
         }
      };
   }

   static TokenAllocation.StrategyAdapter getStrategy(TokenMetadata tokenMetadata, final String dc, final int localReplicas, final IEndpointSnitch snitch, InetAddress endpoint) {
      if(localReplicas != 0 && localReplicas != 1) {
         TokenMetadata.Topology topology = tokenMetadata.getTopology();
         Multimap<String, InetAddress> localRacks = (Multimap)topology.getDatacenterRacks().get(dc);
         int racks = localRacks != null?localRacks.asMap().size():1;
         if(racks > localReplicas) {
            return new TokenAllocation.StrategyAdapter() {
               public int replicas() {
                  return localReplicas;
               }

               public Object getGroup(InetAddress unit) {
                  return snitch.getRack(unit);
               }

               public boolean inAllocationRing(InetAddress other) {
                  return dc.equals(snitch.getDatacenter(other));
               }
            };
         } else if(racks == localReplicas) {
            final String rack = snitch.getRack(endpoint);
            return new TokenAllocation.StrategyAdapter() {
               public int replicas() {
                  return 1;
               }

               public Object getGroup(InetAddress unit) {
                  return unit;
               }

               public boolean inAllocationRing(InetAddress other) {
                  return dc.equals(snitch.getDatacenter(other)) && rack.equals(snitch.getRack(other));
               }
            };
         } else if(racks != 1 && topology.getDatacenterEndpoints().get(dc).size() >= localReplicas) {
            throw new ConfigurationException(String.format("Token allocation failed: the number of racks %d in datacenter %s is lower than its replication factor %d.\nIf you are starting a new datacenter, please make sure that the first %d nodes to start are from different racks.", new Object[]{Integer.valueOf(racks), dc, Integer.valueOf(localReplicas), Integer.valueOf(localReplicas)}));
         } else {
            return new TokenAllocation.StrategyAdapter() {
               public int replicas() {
                  return localReplicas;
               }

               public Object getGroup(InetAddress unit) {
                  return unit;
               }

               public boolean inAllocationRing(InetAddress other) {
                  return dc.equals(snitch.getDatacenter(other));
               }
            };
         }
      } else {
         return new TokenAllocation.StrategyAdapter() {
            public int replicas() {
               return 1;
            }

            public Object getGroup(InetAddress unit) {
               return unit;
            }

            public boolean inAllocationRing(InetAddress other) {
               return dc.equals(snitch.getDatacenter(other));
            }
         };
      }
   }

   interface StrategyAdapter extends ReplicationStrategy<InetAddress> {
      boolean inAllocationRing(InetAddress var1);
   }
}
