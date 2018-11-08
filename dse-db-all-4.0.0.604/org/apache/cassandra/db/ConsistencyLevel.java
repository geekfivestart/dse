package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReadRepairDecision;
import org.apache.cassandra.transport.ProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ConsistencyLevel {
   ANY(0),
   ONE(1),
   TWO(2),
   THREE(3),
   QUORUM(4),
   ALL(5),
   LOCAL_QUORUM(6, true),
   EACH_QUORUM(7),
   SERIAL(8),
   LOCAL_SERIAL(9),
   LOCAL_ONE(10, true);

   private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);
   public final int code;
   private final boolean isDCLocal;
   private static final ConsistencyLevel[] codeIdx;

   private ConsistencyLevel(int code) {
      this(code, false);
   }

   private ConsistencyLevel(int code, boolean isDCLocal) {
      this.code = code;
      this.isDCLocal = isDCLocal;
   }

   public static ConsistencyLevel fromCode(int code) {
      if(code >= 0 && code < codeIdx.length) {
         return codeIdx[code];
      } else {
         throw new ProtocolException(String.format("Unknown code %d for a consistency level", new Object[]{Integer.valueOf(code)}));
      }
   }

   private static int quorumFor(Keyspace keyspace) {
      return keyspace.getReplicationStrategy().getReplicationFactor() / 2 + 1;
   }

   private static int localQuorumFor(Keyspace keyspace, String dc) {
      return keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy?((NetworkTopologyStrategy)keyspace.getReplicationStrategy()).getReplicationFactor(dc) / 2 + 1:quorumFor(keyspace);
   }

   public int blockFor(Keyspace keyspace) {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 1:
      case 2:
         return 1;
      case 3:
         return 1;
      case 4:
         return 2;
      case 5:
         return 3;
      case 6:
      case 7:
         return quorumFor(keyspace);
      case 8:
         return keyspace.getReplicationStrategy().getReplicationFactor();
      case 9:
      case 10:
         return localQuorumFor(keyspace, DatabaseDescriptor.getLocalDataCenter());
      case 11:
         if(!(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)) {
            return quorumFor(keyspace);
         }

         NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
         int n = 0;

         String dc;
         for(Iterator var4 = strategy.getDatacenters().iterator(); var4.hasNext(); n += localQuorumFor(keyspace, dc)) {
            dc = (String)var4.next();
         }

         return n;
      default:
         throw new UnsupportedOperationException("Invalid consistency level: " + this.toString());
      }
   }

   public boolean isSingleNode() {
      return this == ONE || this == LOCAL_ONE || this == ANY;
   }

   public boolean isDatacenterLocal() {
      return this.isDCLocal;
   }

   public boolean isLocal(InetAddress endpoint) {
      return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint));
   }

   public int countLocalEndpoints(Iterable<InetAddress> liveEndpoints) {
      int count = 0;
      Iterator var3 = liveEndpoints.iterator();

      while(var3.hasNext()) {
         InetAddress endpoint = (InetAddress)var3.next();
         if(this.isLocal(endpoint)) {
            ++count;
         }
      }

      return count;
   }

   private Map<String, Integer> countPerDCEndpoints(Keyspace keyspace, Iterable<InetAddress> liveEndpoints) {
      NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
      Map<String, Integer> dcEndpoints = new HashMap();
      Iterator var5 = strategy.getDatacenters().iterator();

      while(var5.hasNext()) {
         String dc = (String)var5.next();
         dcEndpoints.put(dc, Integer.valueOf(0));
      }

      var5 = liveEndpoints.iterator();

      while(var5.hasNext()) {
         InetAddress endpoint = (InetAddress)var5.next();
         String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
         dcEndpoints.put(dc, Integer.valueOf(((Integer)dcEndpoints.get(dc)).intValue() + 1));
      }

      return dcEndpoints;
   }

   public ArrayList<InetAddress> filterForQuery(Keyspace keyspace, ArrayList<InetAddress> scratchLiveEndpoints, ReadRepairDecision readRepair) {
      return this.internalFilterForQuery(keyspace, scratchLiveEndpoints, readRepair, (ArrayList)null);
   }

   public void populateForQuery(Keyspace keyspace, ArrayList<InetAddress> scratchLiveEndpoints, ReadRepairDecision readRepair, ArrayList<InetAddress> scratchTargetEndpoints) {
      this.internalFilterForQuery(keyspace, scratchLiveEndpoints, readRepair, scratchTargetEndpoints);
   }

   private ArrayList<InetAddress> internalFilterForQuery(Keyspace keyspace, ArrayList<InetAddress> scratchLiveEndpoints, ReadRepairDecision readRepair, ArrayList<InetAddress> scratchTargetEndpoints) {
      int liveEndpointsCount = scratchLiveEndpoints.size();
      ArrayList<InetAddress> targetEndpoints = scratchTargetEndpoints == null?new ArrayList(liveEndpointsCount):scratchTargetEndpoints;
      if(this == EACH_QUORUM) {
         readRepair = ReadRepairDecision.NONE;
      }

      if(readRepair == ReadRepairDecision.GLOBAL) {
         targetEndpoints.addAll(scratchLiveEndpoints);
         return targetEndpoints;
      } else if(this == EACH_QUORUM && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
         filterForEachQuorum(keyspace, scratchLiveEndpoints, targetEndpoints);
         return targetEndpoints;
      } else {
         if(this.isDCLocal) {
            Collections.sort(scratchLiveEndpoints, DatabaseDescriptor.getLocalComparator());
         }

         int blockFor;
         switch(null.$SwitchMap$org$apache$cassandra$service$ReadRepairDecision[readRepair.ordinal()]) {
         case 1:
            for(blockFor = 0; blockFor < Math.min(liveEndpointsCount, this.blockFor(keyspace)); ++blockFor) {
               targetEndpoints.add(scratchLiveEndpoints.get(blockFor));
            }

            return targetEndpoints;
         case 2:
            for(blockFor = 0; blockFor < liveEndpointsCount; ++blockFor) {
               InetAddress add = (InetAddress)scratchLiveEndpoints.get(blockFor);
               if(this.isLocal(add)) {
                  targetEndpoints.add(add);
               }
            }

            blockFor = this.blockFor(keyspace);

            for(int i = 0; i < liveEndpointsCount && blockFor > targetEndpoints.size(); ++i) {
               InetAddress add = (InetAddress)scratchLiveEndpoints.get(i);
               if(!this.isLocal(add)) {
                  targetEndpoints.add(add);
               }
            }

            return targetEndpoints;
         default:
            throw new AssertionError();
         }
      }
   }

   private static void filterForEachQuorum(Keyspace keyspace, List<InetAddress> scratchLiveEndpoints, List<InetAddress> scratchTargetEndpoints) {
      NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
      Map<String, ArrayList<InetAddress>> dcsEndpoints = new HashMap();
      Iterator var5 = strategy.getDatacenters().iterator();

      while(var5.hasNext()) {
         String dc = (String)var5.next();
         dcsEndpoints.put(dc, new ArrayList());
      }

      for(int i = 0; i < scratchLiveEndpoints.size(); ++i) {
         InetAddress add = (InetAddress)scratchLiveEndpoints.get(i);
         String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(add);
         ((ArrayList)dcsEndpoints.get(dc)).add(add);
      }

      var5 = dcsEndpoints.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<String, ArrayList<InetAddress>> dcEndpoints = (Entry)var5.next();
         ArrayList<InetAddress> dcEndpoint = (ArrayList)dcEndpoints.getValue();
         scratchTargetEndpoints.addAll(dcEndpoint.subList(0, Math.min(localQuorumFor(keyspace, (String)dcEndpoints.getKey()), dcEndpoint.size())));
      }

   }

   public boolean isSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints) {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 2:
         return this.countLocalEndpoints(liveEndpoints) >= 1;
      case 3:
         return true;
      case 9:
         return this.countLocalEndpoints(liveEndpoints) >= this.blockFor(keyspace);
      case 11:
         if(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
            Iterator var3 = this.countPerDCEndpoints(keyspace, liveEndpoints).entrySet().iterator();

            Entry entry;
            do {
               if(!var3.hasNext()) {
                  return true;
               }

               entry = (Entry)var3.next();
            } while(((Integer)entry.getValue()).intValue() >= localQuorumFor(keyspace, (String)entry.getKey()));

            return false;
         }
      case 4:
      case 5:
      case 6:
      case 7:
      case 8:
      case 10:
      default:
         return Iterables.size(liveEndpoints) >= this.blockFor(keyspace);
      }
   }

   public void assureSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints) throws UnavailableException {
      int blockFor = this.blockFor(keyspace);
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 2:
         if(this.countLocalEndpoints(liveEndpoints) == 0) {
            throw new UnavailableException(this, 1, 0);
         }
      case 3:
         break;
      case 9:
         int localLive = this.countLocalEndpoints(liveEndpoints);
         if(localLive < blockFor) {
            if(logger.isTraceEnabled()) {
               StringBuilder builder = new StringBuilder("Local replicas [");
               Iterator var11 = liveEndpoints.iterator();

               while(var11.hasNext()) {
                  InetAddress endpoint = (InetAddress)var11.next();
                  if(this.isLocal(endpoint)) {
                     builder.append(endpoint).append(",");
                  }
               }

               builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
               logger.trace(builder.toString());
            }

            throw new UnavailableException(this, blockFor, localLive);
         }
         break;
      case 11:
         if(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
            Iterator var5 = this.countPerDCEndpoints(keyspace, liveEndpoints).entrySet().iterator();

            Entry entry;
            int dcBlockFor;
            int dcLive;
            do {
               if(!var5.hasNext()) {
                  return;
               }

               entry = (Entry)var5.next();
               dcBlockFor = localQuorumFor(keyspace, (String)entry.getKey());
               dcLive = ((Integer)entry.getValue()).intValue();
            } while(dcLive >= dcBlockFor);

            throw new UnavailableException(this, (String)entry.getKey(), dcBlockFor, dcLive);
         }
      case 4:
      case 5:
      case 6:
      case 7:
      case 8:
      case 10:
      default:
         int live = Iterables.size(liveEndpoints);
         if(live < blockFor) {
            logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(liveEndpoints), Integer.valueOf(blockFor));
            throw new UnavailableException(this, blockFor, live);
         }
      }

   }

   public void validateForRead(String keyspaceName) throws InvalidRequestException {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 3:
         throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
      default:
      }
   }

   public void validateForWrite(String keyspaceName) throws InvalidRequestException {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 7:
      case 10:
         throw new InvalidRequestException("You must use conditional updates for serializable writes");
      default:
      }
   }

   public void validateForCasCommit(String keyspaceName) throws InvalidRequestException {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 7:
      case 10:
         throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
      case 11:
         this.requireNetworkTopologyStrategy(keyspaceName);
      case 8:
      case 9:
      default:
      }
   }

   public void validateForCas() throws InvalidRequestException {
      if(!this.isSerialConsistency()) {
         throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
      }
   }

   public boolean isSerialConsistency() {
      return this == SERIAL || this == LOCAL_SERIAL;
   }

   public void validateCounterForWrite(TableMetadata metadata) throws InvalidRequestException {
      if(this == ANY) {
         throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.name);
      } else if(this.isSerialConsistency()) {
         throw new InvalidRequestException("Counter operations are inherently non-serializable");
      }
   }

   private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException {
      AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
      if(!(strategy instanceof NetworkTopologyStrategy)) {
         throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", new Object[]{this, strategy.getClass().getName()}));
      }
   }

   public boolean isAtLeastQuorum() {
      switch(null.$SwitchMap$org$apache$cassandra$db$ConsistencyLevel[this.ordinal()]) {
      case 6:
      case 8:
      case 11:
         return true;
      default:
         return false;
      }
   }

   static {
      int maxCode = -1;
      ConsistencyLevel[] var1 = values();
      int var2 = var1.length;

      int var3;
      ConsistencyLevel cl;
      for(var3 = 0; var3 < var2; ++var3) {
         cl = var1[var3];
         maxCode = Math.max(maxCode, cl.code);
      }

      codeIdx = new ConsistencyLevel[maxCode + 1];
      var1 = values();
      var2 = var1.length;

      for(var3 = 0; var3 < var2; ++var3) {
         cl = var1[var3];
         if(codeIdx[cl.code] != null) {
            throw new IllegalStateException("Duplicate code");
         }

         codeIdx[cl.code] = cl;
      }

   }
}
