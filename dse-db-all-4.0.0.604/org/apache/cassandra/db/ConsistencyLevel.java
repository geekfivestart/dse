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
        switch (this) {
            case ONE:
            case LOCAL_ONE: {
                return 1;
            }
            case ANY: {
                return 1;
            }
            case TWO: {
                return 2;
            }
            case THREE: {
                return 3;
            }
            case QUORUM:
            case SERIAL: {
                return ConsistencyLevel.quorumFor(keyspace);
            }
            case ALL: {
                return keyspace.getReplicationStrategy().getReplicationFactor();
            }
            case LOCAL_QUORUM:
            case LOCAL_SERIAL: {
                return ConsistencyLevel.localQuorumFor(keyspace, DatabaseDescriptor.getLocalDataCenter());
            }
            case EACH_QUORUM: {
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
                    int n = 0;
                    for (String dc : strategy.getDatacenters()) {
                        n += ConsistencyLevel.localQuorumFor(keyspace, dc);
                    }
                    return n;
                }
                return ConsistencyLevel.quorumFor(keyspace);
            }
        }
        throw new UnsupportedOperationException("Invalid consistency level: " + this.toString());
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
        ArrayList<InetAddress> targetEndpoints;
        int liveEndpointsCount = scratchLiveEndpoints.size();
        ArrayList<InetAddress> arrayList = targetEndpoints = scratchTargetEndpoints == null ? new ArrayList<InetAddress>(liveEndpointsCount) : scratchTargetEndpoints;
        if (this == EACH_QUORUM) {
            readRepair = ReadRepairDecision.NONE;
        }
        if (readRepair == ReadRepairDecision.GLOBAL) {
            targetEndpoints.addAll(scratchLiveEndpoints);
            return targetEndpoints;
        }
        if (this == EACH_QUORUM && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
            ConsistencyLevel.filterForEachQuorum(keyspace, scratchLiveEndpoints, targetEndpoints);
            return targetEndpoints;
        }
        if (this.isDCLocal) {
            Collections.sort(scratchLiveEndpoints, DatabaseDescriptor.getLocalComparator());
        }
        switch (readRepair) {
            case NONE: {
                for (int i = 0; i < Math.min(liveEndpointsCount, this.blockFor(keyspace)); ++i) {
                    targetEndpoints.add(scratchLiveEndpoints.get(i));
                }
                return targetEndpoints;
            }
            case DC_LOCAL: {
                for (int i = 0; i < liveEndpointsCount; ++i) {
                    InetAddress add = scratchLiveEndpoints.get(i);
                    if (!this.isLocal(add)) continue;
                    targetEndpoints.add(add);
                }
                int blockFor = this.blockFor(keyspace);
                for (int i = 0; i < liveEndpointsCount && blockFor > targetEndpoints.size(); ++i) {
                    InetAddress add = scratchLiveEndpoints.get(i);
                    if (this.isLocal(add)) continue;
                    targetEndpoints.add(add);
                }
                return targetEndpoints;
            }
        }
        throw new AssertionError();
    }



    private static void filterForEachQuorum(Keyspace keyspace, List<InetAddress> scratchLiveEndpoints, List<InetAddress> scratchTargetEndpoints) {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)keyspace.getReplicationStrategy();
        Map<String, ArrayList<InetAddress>> dcsEndpoints = new HashMap();
        for (String dc : strategy.getDatacenters()) {
            dcsEndpoints.put(dc, new ArrayList());
        }
        for (int i = 0; i < scratchLiveEndpoints.size(); ++i) {
            InetAddress add = scratchLiveEndpoints.get(i);
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(add);
            ((ArrayList)dcsEndpoints.get(dc)).add(add);
        }
        for (Map.Entry dcEndpoints : dcsEndpoints.entrySet()) {
            ArrayList dcEndpoint = (ArrayList)dcEndpoints.getValue();
            scratchTargetEndpoints.addAll(dcEndpoint.subList(0, Math.min(ConsistencyLevel.localQuorumFor(keyspace, (String)dcEndpoints.getKey()), dcEndpoint.size())));
        }
    }


    public boolean isSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints) {
        switch (this) {
            case ANY: {
                return true;
            }
            case LOCAL_ONE: {
                return this.countLocalEndpoints(liveEndpoints) >= 1;
            }
            case LOCAL_QUORUM: {
                return this.countLocalEndpoints(liveEndpoints) >= this.blockFor(keyspace);
            }
            case EACH_QUORUM: {
                if (!(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)) break;
                for (Map.Entry<String, Integer> entry : this.countPerDCEndpoints(keyspace, liveEndpoints).entrySet()) {
                    if (entry.getValue() >= ConsistencyLevel.localQuorumFor(keyspace, entry.getKey())) continue;
                    return false;
                }
                return true;
            }
        }
        return Iterables.size(liveEndpoints) >= this.blockFor(keyspace);
    }

    public void assureSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints) throws UnavailableException {
        int blockFor = this.blockFor(keyspace);
        switch (this) {
            case ANY: {
                break;
            }
            case LOCAL_ONE: {
                if (this.countLocalEndpoints(liveEndpoints) != 0) break;
                throw new UnavailableException(this, 1, 0);
            }
            case LOCAL_QUORUM: {
                int localLive = this.countLocalEndpoints(liveEndpoints);
                if (localLive >= blockFor) break;
                if (logger.isTraceEnabled()) {
                    StringBuilder builder = new StringBuilder("Local replicas [");
                    for (InetAddress endpoint : liveEndpoints) {
                        if (!this.isLocal(endpoint)) continue;
                        builder.append(endpoint).append(",");
                    }
                    builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
                    logger.trace(builder.toString());
                }
                throw new UnavailableException(this, blockFor, localLive);
            }
            case EACH_QUORUM: {
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
                    for (Map.Entry<String, Integer> entry : this.countPerDCEndpoints(keyspace, liveEndpoints).entrySet()) {
                        int dcBlockFor = ConsistencyLevel.localQuorumFor(keyspace, entry.getKey());
                        int dcLive = entry.getValue();
                        if (dcLive >= dcBlockFor) continue;
                        throw new UnavailableException(this, entry.getKey(), dcBlockFor, dcLive);
                    }
                    break;
                }
            }
            default: {
                int live = Iterables.size(liveEndpoints);
                if (live >= blockFor) break;
                logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", (Object)Iterables.toString(liveEndpoints), (Object)blockFor);
                throw new UnavailableException(this, blockFor, live);
            }
        }
    }

    public void validateForRead(String keyspaceName) throws InvalidRequestException {
        switch (this) {
            case ANY: {
                throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
            }
        }
    }

    public void validateForWrite(String keyspaceName) throws InvalidRequestException {
        switch (this) {
            case SERIAL:
            case LOCAL_SERIAL: {
                throw new InvalidRequestException("You must use conditional updates for serializable writes");
            }
        }
    }

    public void validateForCasCommit(String keyspaceName) throws InvalidRequestException {
        switch (this) {
            case EACH_QUORUM: {
                this.requireNetworkTopologyStrategy(keyspaceName);
                break;
            }
            case SERIAL:
            case LOCAL_SERIAL: {
                throw new InvalidRequestException((Object)((Object)this) + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
            }
        }
    }

    public void validateForCas() throws InvalidRequestException {
        if (!this.isSerialConsistency()) {
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
        }
    }

    public boolean isSerialConsistency() {
        return this == SERIAL || this == LOCAL_SERIAL;
    }

    public void validateCounterForWrite(TableMetadata metadata) throws InvalidRequestException {
        if (this == ANY) {
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.name);
        }
        if (this.isSerialConsistency()) {
            throw new InvalidRequestException("Counter operations are inherently non-serializable");
        }
    }

    private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy)) {
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", new Object[]{this, strategy.getClass().getName()}));
        }
    }

    public boolean isAtLeastQuorum() {
        switch (this) {
            case QUORUM:
            case ALL:
            case EACH_QUORUM: {
                return true;
            }
        }
        return false;
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
