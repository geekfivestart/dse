package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ReadRepairDecision;

public class ReadContext {
   public final Keyspace keyspace;
   public final ConsistencyLevel consistencyLevel;
   @Nullable
   public final ClientState clientState;
   public final long queryStartNanos;
   public final boolean withDigests;
   public final boolean forContinuousPaging;
   private final boolean blockForAllReplicas;
   @Nullable
   public final ReadReconciliationObserver readObserver;
   public final ReadRepairDecision readRepairDecision;
   private final int consistencyBlockFor;
   private final long readRepairTimeoutInMs;

   private ReadContext(Keyspace keyspace, ConsistencyLevel consistencyLevel, ClientState clientState, long queryStartNanos, boolean withDigests, boolean forContinuousPaging, boolean blockForAllReplicas, ReadReconciliationObserver readObserver, ReadRepairDecision readRepairDecision, long readRepairTimeoutInMs) {
      this.keyspace = keyspace;
      this.consistencyLevel = consistencyLevel;
      this.clientState = clientState;
      this.queryStartNanos = queryStartNanos;
      this.withDigests = withDigests;
      this.forContinuousPaging = forContinuousPaging;
      this.blockForAllReplicas = blockForAllReplicas;
      this.readObserver = readObserver;
      this.readRepairDecision = readRepairDecision;
      this.consistencyBlockFor = consistencyLevel.blockFor(keyspace);
      this.readRepairTimeoutInMs = readRepairTimeoutInMs;
   }

   public static ReadContext.Builder builder(ReadQuery query, ConsistencyLevel consistencyLevel) {
      Keyspace keyspace = Keyspace.open(query.metadata().keyspace);
      return query.applyDefaults(new ReadContext.Builder(keyspace, consistencyLevel));
   }

   public ReadContext withConsistency(ConsistencyLevel newConsistencyLevel) {
      return new ReadContext(this.keyspace, newConsistencyLevel, this.clientState, this.queryStartNanos, this.withDigests, this.forContinuousPaging, this.blockForAllReplicas, this.readObserver, this.readRepairDecision, this.readRepairTimeoutInMs);
   }

   public ReadContext withObserver(ReadReconciliationObserver observer) {
      return new ReadContext(this.keyspace, this.consistencyLevel, this.clientState, this.queryStartNanos, this.withDigests, this.forContinuousPaging, this.blockForAllReplicas, observer, this.readRepairDecision, this.readRepairTimeoutInMs);
   }

   public ReadContext withStartTime(long queryStartNanos) {
      return new ReadContext(this.keyspace, this.consistencyLevel, this.clientState, queryStartNanos, this.withDigests, this.forContinuousPaging, this.blockForAllReplicas, this.readObserver, this.readRepairDecision, this.readRepairTimeoutInMs);
   }

   public ReadContext forNewQuery(long newQueryStartNanos) {
      return new ReadContext(this.keyspace, this.consistencyLevel, this.clientState, newQueryStartNanos, this.withDigests, this.forContinuousPaging, this.blockForAllReplicas, this.readObserver, this.readRepairDecision, this.readRepairTimeoutInMs);
   }

   public ArrayList<InetAddress> filterForQuery(ArrayList<InetAddress> scratchLiveEndpoints) {
      return this.consistencyLevel.filterForQuery(this.keyspace, scratchLiveEndpoints, this.readRepairDecision);
   }

   public void populateForQuery(ArrayList<InetAddress> scratchLiveEndpoints, ArrayList<InetAddress> scratchTargetEndpoints) {
      this.consistencyLevel.populateForQuery(this.keyspace, scratchLiveEndpoints, this.readRepairDecision, scratchTargetEndpoints);
   }

   public int blockFor(List<InetAddress> targets) {
      return this.blockForAllReplicas?targets.size():this.consistencyBlockFor;
   }

   public int requiredResponses() {
      return this.consistencyBlockFor;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.consistencyLevel).append('{');
      sb.append(this.withDigests?"digests":"no digests");
      sb.append(", read repair=").append(this.readRepairDecision);
      if(this.forContinuousPaging) {
         sb.append(", for continuous paging");
      }

      if(this.blockForAllReplicas) {
         sb.append(", block on all replicas");
      }

      if(this.clientState != null) {
         sb.append(", has ClientState");
      }

      if(this.readObserver != null) {
         sb.append(", has observer");
      }

      return sb.append('}').toString();
   }

   public static class Builder {
      private final Keyspace keyspace;
      private final ConsistencyLevel consistencyLevel;
      private ClientState clientState;
      private boolean withDigests;
      private boolean forContinuousPaging;
      private boolean blockForAllReplicas;
      private ReadReconciliationObserver readObserver;
      private ReadRepairDecision readRepairDecision;
      private long readRepairTimeoutInMs;

      private Builder(Keyspace keyspace, ConsistencyLevel consistencyLevel) {
         this.readRepairDecision = ReadRepairDecision.NONE;
         this.readRepairTimeoutInMs = DatabaseDescriptor.getWriteRpcTimeout();
         this.keyspace = keyspace;
         this.consistencyLevel = consistencyLevel;
      }

      public ReadContext.Builder state(ClientState clientState) {
         this.clientState = clientState;
         return this;
      }

      public ReadContext.Builder useDigests() {
         return this.useDigests(true);
      }

      public ReadContext.Builder useDigests(boolean withDigests) {
         this.withDigests = withDigests;
         return this;
      }

      public ReadContext.Builder forContinuousPaging() {
         this.forContinuousPaging = true;
         return this;
      }

      public ReadContext.Builder blockForAllTargets() {
         this.blockForAllReplicas = true;
         return this;
      }

      public ReadContext.Builder observer(ReadReconciliationObserver observer) {
         this.readObserver = observer;
         return this;
      }

      public ReadContext.Builder readRepairDecision(ReadRepairDecision readRepairDecision) {
         this.readRepairDecision = readRepairDecision;
         return this;
      }

      public ReadContext.Builder readRepairTimeoutInMs(long readRepairTimeoutInMs) {
         this.readRepairTimeoutInMs = readRepairTimeoutInMs;
         return this;
      }

      public ReadContext build(long queryStartNanos) {
         return new ReadContext(this.keyspace, this.consistencyLevel, this.clientState, queryStartNanos, this.withDigests, this.forContinuousPaging, this.blockForAllReplicas, this.readObserver, this.readRepairDecision, this.readRepairTimeoutInMs);
      }
   }
}
