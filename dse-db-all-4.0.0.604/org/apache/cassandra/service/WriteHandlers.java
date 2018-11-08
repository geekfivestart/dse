package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.concurrent.TPCTimer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Response;

abstract class WriteHandlers {
   private WriteHandlers() {
   }

   static class DatacenterSyncHandler extends AbstractWriteHandler {
      private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      private final Map<String, AtomicInteger> responses = new HashMap();
      private final AtomicInteger acks = new AtomicInteger(0);

      DatacenterSyncHandler(WriteEndpoints endpoints, ConsistencyLevel consistency, int blockFor, WriteType writeType, long queryStartNanos, TPCTimer timer) {
         super(endpoints, consistency, blockFor, writeType, queryStartNanos, timer);

         assert consistency == ConsistencyLevel.EACH_QUORUM;

         NetworkTopologyStrategy strategy = (NetworkTopologyStrategy)endpoints.keyspace().getReplicationStrategy();
         Iterator var9 = strategy.getDatacenters().iterator();

         while(var9.hasNext()) {
            String dc = (String)var9.next();
            int rf = strategy.getReplicationFactor(dc);
            this.responses.put(dc, new AtomicInteger(rf / 2 + 1));
         }

         var9 = endpoints.pending().iterator();

         while(var9.hasNext()) {
            InetAddress pending = (InetAddress)var9.next();
            ((AtomicInteger)this.responses.get(snitch.getDatacenter(pending))).incrementAndGet();
         }

      }

      public void onResponse(Response<EmptyPayload> response) {
         ((AtomicInteger)this.responses.get(snitch.getDatacenter(response.from()))).getAndDecrement();
         this.acks.incrementAndGet();
         Iterator var2 = this.responses.values().iterator();

         AtomicInteger i;
         do {
            if(!var2.hasNext()) {
               this.complete((Object)null);
               return;
            }

            i = (AtomicInteger)var2.next();
         } while(i.get() <= 0);

      }

      protected int ackCount() {
         return this.acks.get();
      }
   }

   static class DatacenterLocalHandler extends WriteHandlers.SimpleHandler {
      DatacenterLocalHandler(WriteEndpoints endpoints, ConsistencyLevel consistency, int blockFor, WriteType writeType, long queryStartNanos, TPCTimer timer) {
         super(endpoints, consistency, blockFor, writeType, queryStartNanos, timer);
      }

      protected int pendingToBlockFor() {
         return this.consistency.countLocalEndpoints(this.endpoints.pending());
      }

      protected boolean waitingFor(InetAddress from) {
         return this.consistency.isLocal(from);
      }
   }

   static class SimpleHandler extends AbstractWriteHandler {
      protected final AtomicInteger responses = new AtomicInteger(0);

      SimpleHandler(WriteEndpoints endpoints, ConsistencyLevel consistency, int blockFor, WriteType writeType, long queryStartNanos, TPCTimer timer) {
         super(endpoints, consistency, blockFor, writeType, queryStartNanos, timer);
      }

      public void onResponse(Response<EmptyPayload> response) {
         if(this.waitingFor(response.from())) {
            if(this.responses.incrementAndGet() == this.blockFor) {
               this.complete((Object)null);
            }

         }
      }

      protected int pendingToBlockFor() {
         return this.endpoints.pendingCount();
      }

      protected int ackCount() {
         return this.responses.get();
      }
   }
}
