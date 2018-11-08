package org.apache.cassandra.service.paxos;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProposeCallback extends AbstractPaxosCallback<Boolean> {
   private static final Logger logger = LoggerFactory.getLogger(ProposeCallback.class);
   private final AtomicInteger accepts = new AtomicInteger(0);
   private final int requiredAccepts;
   private final boolean failFast;

   public ProposeCallback(int totalTargets, int requiredTargets, boolean failFast, ConsistencyLevel consistency, long queryStartNanoTime) {
      super(totalTargets, consistency, queryStartNanoTime);
      this.requiredAccepts = requiredTargets;
      this.failFast = failFast;
   }

   public void onResponse(Response<Boolean> msg) {
      logger.trace("Propose response {} from {}", msg.payload(), msg.from());
      if(((Boolean)msg.payload()).booleanValue()) {
         this.accepts.incrementAndGet();
      }

      this.latch.countDown();
      if(this.isSuccessful() || this.failFast && this.latch.getCount() + (long)this.accepts.get() < (long)this.requiredAccepts) {
         while(this.latch.getCount() > 0L) {
            this.latch.countDown();
         }
      }

   }

   public int getAcceptCount() {
      return this.accepts.get();
   }

   public boolean isSuccessful() {
      return this.accepts.get() >= this.requiredAccepts;
   }

   public boolean isFullyRefused() {
      return this.latch.getCount() == 0L && this.accepts.get() == 0;
   }
}
