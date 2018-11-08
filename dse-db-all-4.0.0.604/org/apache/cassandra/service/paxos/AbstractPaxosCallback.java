package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;

public abstract class AbstractPaxosCallback<Q> implements MessageCallback<Q> {
   protected final CountDownLatch latch;
   protected final int targets;
   private final ConsistencyLevel consistency;
   private final long queryStartNanoTime;

   public AbstractPaxosCallback(int targets, ConsistencyLevel consistency, long queryStartNanoTime) {
      this.targets = targets;
      this.consistency = consistency;
      this.latch = new CountDownLatch(targets);
      this.queryStartNanoTime = queryStartNanoTime;
   }

   public void onFailure(FailureResponse<Q> response) {
   }

   public void onTimeout(InetAddress host) {
   }

   public int getResponseCount() {
      return (int)((long)this.targets - this.latch.getCount());
   }

   public void await() throws WriteTimeoutException {
      try {
         long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getWriteRpcTimeout()) - (System.nanoTime() - this.queryStartNanoTime);
         if(!this.latch.await(timeout, TimeUnit.NANOSECONDS)) {
            throw new WriteTimeoutException(WriteType.CAS, this.consistency, this.getResponseCount(), this.targets);
         }
      } catch (InterruptedException var3) {
         throw new AssertionError("This latch shouldn't have been interrupted.");
      }
   }
}
