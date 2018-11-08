package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TruncateResponseHandler implements MessageCallback<TruncateResponse> {
   protected static final Logger logger = LoggerFactory.getLogger(TruncateResponseHandler.class);
   protected final SimpleCondition condition = new SimpleCondition();
   private final int responseCount;
   protected final AtomicInteger responses = new AtomicInteger(0);
   private final long start;

   TruncateResponseHandler(int responseCount) {
      assert 1 <= responseCount : "invalid response count " + responseCount;

      this.responseCount = responseCount;
      this.start = System.nanoTime();
   }

   public void get() throws TimeoutException {
      long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getTruncateRpcTimeout()) - (System.nanoTime() - this.start);

      boolean success;
      try {
         success = this.condition.await(timeout, TimeUnit.NANOSECONDS);
      } catch (InterruptedException var5) {
         throw new AssertionError(var5);
      }

      if(!success) {
         throw new TimeoutException("Truncate timed out - received only " + this.responses.get() + " responses");
      }
   }

   public void onResponse(Response<TruncateResponse> message) {
      this.responses.incrementAndGet();
      if(this.responses.get() >= this.responseCount) {
         this.condition.signalAll();
      }

   }

   public void onFailure(FailureResponse<TruncateResponse> message) {
   }
}
