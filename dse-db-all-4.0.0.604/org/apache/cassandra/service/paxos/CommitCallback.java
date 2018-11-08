package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitCallback extends AbstractPaxosCallback<EmptyPayload> {
   private static final Logger logger = LoggerFactory.getLogger(CommitCallback.class);
   private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap();

   public CommitCallback(int targets, ConsistencyLevel consistency, long queryStartNanoTime) {
      super(targets, consistency, queryStartNanoTime);
   }

   public void onFailure(FailureResponse<EmptyPayload> msg) {
      logger.trace("Failure response to commit from {}", msg.from());
      this.failureReasonByEndpoint.put(msg.from(), msg.reason());
      this.latch.countDown();
   }

   public void onResponse(Response<EmptyPayload> msg) {
      logger.trace("Commit response from {}", msg.from());
      this.latch.countDown();
   }

   public Map<InetAddress, RequestFailureReason> getFailureReasons() {
      return this.failureReasonByEndpoint;
   }
}
