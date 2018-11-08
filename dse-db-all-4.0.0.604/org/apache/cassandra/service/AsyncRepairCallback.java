package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.WrappedRunnable;

public class AsyncRepairCallback implements MessageCallback<ReadResponse> {
   private final DataResolver repairResolver;
   private final int blockfor;
   protected final AtomicInteger received = new AtomicInteger(0);

   AsyncRepairCallback(DataResolver repairResolver, int blockfor) {
      this.repairResolver = repairResolver;
      this.blockfor = blockfor;
   }

   public void onResponse(Response<ReadResponse> message) {
      this.repairResolver.preprocess(message);
      if(this.received.incrementAndGet() == this.blockfor) {
         StageManager.getStage(Stage.READ_REPAIR).execute(new WrappedRunnable() {
            protected void runMayThrow() {
               AsyncRepairCallback.this.repairResolver.compareResponses().blockingAwait();
            }
         });
      }

   }

   public void onFailure(FailureResponse<ReadResponse> failureResponse) {
   }

   public void onTimeout(InetAddress host) {
   }
}
