package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class RequestTimeoutException extends RequestExecutionException {
   public final ConsistencyLevel consistency;
   public final int received;
   public final int blockFor;

   protected RequestTimeoutException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor) {
      this(code, String.format("Operation timed out - received only %d responses.", new Object[]{Integer.valueOf(received)}), consistency, received, blockFor);
   }

   protected RequestTimeoutException(ExceptionCode code, String message, ConsistencyLevel consistency, int received, int blockFor) {
      super(code, message);
      this.consistency = consistency;
      this.received = received;
      this.blockFor = blockFor;
   }

   protected RequestTimeoutException(ExceptionCode code, ConsistencyLevel consistency) {
      this(code, "Operation timed out", consistency, 0, 0);
   }
}
