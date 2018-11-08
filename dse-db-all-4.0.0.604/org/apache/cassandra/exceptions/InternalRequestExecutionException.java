package org.apache.cassandra.exceptions;

import org.apache.cassandra.utils.flow.Flow;

public class InternalRequestExecutionException extends RuntimeException implements Flow.NonWrappableException {
   public final RequestFailureReason reason;

   protected InternalRequestExecutionException(RequestFailureReason reason, Throwable cause) {
      super(cause);
      this.reason = reason;
   }

   protected InternalRequestExecutionException(RequestFailureReason reason, String message, Throwable cause) {
      super(message, cause);
      this.reason = reason;
   }

   public InternalRequestExecutionException(RequestFailureReason reason, String message) {
      this(reason, message, (Throwable)null);
   }
}
