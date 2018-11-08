package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.utils.flow.Flow;

public class AbortedOperationException extends RuntimeException implements Flow.NonWrappableException {
   public AbortedOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }

   public AbortedOperationException() {
   }
}
