package org.apache.cassandra.db;

import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

class CounterForwardingException extends InternalRequestExecutionException {
   CounterForwardingException(Throwable t) {
      super(RequestFailureReason.COUNTER_FORWARDING_FAILURE, t);
   }
}
