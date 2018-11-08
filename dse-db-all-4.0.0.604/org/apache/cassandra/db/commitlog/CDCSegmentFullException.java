package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

class CDCSegmentFullException extends InternalRequestExecutionException {
   CDCSegmentFullException() {
      super(RequestFailureReason.CDC_SEGMENT_FULL, String.format("Rejecting Mutation containing CDC-enabled table. Free up space in %s.", new Object[]{DatabaseDescriptor.getCDCLogLocation()}));
   }
}
