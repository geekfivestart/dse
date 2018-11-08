package org.apache.cassandra.exceptions;

import java.net.InetAddress;
import java.util.Map;
import org.apache.cassandra.db.ConsistencyLevel;

public class ReadFailureException extends RequestFailureException {
   public final boolean dataPresent;

   public ReadFailureException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
      super(ExceptionCode.READ_FAILURE, consistency, received, blockFor, failureReasonByEndpoint);
      this.dataPresent = dataPresent;
   }
}
