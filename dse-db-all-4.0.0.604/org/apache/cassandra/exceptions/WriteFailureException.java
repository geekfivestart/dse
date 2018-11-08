package org.apache.cassandra.exceptions;

import java.net.InetAddress;
import java.util.Map;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class WriteFailureException extends RequestFailureException {
   public final WriteType writeType;

   public WriteFailureException(ConsistencyLevel consistency, int received, int blockFor, WriteType writeType, Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
      super(ExceptionCode.WRITE_FAILURE, consistency, received, blockFor, failureReasonByEndpoint);
      this.writeType = writeType;
   }
}
