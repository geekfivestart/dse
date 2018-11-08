package org.apache.cassandra.exceptions;

public class UnknownKeyspaceException extends InternalRequestExecutionException {
   public final String keyspaceName;

   public UnknownKeyspaceException(String keyspaceName) {
      super(RequestFailureReason.UNKNOWN_KEYSPACE, "Unknown (possibly just dropped) keyspace " + keyspaceName);
      this.keyspaceName = keyspaceName;
   }
}
