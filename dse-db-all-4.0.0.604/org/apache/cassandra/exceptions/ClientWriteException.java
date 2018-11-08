package org.apache.cassandra.exceptions;

public class ClientWriteException extends RequestExecutionException {
   public ClientWriteException(String msg) {
      super(ExceptionCode.CLIENT_WRITE_FAILURE, msg);
   }

   public ClientWriteException(String msg, Throwable cause) {
      super(ExceptionCode.CLIENT_WRITE_FAILURE, msg, cause);
   }
}
