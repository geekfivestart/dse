package org.apache.cassandra.exceptions;

public class TruncateException extends RequestExecutionException {
   public TruncateException(Throwable e) {
      super(ExceptionCode.TRUNCATE_ERROR, "Error during truncate: " + e.getMessage(), e);
   }

   public TruncateException(String msg) {
      super(ExceptionCode.TRUNCATE_ERROR, msg);
   }
}
