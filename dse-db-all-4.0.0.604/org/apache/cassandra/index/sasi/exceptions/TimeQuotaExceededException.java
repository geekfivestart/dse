package org.apache.cassandra.index.sasi.exceptions;

public class TimeQuotaExceededException extends RuntimeException {
   public TimeQuotaExceededException(String message) {
      super(message);
   }
}
