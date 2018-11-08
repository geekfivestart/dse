package org.apache.cassandra.exceptions;

public class InvalidRequestException extends RequestValidationException {
   public InvalidRequestException(String msg) {
      super(ExceptionCode.INVALID, msg);
   }

   public InvalidRequestException(String msg, Throwable t) {
      super(ExceptionCode.INVALID, msg, t);
   }
}
