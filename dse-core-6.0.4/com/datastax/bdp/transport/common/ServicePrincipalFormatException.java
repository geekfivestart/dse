package com.datastax.bdp.transport.common;

public class ServicePrincipalFormatException extends RuntimeException {
   public ServicePrincipalFormatException() {
   }

   public ServicePrincipalFormatException(String message) {
      super(message);
   }

   public ServicePrincipalFormatException(String message, Throwable cause) {
      super(message, cause);
   }

   public ServicePrincipalFormatException(Throwable cause) {
      super(cause);
   }
}
