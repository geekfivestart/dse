package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestValidationException;

public class AuthRequestValidationException extends RequestValidationException {
   private static final long serialVersionUID = 7396396573035926056L;

   public AuthRequestValidationException(ExceptionCode code, String msg) {
      super(code, msg);
   }

   public AuthRequestValidationException(ExceptionCode code, String msg, Throwable e) {
      super(code, msg, e);
   }
}
