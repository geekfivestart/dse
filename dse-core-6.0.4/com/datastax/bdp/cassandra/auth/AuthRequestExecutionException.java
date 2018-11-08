package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;

public class AuthRequestExecutionException extends RequestExecutionException {
   private static final long serialVersionUID = -6397953915430488156L;

   public AuthRequestExecutionException(ExceptionCode code, String msg) {
      super(code, msg);
   }

   public AuthRequestExecutionException(ExceptionCode code, String msg, Throwable e) {
      super(code, msg, e);
   }
}
