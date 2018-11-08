package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.exceptions.AuthenticationException;

public class DseAuthenticationException extends AuthenticationException {
   private static final long serialVersionUID = 1L;
   public static final String reason = "Failed to login. Please re-try.";
   public final String authorizationUser;
   public final String authenticationUser;

   public DseAuthenticationException() {
      this("unknown");
   }

   public DseAuthenticationException(String authorizationUser) {
      super("Failed to login. Please re-try.");
      this.authorizationUser = authorizationUser;
      this.authenticationUser = null;
   }

   public DseAuthenticationException(String authorizationUser, String authenticationUser) {
      super("Failed to login. Please re-try.");
      this.authorizationUser = authorizationUser;
      this.authenticationUser = authenticationUser;
   }
}
