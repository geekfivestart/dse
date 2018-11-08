package com.datastax.bdp.config;

public class AuthorizationOptions {
   public Boolean enabled;
   public String transitional_mode;
   public Boolean allow_row_level_security;

   public AuthorizationOptions() {
   }
}
