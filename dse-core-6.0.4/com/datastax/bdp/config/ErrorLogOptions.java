package com.datastax.bdp.config;

public class ErrorLogOptions {
   public boolean enabled;
   public String ttl_seconds;
   public String max_writers;

   public ErrorLogOptions() {
   }
}
