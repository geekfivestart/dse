package com.datastax.bdp.config;

public class NodeHealthOptions {
   public String refresh_rate_ms;
   public String uptime_ramp_up_period_seconds;
   public String dropped_mutation_window_minutes;

   public NodeHealthOptions() {
   }
}
