package com.datastax.bdp.config;

public class CqlSlowLogOptions {
   public static final Integer NUM_SLOWEST_QUERIES_LOWER_BOUND = Integer.valueOf(1);
   public static final Integer NUM_SLOWEST_QUERIES_DEFAULT = Integer.valueOf(5);
   public static final Boolean SKIP_WRITING_TO_DB_DEFAULT = Boolean.valueOf(true);
   public static final Double THRESHOLD_DEFAULT = Double.valueOf(200.0D);
   public static final Boolean ENABLED_DEFAULT = Boolean.valueOf(true);
   public static final Integer TTL_DEFAULT = Integer.valueOf(86400);
   public static final Integer MINIMUM_SAMPLES_DEFAULT = Integer.valueOf(100);
   public boolean enabled;
   public String threshold;
   public String ttl_seconds;
   public String minimum_samples;
   public boolean skip_writing_to_db;
   public String num_slowest_queries;
   /** @deprecated */
   @Deprecated
   public String async_writers;
   /** @deprecated */
   @Deprecated
   String threshold_ms;

   public CqlSlowLogOptions() {
      this.enabled = ENABLED_DEFAULT.booleanValue();
      this.threshold = THRESHOLD_DEFAULT.toString();
      this.ttl_seconds = TTL_DEFAULT.toString();
      this.minimum_samples = MINIMUM_SAMPLES_DEFAULT.toString();
      this.skip_writing_to_db = SKIP_WRITING_TO_DB_DEFAULT.booleanValue();
      this.num_slowest_queries = NUM_SLOWEST_QUERIES_DEFAULT.toString();
   }
}
