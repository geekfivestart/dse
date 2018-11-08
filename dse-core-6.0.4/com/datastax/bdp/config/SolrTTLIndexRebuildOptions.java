package com.datastax.bdp.config;

public class SolrTTLIndexRebuildOptions {
   public String fixed_rate_period;
   public String initial_delay;
   public String max_docs_per_batch;
   public String thread_pool_size;

   public SolrTTLIndexRebuildOptions() {
   }
}
