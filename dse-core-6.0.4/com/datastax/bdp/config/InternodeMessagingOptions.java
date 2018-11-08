package com.datastax.bdp.config;

public class InternodeMessagingOptions {
   public String server_acceptor_threads;
   public String server_worker_threads;
   public String client_max_connections;
   public String client_worker_threads;
   public String handshake_timeout_seconds;
   public String port;
   public String frame_length_in_mb;
   public String client_request_timeout_seconds;

   public InternodeMessagingOptions() {
   }
}
