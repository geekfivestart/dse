package com.datastax.bdp.config;

import java.util.Objects;
import java.util.Set;

public class DseFsOptions {
   public Boolean enabled;
   public String keyspace_name;
   public String work_dir;
   public String public_port;
   public String private_port;
   public String service_startup_timeout_ms;
   public String service_close_timeout_ms;
   public String server_close_timeout_ms;
   public String compression_frame_max_size;
   public String query_cache_size;
   public String query_cache_expire_after_ms;
   public Set<DseFsOptions.DseFsDataDirectoryOption> data_directories;
   public DseFsOptions.RestOptions rest_options;
   public DseFsOptions.GossipOptions gossip_options;
   public DseFsOptions.TransactionOptions transaction_options;
   public DseFsOptions.BlockAllocatorOptions block_allocator_options;

   public DseFsOptions() {
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         DseFsOptions that = (DseFsOptions)o;
         return Objects.equals(this.enabled, that.enabled) && Objects.equals(this.keyspace_name, that.keyspace_name) && Objects.equals(this.work_dir, that.work_dir) && Objects.equals(this.public_port, that.public_port) && Objects.equals(this.private_port, that.private_port) && Objects.equals(this.service_startup_timeout_ms, that.service_startup_timeout_ms) && Objects.equals(this.service_close_timeout_ms, that.service_close_timeout_ms) && Objects.equals(this.server_close_timeout_ms, that.server_close_timeout_ms) && Objects.equals(this.compression_frame_max_size, that.compression_frame_max_size) && Objects.equals(this.query_cache_size, that.query_cache_size) && Objects.equals(this.query_cache_expire_after_ms, that.query_cache_expire_after_ms) && Objects.equals(this.data_directories, that.data_directories) && Objects.equals(this.rest_options, that.rest_options) && Objects.equals(this.gossip_options, that.gossip_options) && Objects.equals(this.transaction_options, that.transaction_options) && Objects.equals(this.block_allocator_options, that.block_allocator_options);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.enabled, this.keyspace_name, this.work_dir, this.public_port, this.private_port, this.service_startup_timeout_ms, this.service_close_timeout_ms, this.server_close_timeout_ms, this.compression_frame_max_size, this.query_cache_size, this.query_cache_expire_after_ms, this.data_directories, this.rest_options, this.gossip_options, this.transaction_options, this.block_allocator_options});
   }

   public static class BlockAllocatorOptions {
      public String overflow_margin_mb;
      public String overflow_factor;

      public BlockAllocatorOptions() {
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            DseFsOptions.BlockAllocatorOptions that = (DseFsOptions.BlockAllocatorOptions)o;
            return Objects.equals(this.overflow_margin_mb, that.overflow_margin_mb) && Objects.equals(this.overflow_factor, that.overflow_factor);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.overflow_margin_mb, this.overflow_factor});
      }
   }

   public static class DseFsDataDirectoryOption {
      public String dir;
      public String min_free_space;
      public String storage_weight;

      public DseFsDataDirectoryOption() {
      }

      DseFsOptions.DseFsDataDirectoryOption setMinFreeSpace(String minFreeSpace) {
         this.min_free_space = minFreeSpace;
         return this;
      }

      DseFsOptions.DseFsDataDirectoryOption setStorageWeight(String storageWeight) {
         this.storage_weight = storageWeight;
         return this;
      }

      DseFsOptions.DseFsDataDirectoryOption setDir(String dir) {
         this.dir = dir;
         return this;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            DseFsOptions.DseFsDataDirectoryOption that = (DseFsOptions.DseFsDataDirectoryOption)o;
            return Objects.equals(this.dir, that.dir) && Objects.equals(this.min_free_space, that.min_free_space) && Objects.equals(this.storage_weight, that.storage_weight);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.dir, this.min_free_space, this.storage_weight});
      }
   }

   public static class RestOptions {
      public String server_request_timeout_ms;
      public String request_timeout_ms;
      public String connection_open_timeout_ms;
      public String idle_connection_timeout_ms;
      public String internode_idle_connection_timeout_ms;
      public String client_close_timeout_ms;
      public String core_max_concurrent_connections_per_host;

      public RestOptions() {
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            DseFsOptions.RestOptions that = (DseFsOptions.RestOptions)o;
            return Objects.equals(this.server_request_timeout_ms, that.server_request_timeout_ms) && Objects.equals(this.request_timeout_ms, that.request_timeout_ms) && Objects.equals(this.connection_open_timeout_ms, that.connection_open_timeout_ms) && Objects.equals(this.idle_connection_timeout_ms, that.idle_connection_timeout_ms) && Objects.equals(this.internode_idle_connection_timeout_ms, that.internode_idle_connection_timeout_ms) && Objects.equals(this.client_close_timeout_ms, that.client_close_timeout_ms) && Objects.equals(this.core_max_concurrent_connections_per_host, that.core_max_concurrent_connections_per_host);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.server_request_timeout_ms, this.request_timeout_ms, this.connection_open_timeout_ms, this.idle_connection_timeout_ms, this.internode_idle_connection_timeout_ms, this.client_close_timeout_ms, this.core_max_concurrent_connections_per_host});
      }
   }

   public static class GossipOptions {
      public String startup_delay_ms;
      public String round_delay_ms;
      public String shutdown_delay_ms;

      public GossipOptions() {
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            DseFsOptions.GossipOptions that = (DseFsOptions.GossipOptions)o;
            return Objects.equals(this.startup_delay_ms, that.startup_delay_ms) && Objects.equals(this.round_delay_ms, that.round_delay_ms) && Objects.equals(this.shutdown_delay_ms, that.shutdown_delay_ms);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.startup_delay_ms, this.round_delay_ms, this.shutdown_delay_ms});
      }
   }

   public static class TransactionOptions {
      public String transaction_timeout_ms;
      public String conflict_retry_delay_ms;
      public String conflict_retry_count;
      public String execution_retry_delay_ms;
      public String execution_retry_count;

      public TransactionOptions() {
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            DseFsOptions.TransactionOptions that = (DseFsOptions.TransactionOptions)o;
            return Objects.equals(this.transaction_timeout_ms, that.transaction_timeout_ms) && Objects.equals(this.conflict_retry_delay_ms, that.conflict_retry_delay_ms) && Objects.equals(this.execution_retry_delay_ms, that.execution_retry_delay_ms) && Objects.equals(this.execution_retry_count, that.execution_retry_count);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.transaction_timeout_ms, this.conflict_retry_delay_ms, this.execution_retry_delay_ms, this.execution_retry_count});
      }
   }
}
