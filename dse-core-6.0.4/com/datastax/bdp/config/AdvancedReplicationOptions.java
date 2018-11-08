package com.datastax.bdp.config;

public class AdvancedReplicationOptions {
   public boolean enabled = false;
   public boolean conf_driver_password_encryption_enabled = false;
   public String security_base_path = "";
   public String advanced_replication_directory;

   public AdvancedReplicationOptions() {
   }
}
