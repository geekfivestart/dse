package com.datastax.bdp.config;

public class SparkUIOptions {
   public String encryption;
   public org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions encryption_options = new org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions();

   public SparkUIOptions() {
   }
}
