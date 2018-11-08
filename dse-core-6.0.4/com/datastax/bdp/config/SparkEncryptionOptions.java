package com.datastax.bdp.config;

public class SparkEncryptionOptions {
   public String enabled = null;
   public String keystore = null;
   public String key_password = null;
   public String keystore_password = null;
   public String truststore = null;
   public String truststore_password = null;
   public String protocol = null;
   public String[] cipher_suites = null;

   public SparkEncryptionOptions() {
   }

   public boolean containsValues() {
      return this.enabled != null || this.keystore != null || this.keystore_password != null || this.truststore != null || this.truststore_password != null || this.protocol != null || this.key_password != null || this.cipher_suites != null;
   }
}
