package com.datastax.bdp.config;

public class ClientEncryptionOptions {
   public String enabled = null;
   public String keystore = null;
   public String keystore_password = null;
   public String truststore = null;
   public String truststore_password = null;
   public String protocol = null;
   public String keystore_type = null;
   public String truststore_type = null;
   public String[] cipher_suites = null;

   public ClientEncryptionOptions() {
   }

   public boolean containsValues() {
      return this.enabled != null || this.keystore != null || this.keystore_password != null || this.truststore != null || this.truststore_password != null || this.protocol != null || this.keystore_type != null || this.truststore_type != null || this.cipher_suites != null;
   }
}
