package com.datastax.bdp.config;

import com.datastax.bdp.cassandra.crypto.LocalFileSystemKeyProviderFactory;

public class SystemTableEncryptionOptions {
   public boolean enabled = false;
   public String cipher_algorithm = "AES";
   public Integer secret_key_strength = Integer.valueOf(128);
   public Integer chunk_length_kb = Integer.valueOf(64);
   public String key_name = "system_table_keytab";
   public String key_provider = LocalFileSystemKeyProviderFactory.class.getName();
   public String kmip_host = null;

   public SystemTableEncryptionOptions() {
   }

   public SystemTableEncryptionOptions.Wrapper getWrapper() {
      return new SystemTableEncryptionOptions.Wrapper();
   }

   public String toString() {
      return "SystemTableEncryptionOptions{enabled=" + this.enabled + ", cipher_algorithm='" + this.cipher_algorithm + '\'' + ", secret_key_strength=" + this.secret_key_strength + ", chunk_length_kb=" + this.chunk_length_kb + ", key_name='" + this.key_name + '\'' + ", key_provider='" + this.key_provider + '\'' + ", kmip_host='" + this.kmip_host + '\'' + '}';
   }

   public class Wrapper {
      public Wrapper() {
      }

      public boolean isEnabled() {
         return SystemTableEncryptionOptions.this.enabled;
      }

      public String getCipherAlgorithm() {
         return SystemTableEncryptionOptions.this.cipher_algorithm;
      }

      public Integer getSecretKeyStrength() {
         return SystemTableEncryptionOptions.this.secret_key_strength;
      }

      public Integer getChunkLengthInKB() {
         return SystemTableEncryptionOptions.this.chunk_length_kb;
      }

      public String getKeyName() {
         return SystemTableEncryptionOptions.this.key_name;
      }

      public String getKmipHost() {
         return SystemTableEncryptionOptions.this.kmip_host;
      }

      public String getKeyProvider() {
         return SystemTableEncryptionOptions.this.key_provider;
      }

      public String toString() {
         return "SystemTableEncryptionOptions.Wrapper{enabled=" + SystemTableEncryptionOptions.this.enabled + ", cipher_algorithm='" + SystemTableEncryptionOptions.this.cipher_algorithm + '\'' + ", secret_key_strength=" + SystemTableEncryptionOptions.this.secret_key_strength + ", chunk_length_kb=" + SystemTableEncryptionOptions.this.chunk_length_kb + ", key_name='" + SystemTableEncryptionOptions.this.key_name + '\'' + ", key_provider='" + SystemTableEncryptionOptions.this.key_provider + '\'' + ", kmip_host='" + SystemTableEncryptionOptions.this.kmip_host + '\'' + '}';
      }
   }
}
