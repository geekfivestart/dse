package org.apache.cassandra.config;

public class SystemTableEncryptionOptions {
   public boolean enabled = false;
   public String cipher_algorithm = "AES";
   public int secret_key_strength = 128;
   public int chunk_length_kb = 64;
   public String key_name = "system_table_keytab";
   public String key_provider = "com.datastax.bdp.cassandra.crypto.LocalFileSystemKeyProviderFactory";
   public String kmip_host = null;

   public SystemTableEncryptionOptions() {
   }

   public boolean isKmipKeyProvider() {
      return this.key_provider.endsWith("KmipKeyProviderFactory");
   }
}
