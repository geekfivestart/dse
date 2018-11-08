package org.apache.cassandra.config;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

public class TransparentDataEncryptionOptions {
   public boolean enabled;
   public int chunk_length_kb;
   public String cipher;
   public String key_alias;
   public int iv_length;
   public ParameterizedClass key_provider;

   public TransparentDataEncryptionOptions() {
      this.enabled = false;
      this.chunk_length_kb = 64;
      this.cipher = "AES/CBC/PKCS5Padding";
      this.iv_length = 16;
   }

   public TransparentDataEncryptionOptions(boolean enabled) {
      this.enabled = false;
      this.chunk_length_kb = 64;
      this.cipher = "AES/CBC/PKCS5Padding";
      this.iv_length = 16;
      this.enabled = enabled;
   }

   public TransparentDataEncryptionOptions(String cipher, String keyAlias, ParameterizedClass keyProvider) {
      this(true, cipher, keyAlias, keyProvider);
   }

   public TransparentDataEncryptionOptions(boolean enabled, String cipher, String keyAlias, ParameterizedClass keyProvider) {
      this.enabled = false;
      this.chunk_length_kb = 64;
      this.cipher = "AES/CBC/PKCS5Padding";
      this.iv_length = 16;
      this.enabled = enabled;
      this.cipher = cipher;
      this.key_alias = keyAlias;
      this.key_provider = keyProvider;
   }

   public String get(String key) {
      return (String)this.key_provider.parameters.get(key);
   }

   @VisibleForTesting
   public void remove(String key) {
      this.key_provider.parameters.remove(key);
   }

   public boolean equals(Object o) {
      return o instanceof TransparentDataEncryptionOptions && this.equals((TransparentDataEncryptionOptions)o);
   }

   public boolean equals(TransparentDataEncryptionOptions other) {
      return Objects.equals(this.cipher, other.cipher) && Objects.equals(this.key_alias, other.key_alias);
   }
}
