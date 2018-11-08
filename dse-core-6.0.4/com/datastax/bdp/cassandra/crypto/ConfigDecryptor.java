package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.config.DseConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

public class ConfigDecryptor {
   private static ConfigDecryptor instance;
   private SystemKey systemKey;

   public static ConfigDecryptor getInstance() {
      if(instance == null) {
         instance = new ConfigDecryptor();
      }

      return instance;
   }

   private ConfigDecryptor() {
      if(DseConfig.getConfigEncryptionActive()) {
         String keyFile = DseConfig.getConfigEncryptionKeyName();
         if(keyFile == null) {
            throw new IllegalArgumentException("config_encryption_key_name must be set if config encryption is enabled");
         }

         try {
            this.systemKey = SystemKey.getSystemKey(keyFile);
         } catch (IOException var3) {
            throw new IllegalArgumentException(var3);
         }
      } else {
         this.systemKey = null;
      }

   }

   public String decryptIfActive(String input) throws IOException {
      return input == null?null:(this.isActive()?this.systemKey.decrypt(input):input);
   }

   public boolean isActive() {
      return this.systemKey != null;
   }

   @VisibleForTesting
   protected void setSystemKey(SystemKey systemKey) {
      this.systemKey = systemKey;
   }
}
