package org.apache.cassandra.security;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JKSKeyProvider implements KeyProvider {
   private static final Logger logger = LoggerFactory.getLogger(JKSKeyProvider.class);
   static final String PROP_KEYSTORE = "keystore";
   static final String PROP_KEYSTORE_PW = "keystore_password";
   static final String PROP_KEYSTORE_TYPE = "store_type";
   static final String PROP_KEY_PW = "key_password";
   private final KeyStore store;
   private final boolean isJceks;
   private final TransparentDataEncryptionOptions options;

   public JKSKeyProvider(TransparentDataEncryptionOptions options) {
      this.options = options;
      logger.info("initializing keystore from file {}", options.get("keystore"));

      try {
         InputStream inputStream = Files.newInputStream(Paths.get(options.get("keystore"), new String[0]), new OpenOption[0]);
         Throwable var3 = null;

         try {
            this.store = KeyStore.getInstance(options.get("store_type"));
            this.store.load(inputStream, options.get("keystore_password").toCharArray());
            this.isJceks = this.store.getType().equalsIgnoreCase("jceks");
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if(inputStream != null) {
               if(var3 != null) {
                  try {
                     inputStream.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  inputStream.close();
               }
            }

         }

      } catch (Exception var15) {
         throw new RuntimeException("couldn't load keystore", var15);
      }
   }

   public Key getSecretKey(String keyAlias) throws IOException {
      if(this.isJceks) {
         keyAlias = keyAlias.toLowerCase();
      }

      Key key;
      try {
         String password = this.options.get("key_password");
         if(password == null || password.isEmpty()) {
            password = this.options.get("keystore_password");
         }

         key = this.store.getKey(keyAlias, password.toCharArray());
      } catch (Exception var4) {
         throw new IOException("unable to load key from keystore");
      }

      if(key == null) {
         throw new IOException(String.format("key %s was not found in keystore", new Object[]{keyAlias}));
      } else {
         return key;
      }
   }
}
