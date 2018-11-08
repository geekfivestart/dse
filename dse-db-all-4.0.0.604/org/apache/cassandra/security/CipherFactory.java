package org.apache.cassandra.security;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CipherFactory {
   private final Logger logger = LoggerFactory.getLogger(CipherFactory.class);
   private static final FastThreadLocal<CipherFactory.CachedCipher> cipherThreadLocal = new FastThreadLocal();
   private final SecureRandom secureRandom;
   private final LoadingCache<String, Key> cache;
   private final int ivLength;
   private final KeyProvider keyProvider;

   public CipherFactory(TransparentDataEncryptionOptions options) {
      this.logger.info("initializing CipherFactory");
      this.ivLength = options.iv_length;

      try {
         this.secureRandom = SecureRandom.getInstance("SHA1PRNG");
         Class<KeyProvider> keyProviderClass = Class.forName(options.key_provider.class_name);
         Constructor ctor = keyProviderClass.getConstructor(new Class[]{TransparentDataEncryptionOptions.class});
         this.keyProvider = (KeyProvider)ctor.newInstance(new Object[]{options});
      } catch (Exception var4) {
         throw new RuntimeException("couldn't load cipher factory", var4);
      }

      this.cache = Caffeine.newBuilder().maximumSize(64L).executor(MoreExecutors.directExecutor()).removalListener((key, value, cause) -> {
         this.logger.info("key {} removed from cipher key cache", key);
      }).build((alias) -> {
         this.logger.info("loading secret key for alias {}", alias);
         return this.keyProvider.getSecretKey(alias);
      });
   }

   public Cipher getEncryptor(String transformation, String keyAlias) throws IOException {
      byte[] iv = new byte[this.ivLength];
      this.secureRandom.nextBytes(iv);
      return this.buildCipher(transformation, keyAlias, iv, 1);
   }

   public Cipher getDecryptor(String transformation, String keyAlias, byte[] iv) throws IOException {
      assert iv != null && iv.length > 0 : "trying to decrypt, but the initialization vector is empty";

      return this.buildCipher(transformation, keyAlias, iv, 2);
   }

   @VisibleForTesting
   Cipher buildCipher(String transformation, String keyAlias, byte[] iv, int cipherMode) throws IOException {
      try {
         CipherFactory.CachedCipher cachedCipher = (CipherFactory.CachedCipher)cipherThreadLocal.get();
         if(cachedCipher != null) {
            Cipher cipher = cachedCipher.cipher;
            if(cachedCipher.mode == cipherMode && cipher.getAlgorithm().equals(transformation) && cachedCipher.keyAlias.equals(keyAlias) && Arrays.equals(cipher.getIV(), iv)) {
               return cipher;
            }
         }

         Key key = this.retrieveKey(keyAlias);
         Cipher cipher = Cipher.getInstance(transformation);
         cipher.init(cipherMode, key, new IvParameterSpec(iv));
         cipherThreadLocal.set(new CipherFactory.CachedCipher(cipherMode, keyAlias, cipher));
         return cipher;
      } catch (NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchAlgorithmException var8) {
         this.logger.error("could not build cipher", var8);
         throw new IOException("cannot load cipher", var8);
      }
   }

   private Key retrieveKey(String keyAlias) throws IOException {
      try {
         return (Key)this.cache.get(keyAlias);
      } catch (CompletionException var3) {
         if(var3.getCause() instanceof IOException) {
            throw (IOException)var3.getCause();
         } else {
            throw new IOException("failed to load key from cache: " + keyAlias, var3);
         }
      }
   }

   private static class CachedCipher {
      public final int mode;
      public final String keyAlias;
      public final Cipher cipher;

      private CachedCipher(int mode, String keyAlias, Cipher cipher) {
         this.mode = mode;
         this.keyAlias = keyAlias;
         this.cipher = cipher;
      }
   }
}
