package org.apache.cassandra.io.compress;

import com.datastax.bdp.cassandra.crypto.IKeyProvider;
import com.datastax.bdp.cassandra.crypto.IKeyProviderFactory;
import com.datastax.bdp.cassandra.crypto.ReplicatedKeyProviderFactory;
import com.datastax.bdp.util.OptionMap;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EncryptionConfig {
   public static final String CIPHER_ALGORITHM = "cipher_algorithm";
   public static final String SECRET_KEY_STRENGTH = "secret_key_strength";
   public static final String SECRET_KEY_PROVIDER_FACTORY_CLASS = "secret_key_provider_factory_class";
   public static final String KEY_PROVIDER = "key_provider";
   public static final String IV_LENGTH = "iv_length";
   private static final String DEFAULT_CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
   private static final int DEFAULT_SECRET_KEY_STRENGTH = 128;
   private final IKeyProviderFactory keyProviderFactory;
   private final IKeyProvider keyProvider;
   private final String cipherName;
   private final int keyStrength;
   private final boolean ivEnabled;
   private final int ivLength;
   private final Map<String, String> encryptionOptions;

   public static EncryptionConfig.Builder forClass(Class<?> callerClass) {
      return new EncryptionConfig.Builder(callerClass);
   }

   private EncryptionConfig(IKeyProviderFactory keyProviderFactory, IKeyProvider keyProvider, String cipherName, int keyStrength, boolean ivEnabled, int ivLength, Map<String, String> encryptionOptions) {
      this.keyProviderFactory = keyProviderFactory;
      this.keyProvider = keyProvider;
      this.cipherName = cipherName;
      this.keyStrength = keyStrength;
      this.ivEnabled = ivEnabled;
      this.ivLength = ivLength;
      this.encryptionOptions = ImmutableMap.copyOf(encryptionOptions);
   }

   public IKeyProviderFactory getKeyProviderFactory() {
      return this.keyProviderFactory;
   }

   public IKeyProvider getKeyProvider() {
      return this.keyProvider;
   }

   public int getKeyStrength() {
      return this.keyStrength;
   }

   public String getCipherName() {
      return this.cipherName;
   }

   public boolean isIvEnabled() {
      return this.ivEnabled;
   }

   public int getIvLength() {
      return this.ivLength;
   }

   public Map<String, String> asMap() {
      return this.encryptionOptions;
   }

   public static class Builder {
      private final Class<?> callerClass;
      private final Map<String, String> compressionOptions;

      private Builder(Class<?> callerClass) {
         this.compressionOptions = new HashMap();
         this.callerClass = callerClass;
      }

      public EncryptionConfig.Builder fromCompressionOptions(Map<String, String> compressionOptions) {
         this.compressionOptions.putAll(compressionOptions);
         return this;
      }

      public EncryptionConfig build() {
         OptionMap optionMap = new OptionMap(this.compressionOptions);
         String cipherName = optionMap.get("cipher_algorithm", "AES/CBC/PKCS5Padding");
         int keyStrength = optionMap.get("secret_key_strength", 128);
         int userIvLength = optionMap.get("iv_length", -1);
         boolean ivEnabled = cipherName.matches(".*/(CBC|CFB|OFB|PCBC)/.*");
         int ivLength = !ivEnabled?0:(userIvLength > 0?userIvLength:this.getIvLength(cipherName.replaceAll("/.*", "")));

         try {
            Class<?> keyProviderFactoryClass = this.getKeyFactory(this.compressionOptions);
            IKeyProviderFactory keyProviderFactory = (IKeyProviderFactory)keyProviderFactoryClass.newInstance();
            IKeyProvider keyProvider = keyProviderFactory.getKeyProvider(this.compressionOptions);
            return new EncryptionConfig(keyProviderFactory, keyProvider, cipherName, keyStrength, ivEnabled, ivLength, this.compressionOptions);
         } catch (IllegalAccessException | ClassNotFoundException | IOException | InstantiationException var10) {
            throw new RuntimeException("Failed to initialize " + this.callerClass.getSimpleName(), var10);
         }
      }

      private Class<?> getKeyFactory(Map<String, String> options) throws ClassNotFoundException {
         String className;
         if(options.containsKey("key_provider")) {
            className = (String)options.get("key_provider");
         } else if(options.containsKey("secret_key_provider_factory_class")) {
            className = (String)options.get("secret_key_provider_factory_class");
         } else {
            className = ReplicatedKeyProviderFactory.class.getName();
         }

         if(!className.contains(".")) {
            className = "com.datastax.bdp.cassandra.crypto." + className;
         }

         return Class.forName(className);
      }

      private int getIvLength(String algorithm) {
         return algorithm.equals("AES")?16:8;
      }
   }
}
