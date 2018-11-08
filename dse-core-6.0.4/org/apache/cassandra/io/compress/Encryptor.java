package org.apache.cassandra.io.compress;

import com.datastax.bdp.cassandra.crypto.KeyAccessException;
import com.datastax.bdp.cassandra.crypto.KeyGenerationException;
import com.datastax.bdp.server.DseDaemon;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;
import javax.crypto.NoSuchPaddingException;
import org.apache.cassandra.service.StorageService;

public class Encryptor implements ICompressor {
   private static final SecureRandom random = new SecureRandom();
   private final EncryptionConfig encryptionConfig;
   private final ThreadLocal<StatefulEncryptor> encryptor = new ThreadLocal();
   private final ThreadLocal<StatefulDecryptor> decryptor = new ThreadLocal();

   public static Encryptor create(Map<String, String> options) {
      return new Encryptor(options);
   }

   public Encryptor(Map<String, String> options) {
      this.encryptionConfig = EncryptionConfig.forClass(this.getClass()).fromCompressionOptions(options).build();

      try {
         if(DseDaemon.isSetup() && !StorageService.instance.isBootstrapMode()) {
            this.getEncryptor();
            this.getDecryptor();
         }

      } catch (NoSuchAlgorithmException var3) {
         throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName() + ". Cipher algorithm not supported: " + this.encryptionConfig.getCipherName());
      } catch (NoSuchPaddingException var4) {
         throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName() + ". Cipher padding not supported: " + this.encryptionConfig.getCipherName());
      } catch (Exception var5) {
         throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName(), var5);
      }
   }

   private StatefulEncryptor getEncryptor() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException, KeyAccessException, KeyGenerationException {
      if(this.encryptor.get() == null) {
         this.encryptor.set(new StatefulEncryptor(this.encryptionConfig, random));
      }

      return (StatefulEncryptor)this.encryptor.get();
   }

   private StatefulDecryptor getDecryptor() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
      if(this.decryptor.get() == null) {
         this.decryptor.set(new StatefulDecryptor(this.encryptionConfig, random));
      }

      return (StatefulDecryptor)this.decryptor.get();
   }

   public int initialCompressedBufferLength(int chunkLength) {
      try {
         return this.getEncryptor().outputLength(chunkLength);
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
      try {
         this.getEncryptor().encrypt(input, output);
      } catch (Exception var4) {
         throw new IOException("Failed to encrypt data", var4);
      }
   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
      try {
         return this.getDecryptor().decrypt(input, inputOffset, inputLength, output, outputOffset);
      } catch (Exception var7) {
         throw new IOException("Failed to decrypt data", var7);
      }
   }

   public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
      try {
         this.getDecryptor().decrypt(input, output);
      } catch (Exception var4) {
         throw new IOException("Failed to decrypt data", var4);
      }
   }

   public BufferType preferredBufferType() {
      return BufferType.OFF_HEAP;
   }

   public boolean supports(BufferType bufferType) {
      return true;
   }

   public Set<String> supportedOptions() {
      return Sets.union(Sets.newHashSet(new String[]{"cipher_algorithm", "secret_key_strength", "iv_length", "key_provider", "secret_key_provider_factory_class"}), this.encryptionConfig.getKeyProviderFactory().supportedOptions());
   }
}
