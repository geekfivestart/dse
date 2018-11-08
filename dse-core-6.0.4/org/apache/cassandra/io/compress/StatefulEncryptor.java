package org.apache.cassandra.io.compress;

import com.datastax.bdp.cassandra.crypto.IKeyProvider;
import com.datastax.bdp.cassandra.crypto.IMultiKeyProvider;
import com.datastax.bdp.cassandra.crypto.KeyAccessException;
import com.datastax.bdp.cassandra.crypto.KeyGenerationException;
import com.datastax.bdp.util.Isaac;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

public class StatefulEncryptor {
   private final SecureRandom random;
   private final EncryptionConfig config;
   private final byte[] iv;
   private final Cipher cipher;
   private final Isaac fastRandom;
   private boolean initialized = false;

   public StatefulEncryptor(EncryptionConfig config, SecureRandom random) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, KeyAccessException, KeyGenerationException {
      this.random = random;
      this.config = config;
      this.iv = new byte[config.getIvLength()];
      this.cipher = Cipher.getInstance(config.getCipherName());
      int[] seed = new int[256];

      for(int i = 0; i < seed.length; ++i) {
         seed[i] = random.nextInt();
      }

      this.fastRandom = new Isaac(seed);
      this.maybeInit(config.getKeyProvider().getSecretKey(config.getCipherName(), config.getKeyStrength()));
   }

   private void maybeInit(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException {
      if(!this.initialized) {
         this.init(key);
      }

   }

   private void init(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException {
      if(this.config.isIvEnabled()) {
         this.cipher.init(1, key, this.createIV(), this.random);
      } else {
         this.cipher.init(1, key, this.random);
      }

      this.initialized = true;
   }

   private AlgorithmParameterSpec createIV() {
      for(int i = 0; i < this.config.getIvLength(); i += 4) {
         int value = this.fastRandom.nextInt();
         this.iv[i] = (byte)(value >>> 24);
         this.iv[i + 1] = (byte)(value >>> 16);
         this.iv[i + 2] = (byte)(value >>> 8);
         this.iv[i + 3] = (byte)value;
      }

      return new IvParameterSpec(this.iv);
   }

   public void encrypt(ByteBuffer input, ByteBuffer output) throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException {
      IKeyProvider keyProvider = this.config.getKeyProvider();
      SecretKey key;
      if(keyProvider instanceof IMultiKeyProvider) {
         key = ((IMultiKeyProvider)keyProvider).writeHeader(this.config.getCipherName(), this.config.getKeyStrength(), output);
      } else {
         key = keyProvider.getSecretKey(this.config.getCipherName(), this.config.getKeyStrength());
      }

      this.maybeInit(key);
      if(this.config.isIvEnabled()) {
         output.put(this.iv);
      }

      this.cipher.doFinal(input, output);
      this.initialized = false;
   }

   public int outputLength(int inputSize) throws InvalidAlgorithmParameterException, InvalidKeyException, KeyAccessException, KeyGenerationException {
      IKeyProvider keyProvider = this.config.getKeyProvider();
      this.maybeInit(keyProvider.getSecretKey(this.config.getCipherName(), this.config.getKeyStrength()));
      int headerSize = keyProvider instanceof IMultiKeyProvider?((IMultiKeyProvider)keyProvider).headerLength():0;
      return this.config.getIvLength() + this.cipher.getOutputSize(inputSize) + headerSize;
   }
}
