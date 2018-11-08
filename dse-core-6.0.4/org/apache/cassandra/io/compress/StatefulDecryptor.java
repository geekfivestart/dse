package org.apache.cassandra.io.compress;

import com.datastax.bdp.cassandra.crypto.IKeyProvider;
import com.datastax.bdp.cassandra.crypto.IMultiKeyProvider;
import com.datastax.bdp.cassandra.crypto.KeyAccessException;
import com.datastax.bdp.cassandra.crypto.KeyGenerationException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

public class StatefulDecryptor {
   private final EncryptionConfig encryptionConfig;
   private final SecureRandom secureRandom;
   private final Cipher cipher;

   public StatefulDecryptor(EncryptionConfig encryptionConfig, SecureRandom secureRandom) throws NoSuchPaddingException, NoSuchAlgorithmException {
      this.encryptionConfig = encryptionConfig;
      this.secureRandom = secureRandom;
      this.cipher = Cipher.getInstance(encryptionConfig.getCipherName());
   }

   public void decrypt(ByteBuffer input, ByteBuffer output) throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException {
      IKeyProvider keyProvider = this.encryptionConfig.getKeyProvider();
      SecretKey key;
      if(keyProvider instanceof IMultiKeyProvider) {
         key = ((IMultiKeyProvider)keyProvider).readHeader(this.encryptionConfig.getCipherName(), this.encryptionConfig.getKeyStrength(), input);
      } else {
         key = keyProvider.getSecretKey(this.encryptionConfig.getCipherName(), this.encryptionConfig.getKeyStrength());
      }

      this.init(key, input);
      int requiredOutputSize = this.cipher.getOutputSize(input.limit() - input.position());
      int outputSize = output.limit() - output.position();
      if(outputSize >= requiredOutputSize) {
         this.cipher.doFinal(input, output);
      } else {
         ByteBuffer tempBuffer = ByteBuffer.allocate(requiredOutputSize);
         this.cipher.doFinal(input, tempBuffer);
         tempBuffer.flip();
         int actualOutputSize = tempBuffer.remaining();
         if(actualOutputSize > outputSize) {
            throw new ShortBufferException("Need at least " + actualOutputSize + " bytes of space in output buffer");
         }

         output.put(tempBuffer);
      }

   }

   public int decrypt(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException {
      IKeyProvider keyProvider = this.encryptionConfig.getKeyProvider();
      int headerSize = false;
      SecretKey key;
      if(keyProvider instanceof IMultiKeyProvider) {
         ByteBuffer inputBuffer = ByteBuffer.wrap(input, inputOffset, inputLength - inputOffset);
         key = ((IMultiKeyProvider)keyProvider).readHeader(this.encryptionConfig.getCipherName(), this.encryptionConfig.getKeyStrength(), inputBuffer);
         int headerSize = inputBuffer.position() - inputOffset;
         inputLength -= headerSize;
         inputOffset = inputBuffer.position();
      } else {
         key = keyProvider.getSecretKey(this.encryptionConfig.getCipherName(), this.encryptionConfig.getKeyStrength());
      }

      this.init(key, input, inputOffset);
      return this.cipher.doFinal(input, inputOffset + this.encryptionConfig.getIvLength(), inputLength - this.encryptionConfig.getIvLength(), output, outputOffset);
   }

   private void init(SecretKey key, ByteBuffer input) throws InvalidKeyException, InvalidAlgorithmParameterException {
      if(this.encryptionConfig.isIvEnabled()) {
         byte[] iv = new byte[this.encryptionConfig.getIvLength()];
         input.get(iv);
         IvParameterSpec ivParam = new IvParameterSpec(iv);
         this.cipher.init(2, key, ivParam, this.secureRandom);
      } else {
         this.cipher.init(2, key, this.secureRandom);
      }

   }

   private void init(SecretKey key, byte[] input, int inputOffset) throws InvalidKeyException, InvalidAlgorithmParameterException {
      if(this.encryptionConfig.isIvEnabled()) {
         IvParameterSpec ivParam = new IvParameterSpec(input, inputOffset, this.encryptionConfig.getIvLength());
         this.cipher.init(2, key, ivParam, this.secureRandom);
      } else {
         this.cipher.init(2, key, this.secureRandom);
      }

   }
}
