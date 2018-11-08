package com.datastax.bdp.cassandra.crypto;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;

public abstract class SystemKey {
   private static final Random random = new SecureRandom();
   private static final ConcurrentHashMap<String, SystemKey> keys = new ConcurrentHashMap();
   private static final byte[] NONE = new byte[0];

   public SystemKey() {
   }

   protected abstract SecretKey getKey() throws KeyAccessException;

   protected abstract String getCipherName();

   protected abstract int getKeyStrength();

   protected abstract int getIvLength();

   public abstract String getName();

   private static byte[] createIv(int size) {
      assert size >= 0;

      if(size == 0) {
         return NONE;
      } else {
         byte[] b = new byte[size];
         random.nextBytes(b);
         return b;
      }
   }

   protected static int getIvLength(String cipherName) throws IOException {
      if(cipherName.matches(".*/(CBC|CFB|OFB|PCBC)/.*")) {
         try {
            return Cipher.getInstance(cipherName).getBlockSize();
         } catch (NoSuchPaddingException | NoSuchAlgorithmException var2) {
            throw new IOException(var2);
         }
      } else {
         return 0;
      }
   }

   public byte[] encrypt(byte[] input) throws IOException {
      try {
         byte[] iv = createIv(this.getIvLength());
         Cipher cipher = Cipher.getInstance(this.getCipherName());
         if(iv.length > 0) {
            cipher.init(1, this.getKey(), new IvParameterSpec(iv));
         } else {
            cipher.init(1, this.getKey());
         }

         byte[] output = cipher.doFinal(input);
         return ArrayUtils.addAll(iv, output);
      } catch (InvalidKeyException | NoSuchAlgorithmException | KeyAccessException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException | NoSuchPaddingException var5) {
         throw new IOException("Couldn't encrypt input", var5);
      }
   }

   public String encrypt(String input) throws IOException {
      return Base64.encodeBase64String(this.encrypt(input.getBytes()));
   }

   public byte[] decrypt(byte[] input) throws IOException {
      if(input == null) {
         throw new IOException("input is null");
      } else {
         try {
            byte[] iv = this.getIvLength() > 0?Arrays.copyOfRange(input, 0, this.getIvLength()):NONE;
            Cipher cipher = Cipher.getInstance(this.getCipherName());
            if(iv.length > 0) {
               cipher.init(2, this.getKey(), new IvParameterSpec(iv));
            } else {
               cipher.init(2, this.getKey());
            }

            return cipher.doFinal(input, this.getIvLength(), input.length - this.getIvLength());
         } catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException | NoSuchAlgorithmException | KeyAccessException | InvalidAlgorithmParameterException | NoSuchPaddingException var4) {
            throw new IOException("Couldn't decrypt input", var4);
         }
      }
   }

   public String decrypt(String input) throws IOException {
      if(input == null) {
         throw new IOException("input is null");
      } else {
         return new String(this.decrypt(Base64.decodeBase64(input.getBytes())));
      }
   }

   public static SystemKey getSystemKey(String path) throws IOException {
      SystemKey systemKey = (SystemKey)keys.get(path);
      if(systemKey == null) {
         if(KmipSystemKey.isKmipPath(path)) {
            systemKey = KmipSystemKey.getKey(path);
         } else {
            systemKey = LocalSystemKey.getKey(path);
         }

         SystemKey previous = (SystemKey)keys.putIfAbsent(path, systemKey);
         if(previous != null) {
            systemKey = previous;
         }
      }

      return (SystemKey)systemKey;
   }

   protected static String getKeyType(String cipherName) {
      return cipherName.replaceAll("/.*", "");
   }
}
