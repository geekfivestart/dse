package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.util.FileSystemUtil;
import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentMap;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

public class LocalFileSystemKeyProvider implements IKeyProvider {
   private static final SecureRandom random = new SecureRandom();
   private final File keyFile;
   private ConcurrentMap<String, SecretKey> keys = Maps.newConcurrentMap();

   public LocalFileSystemKeyProvider(File keyFile) throws IOException {
      this.keyFile = keyFile;
      if(!keyFile.exists()) {
         if(!keyFile.getParentFile().exists() && !keyFile.getParentFile().mkdirs()) {
            throw new IOException("Failed to create directory: " + keyFile.getParentFile());
         }

         if(!keyFile.createNewFile()) {
            throw new IOException("Failed to create file: " + keyFile);
         }

         if(!keyFile.setWritable(true, true)) {
            throw new IOException("File not writeable: " + keyFile);
         }

         if(!keyFile.setReadable(true, true)) {
            throw new IOException("File not readable: " + keyFile);
         }

         if(FileSystemUtil.enabled && FileSystemUtil.chmod(keyFile.getPath(), 384) != 0) {
            throw new IOException("Could not set file permissions to 0600 for: " + keyFile);
         }
      }

      this.loadKeys();
   }

   public SecretKey getSecretKey(String cipherName, int keyStrength) throws KeyAccessException, KeyGenerationException {
      try {
         String mapKey = this.getMapKey(cipherName, keyStrength);
         SecretKey secretKey = (SecretKey)this.keys.get(mapKey);
         if(secretKey == null) {
            secretKey = this.generateNewKey(cipherName, keyStrength);
            this.checkKey(cipherName, secretKey);
            SecretKey previous = (SecretKey)this.keys.putIfAbsent(mapKey, secretKey);
            if(previous == null) {
               this.appendKey(cipherName, keyStrength, secretKey);
            } else {
               secretKey = previous;
            }
         }

         return secretKey;
      } catch (IOException var6) {
         throw new KeyGenerationException("Could not write secret key: " + var6.getMessage(), var6);
      } catch (NoSuchAlgorithmException var7) {
         throw new KeyGenerationException("Failed to generate secret key: " + var7.getMessage(), var7);
      }
   }

   private void checkKey(String cipherName, SecretKey secretKey) throws KeyGenerationException {
      try {
         Cipher cipher = Cipher.getInstance(cipherName);
         cipher.init(1, secretKey, random);
      } catch (InvalidKeyException | NoSuchPaddingException | NoSuchAlgorithmException var4) {
         throw new KeyGenerationException("Error generating secret key: " + var4.getMessage(), var4);
      }
   }

   private SecretKey generateNewKey(String cipherName, int keyStrength) throws NoSuchAlgorithmException {
      KeyGenerator kgen = KeyGenerator.getInstance(this.getKeyType(cipherName));
      kgen.init(keyStrength, random);
      return kgen.generateKey();
   }

   private String getMapKey(String cipherName, int keyStrength) {
      return cipherName + ":" + keyStrength;
   }

   private synchronized void appendKey(String cipherName, int keyStrength, SecretKey key) throws IOException {
      PrintStream ps = null;

      try {
         ps = new PrintStream(new FileOutputStream(this.keyFile, true));
         ps.println(cipherName + ":" + keyStrength + ":" + Base64.encodeBase64String(key.getEncoded()));
      } finally {
         IOUtils.closeQuietly(ps);
      }

   }

   private synchronized void loadKeys() throws IOException {
      this.keys.clear();
      BufferedReader is = null;

      try {
         is = new BufferedReader(new InputStreamReader(new FileInputStream(this.keyFile)));

         String line;
         while((line = is.readLine()) != null) {
            String[] fields = line.split(":");
            String cipherName = fields[0];
            int keyStrength = Integer.parseInt(fields[1]);
            byte[] key = Base64.decodeBase64(fields[2].getBytes());
            this.keys.put(this.getMapKey(cipherName, keyStrength), new SecretKeySpec(key, this.getKeyType(cipherName)));
         }
      } finally {
         IOUtils.closeQuietly(is);
      }

   }

   private String getKeyType(String cipherName) {
      return cipherName.replaceAll("/.*", "");
   }
}
