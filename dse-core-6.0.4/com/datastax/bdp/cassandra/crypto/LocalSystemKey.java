package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.util.FileSystemUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

public class LocalSystemKey extends SystemKey {
   private static final SecureRandom random = new SecureRandom();
   private final File keyFile;
   private final String cipherName;
   private final int keyStrength;
   private final int ivLength;
   private final SecretKey key;

   public LocalSystemKey(File keyFile) throws IOException {
      assert keyFile != null;

      this.keyFile = keyFile;
      BufferedReader is = null;

      try {
         is = new BufferedReader(new InputStreamReader(new FileInputStream(keyFile)));
         String line = is.readLine();
         if(line == null) {
            throw new IOException("Key file: " + keyFile + " is empty");
         }

         String[] fields = line.split(":");
         if(fields.length != 3) {
            throw new IOException("Malformed key file");
         }

         this.cipherName = fields[0];
         this.keyStrength = Integer.parseInt(fields[1]);
         byte[] keyBytes = Base64.decodeBase64(fields[2].getBytes());
         this.key = new SecretKeySpec(keyBytes, getKeyType(this.cipherName));
         this.ivLength = getIvLength(this.cipherName);
      } finally {
         IOUtils.closeQuietly(is);
      }

   }

   protected SecretKey getKey() {
      return this.key;
   }

   protected String getCipherName() {
      return this.cipherName;
   }

   protected int getKeyStrength() {
      return this.keyStrength;
   }

   protected int getIvLength() {
      return this.ivLength;
   }

   public static File createKey(String path, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException {
      return createKey((File)null, path, cipherName, keyStrength);
   }

   public static File createKey(File directory, String path, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException {
      if(directory == null) {
         directory = DseConfig.getSystemKeyDirectory();
      }

      File keyFile = new File(directory, path);
      KeyGenerator keyGen = KeyGenerator.getInstance(getKeyType(cipherName));
      keyGen.init(keyStrength, random);
      SecretKey key = keyGen.generateKey();
      Cipher.getInstance(cipherName);
      if(keyFile.exists()) {
         throw new IOException("File already exists: " + keyFile);
      } else if(!keyFile.getParentFile().exists() && !keyFile.getParentFile().mkdirs()) {
         throw new IOException("Failed to create directory: " + keyFile.getParentFile());
      } else if(!keyFile.createNewFile()) {
         throw new IOException("Failed to create file: " + keyFile);
      } else if(!keyFile.setWritable(true, true)) {
         throw new IOException("File not writeable: " + keyFile);
      } else if(!keyFile.setReadable(true, true)) {
         throw new IOException("File not readable: " + keyFile);
      } else if(FileSystemUtil.enabled && FileSystemUtil.chmod(keyFile.getPath(), 384) != 0) {
         throw new IOException("Could not set file permissions to 0600 for: " + keyFile);
      } else {
         PrintStream ps = null;

         try {
            ps = new PrintStream(new FileOutputStream(keyFile));
            ps.println(cipherName + ":" + keyStrength + ":" + Base64.encodeBase64String(key.getEncoded()));
         } finally {
            IOUtils.closeQuietly(ps);
         }

         return keyFile;
      }
   }

   public static LocalSystemKey getKey(String path) throws IOException {
      File systemKeyFile = new File(DseConfig.getSystemKeyDirectory(), path);
      if(!systemKeyFile.exists()) {
         throw new IOException(String.format("Master key file '%s' does not exist", new Object[]{systemKeyFile.getAbsolutePath()}));
      } else {
         return new LocalSystemKey(systemKeyFile);
      }
   }

   public String getName() {
      return this.keyFile.getName();
   }

   public String getAbsolutePath() {
      return this.keyFile.getAbsolutePath();
   }
}
