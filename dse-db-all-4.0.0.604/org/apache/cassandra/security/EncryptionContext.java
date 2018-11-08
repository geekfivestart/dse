package org.apache.cassandra.security;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.crypto.Cipher;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.utils.Hex;

public class EncryptionContext {
   public static final String ENCRYPTION_CIPHER = "encCipher";
   public static final String ENCRYPTION_KEY_ALIAS = "encKeyAlias";
   public static final String ENCRYPTION_IV = "encIV";
   private final TransparentDataEncryptionOptions tdeOptions;
   private final ICompressor compressor;
   private final CipherFactory cipherFactory;
   private final byte[] iv;
   private final int chunkLength;

   public EncryptionContext() {
      this(new TransparentDataEncryptionOptions());
   }

   public EncryptionContext(TransparentDataEncryptionOptions tdeOptions) {
      this(tdeOptions, (byte[])null, true);
   }

   @VisibleForTesting
   public EncryptionContext(TransparentDataEncryptionOptions tdeOptions, byte[] iv, boolean init) {
      this.tdeOptions = tdeOptions;
      this.compressor = LZ4Compressor.create(Collections.emptyMap());
      this.chunkLength = tdeOptions.chunk_length_kb * 1024;
      this.iv = iv;
      CipherFactory factory = null;
      if(tdeOptions.enabled && init) {
         try {
            factory = new CipherFactory(tdeOptions);
         } catch (Exception var6) {
            throw new ConfigurationException("failed to load key provider for transparent data encryption", var6);
         }
      }

      this.cipherFactory = factory;
   }

   public ICompressor getCompressor() {
      return this.compressor;
   }

   public Cipher getEncryptor() throws IOException {
      return this.cipherFactory.getEncryptor(this.tdeOptions.cipher, this.tdeOptions.key_alias);
   }

   public Cipher getDecryptor() throws IOException {
      if(this.iv != null && this.iv.length != 0) {
         return this.cipherFactory.getDecryptor(this.tdeOptions.cipher, this.tdeOptions.key_alias, this.iv);
      } else {
         throw new IllegalStateException("no initialization vector (IV) found in this context");
      }
   }

   public boolean isEnabled() {
      return this.tdeOptions.enabled;
   }

   public int getChunkLength() {
      return this.chunkLength;
   }

   public byte[] getIV() {
      return this.iv;
   }

   public TransparentDataEncryptionOptions getTransparentDataEncryptionOptions() {
      return this.tdeOptions;
   }

   public boolean equals(Object o) {
      return o instanceof EncryptionContext && this.equals((EncryptionContext)o);
   }

   public boolean equals(EncryptionContext other) {
      return Objects.equals(this.tdeOptions, other.tdeOptions) && Objects.equals(this.compressor, other.compressor) && Arrays.equals(this.iv, other.iv);
   }

   public Map<String, String> toHeaderParameters() {
      Map<String, String> map = new HashMap(3);
      if(this.tdeOptions.enabled) {
         map.put("encCipher", this.tdeOptions.cipher);
         map.put("encKeyAlias", this.tdeOptions.key_alias);
         if(this.iv != null && this.iv.length > 0) {
            map.put("encIV", Hex.bytesToHex(this.iv));
         }
      }

      return map;
   }

   public static EncryptionContext createFromMap(Map<?, ?> parameters, EncryptionContext encryptionContext) {
      if(parameters != null && !parameters.isEmpty()) {
         String keyAlias = (String)parameters.get("encKeyAlias");
         String cipher = (String)parameters.get("encCipher");
         String ivString = (String)parameters.get("encIV");
         if(keyAlias != null && cipher != null) {
            TransparentDataEncryptionOptions tdeOptions = new TransparentDataEncryptionOptions(cipher, keyAlias, encryptionContext.getTransparentDataEncryptionOptions().key_provider);
            byte[] iv = ivString != null?Hex.hexToBytes(ivString):null;
            return new EncryptionContext(tdeOptions, iv, true);
         } else {
            return new EncryptionContext(new TransparentDataEncryptionOptions(false));
         }
      } else {
         return new EncryptionContext(new TransparentDataEncryptionOptions(false));
      }
   }
}
