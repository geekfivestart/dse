package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.nio.ByteBuffer;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KmipKeyProvider implements IMultiKeyProvider {
   private static final Logger logger = LoggerFactory.getLogger(KmipKeyProvider.class);
   private static final int HEADER_VERSION_SIZE = 1;
   private static final int KEY_LENGTH_SIZE = 4;
   private static final String maxKeyLengthProperty = KmipKeyProvider.class.getName() + ".maxKeyLength";
   private static final int MAX_KEY_LENGTH;
   private static final byte CURRENT_VERSION = 0;
   private final KmipHost kmipHost;
   private final KmipHost.Options options;

   public KmipKeyProvider(KmipHost kmipHost) {
      this(kmipHost, KmipHost.Options.NONE);
   }

   public KmipKeyProvider(KmipHost kmipHost, KmipHost.Options options) {
      this.kmipHost = kmipHost;
      this.options = options != null?options:KmipHost.Options.NONE;
   }

   public SecretKey writeHeader(String cipherName, int keyStrength, ByteBuffer output) throws KeyAccessException, KeyGenerationException {
      KmipHost.Key key = this.kmipHost.getOrCreateByAttrs(cipherName, keyStrength, this.options);
      byte[] idBytes = key.id.getBytes();
      if(idBytes.length > MAX_KEY_LENGTH) {
         throw new RuntimeException(String.format("Max key id size: %s, got %s. Set %s property to at least %s", new Object[]{Integer.valueOf(MAX_KEY_LENGTH), Integer.valueOf(idBytes.length), maxKeyLengthProperty, Integer.valueOf(idBytes.length)}));
      } else {
         output.put((byte)0);
         output.putInt(idBytes.length);
         output.put(idBytes);
         logger.debug("Wrote chunk header for {}", key);
         return key.key;
      }
   }

   public SecretKey readHeader(String cipherName, int keyStrength, ByteBuffer input) throws KeyAccessException, KeyGenerationException {
      byte version = input.get();
      if(version != 0) {
         throw new KeyAccessException(String.format("Unknown kmip header version %s", new Object[]{Byte.valueOf(version)}));
      } else {
         byte[] idBytes = new byte[input.getInt()];
         input.get(idBytes);
         String keyId = new String(idBytes);
         KmipHost.Key key = this.kmipHost.getById(keyId);
         logger.debug("Read chunk header for {}", key);
         return key.key;
      }
   }

   public int headerLength() {
      return 5 + MAX_KEY_LENGTH;
   }

   public SecretKey getSecretKey(String cipherName, int keyStrength) throws KeyAccessException, KeyGenerationException {
      return this.kmipHost.getOrCreateByAttrs(cipherName, keyStrength, this.options).key;
   }

   static {
      MAX_KEY_LENGTH = Integer.getInteger(maxKeyLengthProperty, 256).intValue();
   }
}
