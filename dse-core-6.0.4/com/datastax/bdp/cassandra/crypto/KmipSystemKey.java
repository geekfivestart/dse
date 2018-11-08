package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHosts;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import javax.crypto.SecretKey;
import org.apache.cassandra.utils.Pair;

public class KmipSystemKey extends SystemKey {
   private final KmipHost kmipHost;
   private final String keyId;
   private final String cipherName;
   private final int keyStrength;
   private final int ivLength;

   public KmipSystemKey(KmipHost kmipHost, String keyId) throws KeyAccessException {
      this.kmipHost = kmipHost;
      this.keyId = keyId;
      KmipHost.Key key = kmipHost.getById(keyId);
      this.cipherName = key.cipher;
      this.keyStrength = key.strength;

      try {
         this.ivLength = getIvLength(this.cipherName);
      } catch (IOException var5) {
         throw new KeyAccessException(var5);
      }
   }

   protected SecretKey getKey() throws KeyAccessException {
      KmipHost.Key key = this.kmipHost.getById(this.keyId);
      return key.key;
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

   public String getName() {
      return String.format("kmip://%s/%s", new Object[]{this.kmipHost.getHostName(), this.keyId});
   }

   public static boolean isKmipPath(String path) {
      try {
         parseHostAndId(path);
         return true;
      } catch (IOException var2) {
         return false;
      }
   }

   public static KmipSystemKey createKey(String hostName, String cipherName, int keyStrength, KmipHost.Options options) throws IOException {
      KmipHost host = KmipHosts.getHost(hostName);
      String keyId = host.createKey(cipherName, keyStrength, options);

      try {
         return new KmipSystemKey(host, keyId);
      } catch (KeyAccessException var7) {
         throw new IOException(var7);
      }
   }

   public static Pair<String, String> parseHostAndId(String path) throws IOException {
      URL keyUrl;
      try {
         keyUrl = new URL((URL)null, path, new URLStreamHandler() {
            protected URLConnection openConnection(URL u) throws IOException {
               return null;
            }
         });
      } catch (MalformedURLException var3) {
         throw new IOException(var3.getMessage());
      }

      if(!keyUrl.getProtocol().equals("kmip")) {
         throw new IOException("url protocol not kmip");
      } else if(keyUrl.getPath().length() < 2) {
         throw new IOException(String.format("System key path doesn't include key id: %s", new Object[]{path}));
      } else {
         return Pair.create(keyUrl.getHost(), keyUrl.getPath().substring(1));
      }
   }

   public static KmipSystemKey getKey(String path) throws IOException {
      Pair<String, String> keyUrl = parseHostAndId(path);
      KmipHost host = KmipHosts.getHost((String)keyUrl.left);
      if(host == null) {
         throw new IOException(String.format("Kmip host '%s' is not configured", new Object[]{keyUrl.left}));
      } else {
         try {
            return new KmipSystemKey(host, (String)keyUrl.right);
         } catch (KeyAccessException var4) {
            throw new IOException(var4);
         }
      }
   }
}
