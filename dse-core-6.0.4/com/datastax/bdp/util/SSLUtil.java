package com.datastax.bdp.util;

import com.datastax.bdp.config.ClientConfiguration;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Set;
import java.util.Base64.Encoder;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtil {
   private static final Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);
   public static final String[] DEFAULT_CIPHER_SUITES;

   public SSLUtil() {
   }

   public static TrustManagerFactory initTrustManagerFactory(String keystorePath, String keystoreType, String keystorePassword) throws IOException, GeneralSecurityException {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      KeyStore ts = createKeyStore(keystorePath, keystoreType, keystorePassword);
      tmf.init(ts);
      return tmf;
   }

   public static KeyManagerFactory initKeyManagerFactory(String keystorePath, String keystoreType, String keystorePassword, String keyPassword) throws IOException, GeneralSecurityException {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      KeyStore ks = createKeyStore(keystorePath, keystoreType, keystorePassword);
      kmf.init(ks, keyPassword != null?keyPassword.toCharArray():null);
      return kmf;
   }

   private static KeyStore createKeyStore(String keystorePath, String type, String password) throws IOException, GeneralSecurityException {
      if(keystorePath == null) {
         return null;
      } else {
         byte[] keystore = FileUtils.readFileToByteArray(new File(keystorePath));
         return createKeyStore(ByteBuffer.wrap(keystore), type != null?type:KeyStore.getDefaultType(), password != null?password.toCharArray():null);
      }
   }

   private static KeyStore createKeyStore(ByteBuffer keystore, String type, char[] password) throws IOException, GeneralSecurityException {
      KeyStore ks = KeyStore.getInstance(type);
      ks.load(ByteBufferUtil.inputStream(keystore), password);
      return ks;
   }

   public static SSLContext initSSLContext(TrustManagerFactory tmf, KeyManagerFactory kmf, String protocol) throws GeneralSecurityException {
      TrustManager[] tms = tmf != null?tmf.getTrustManagers():null;
      KeyManager[] kms = kmf != null?kmf.getKeyManagers():null;
      SSLContext ctx = SSLContext.getInstance(protocol);
      ctx.init(kms, tms, new SecureRandom());
      return ctx;
   }

   public static Optional<SSLContext> createSSLContext(ClientConfiguration config) throws IOException, GeneralSecurityException {
      if(config.isSslEnabled()) {
         TrustManagerFactory tmf = initTrustManagerFactory(config.getSslTruststorePath(), config.getSslTruststoreType(), config.getSslTruststorePassword());
         KeyManagerFactory kmf = null;
         if(config.getSslKeystorePath() != null) {
            kmf = initKeyManagerFactory(config.getSslKeystorePath(), config.getSslKeystoreType(), config.getSslKeystorePassword(), config.getSslKeystorePassword());
         }

         return Optional.of(initSSLContext(tmf, kmf, config.getSslProtocol()));
      } else {
         return Optional.empty();
      }
   }

   public static KeyStore makeTrustStoreFromKeyStore(String path, String type, String password) throws IOException, GeneralSecurityException {
      KeyStore ks = createKeyStore(path, type, password);
      KeyStore exportedKeyStore = KeyStore.getInstance(ks.getType());
      exportedKeyStore.load((InputStream)null, (char[])null);
      Enumeration aliases = ks.aliases();

      while(aliases.hasMoreElements()) {
         String alias = (String)aliases.nextElement();
         Certificate cert = ks.getCertificate(alias);
         if(cert != null) {
            exportedKeyStore.setCertificateEntry(alias, cert);
         }
      }

      return exportedKeyStore;
   }

   public static byte[] exportTruststore(String path, String type, String password) throws IOException, GeneralSecurityException {
      KeyStore exportedKeyStore = makeTrustStoreFromKeyStore(path, type, password);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      exportedKeyStore.store(out, new char[0]);
      return out.toByteArray();
   }

   public static String saveTruststore(byte[] truststore, String type, Path path, String password, boolean forceOverwrite) {
      try {
         String passwordString = password;
         if(password == null) {
            byte[] truststorePassword = new byte[16];
            (new SecureRandom()).nextBytes(truststorePassword);
            passwordString = (new BigInteger(truststorePassword)).toString(36);
         }

         KeyStore ks = KeyStore.getInstance(type);
         ks.load(new ByteArrayInputStream(truststore), new char[0]);
         Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwx------");
         if(!Files.exists(path, new LinkOption[0])) {
            Files.createFile(path, new FileAttribute[]{PosixFilePermissions.asFileAttribute(permissions)});
            forceOverwrite = true;
         }

         OutputStream out = Files.newOutputStream(path, DseUtil.fileWriteOpts(forceOverwrite));
         Throwable var9 = null;

         try {
            ks.store(out, passwordString.toCharArray());
         } catch (Throwable var19) {
            var9 = var19;
            throw var19;
         } finally {
            if(out != null) {
               if(var9 != null) {
                  try {
                     out.close();
                  } catch (Throwable var18) {
                     var9.addSuppressed(var18);
                  }
               } else {
                  out.close();
               }
            }

         }

         return passwordString;
      } catch (Exception var21) {
         throw new RuntimeException("Failed to save truststore to " + path, var21);
      }
   }

   public static void savePem(byte[] data, Path path, SSLUtil.PemHeader pemHeader) {
      try {
         Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-------");
         Files.createFile(path, new FileAttribute[]{PosixFilePermissions.asFileAttribute(permissions)});
         OutputStream fout = Files.newOutputStream(path, DseUtil.fileWriteOpts(true));
         Throwable var5 = null;

         try {
            PrintStream out = new PrintStream(fout, true);
            Encoder encoder = Base64.getMimeEncoder();
            out.println(pemHeader.getBegin());
            out.println(encoder.encodeToString(data));
            out.println(pemHeader.getEnd());
         } catch (Throwable var16) {
            var5 = var16;
            throw var16;
         } finally {
            if(fout != null) {
               if(var5 != null) {
                  try {
                     fout.close();
                  } catch (Throwable var15) {
                     var5.addSuppressed(var15);
                  }
               } else {
                  fout.close();
               }
            }

         }

      } catch (Exception var18) {
         throw new RuntimeException("Failed to save PEM data to " + path, var18);
      }
   }

   public static Optional<byte[]> getCertificate(Path path, String type, String password, Optional<String> alias) {
      try {
         InputStream out = Files.newInputStream(path, new OpenOption[]{StandardOpenOption.READ});
         Throwable var5 = null;

         Optional var6;
         try {
            var6 = getCertificate(out, type, password, alias);
         } catch (Throwable var16) {
            var5 = var16;
            throw var16;
         } finally {
            if(out != null) {
               if(var5 != null) {
                  try {
                     out.close();
                  } catch (Throwable var15) {
                     var5.addSuppressed(var15);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var6;
      } catch (IOException var18) {
         throw new RuntimeException("Cannot load keystore from " + path.toString());
      }
   }

   public static Optional<byte[]> getCertificate(InputStream in, String type, String password, Optional<String> alias) {
      String ksType = type != null?type:KeyStore.getDefaultType();
      char[] ksPassword = password != null?password.toCharArray():null;

      try {
         KeyStore ks = KeyStore.getInstance(ksType);
         ks.load(in, ksPassword);
         Optional<String> resolvedAlias = getAlias(alias, ks.aliases());
         if(resolvedAlias.isPresent()) {
            Certificate cert = ks.getCertificate((String)resolvedAlias.get());
            if(cert != null) {
               return Optional.of(cert.getEncoded());
            }
         }
      } catch (Exception var9) {
         throw new RuntimeException("Failed to retrieve a certificate", var9);
      }

      return Optional.empty();
   }

   public static Optional<byte[]> getKey(Path path, String type, String password, String keyPassword, Optional<String> alias) {
      try {
         InputStream out = Files.newInputStream(path, new OpenOption[]{StandardOpenOption.READ});
         Throwable var6 = null;

         Optional var7;
         try {
            var7 = getKey(out, type, password, keyPassword, alias);
         } catch (Throwable var17) {
            var6 = var17;
            throw var17;
         } finally {
            if(out != null) {
               if(var6 != null) {
                  try {
                     out.close();
                  } catch (Throwable var16) {
                     var6.addSuppressed(var16);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var7;
      } catch (IOException var19) {
         throw new RuntimeException("Cannot load keystore from " + path.toString());
      }
   }

   public static Optional<byte[]> getKey(InputStream in, String type, String password, String keyPassword, Optional<String> alias) {
      String ksType = type != null?type:KeyStore.getDefaultType();
      char[] ksPassword = password != null?password.toCharArray():null;
      char[] ksKeyPassword = keyPassword != null?keyPassword.toCharArray():null;

      try {
         KeyStore ks = KeyStore.getInstance(ksType);
         ks.load(in, ksPassword);
         Optional<String> resolvedAlias = getAlias(alias, ks.aliases());
         if(resolvedAlias.isPresent()) {
            Key key = ks.getKey((String)resolvedAlias.get(), ksKeyPassword);
            if(key != null) {
               return Optional.of(key.getEncoded());
            }
         }
      } catch (Exception var11) {
         throw new RuntimeException("Failed to retrieve a key", var11);
      }

      return Optional.empty();
   }

   private static Optional<String> getAlias(Optional<String> providedAlias, Enumeration<String> aliases) {
      while(true) {
         if(aliases.hasMoreElements()) {
            String alias = (String)aliases.nextElement();
            if(providedAlias.isPresent()) {
               if(!((String)providedAlias.get()).equals(alias)) {
                  continue;
               }

               return providedAlias;
            }

            return Optional.of(alias);
         }

         return Optional.empty();
      }
   }

   static {
      try {
         DEFAULT_CIPHER_SUITES = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
      } catch (NoSuchAlgorithmException var1) {
         throw new RuntimeException("Cannot get the default set of cipher suites.", var1);
      }
   }

   public static enum PemHeader {
      CERTIFICATE("CERTIFICATE"),
      PRIVATE_KEY("PRIVATE KEY");

      private final String header;

      private PemHeader(String header) {
         this.header = header;
      }

      public String getBegin() {
         return String.format("-----BEGIN %s-----", new Object[]{this.header});
      }

      public String getEnd() {
         return String.format("-----END %s-----", new Object[]{this.header});
      }
   }
}
