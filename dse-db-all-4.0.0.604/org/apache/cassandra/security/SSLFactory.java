package org.apache.cassandra.security;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SSLFactory {
   private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);
   private static boolean checkedExpiry = false;

   public SSLFactory() {
   }

   public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException {
      SSLContext ctx = createSSLContext(options, true);
      SSLServerSocket serverSocket = (SSLServerSocket)ctx.getServerSocketFactory().createServerSocket();

      try {
         serverSocket.setReuseAddress(true);
         prepareSocket(serverSocket, options);
         serverSocket.bind(new InetSocketAddress(address, port), 500);
         return serverSocket;
      } catch (SecurityException | IOException | IllegalArgumentException var6) {
         serverSocket.close();
         throw var6;
      }
   }

   public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
      SSLContext ctx = createSSLContext(options, true);
      SSLSocket socket = (SSLSocket)ctx.getSocketFactory().createSocket(address, port, localAddress, localPort);

      try {
         prepareSocket(socket, options);
         return socket;
      } catch (IllegalArgumentException var8) {
         socket.close();
         throw var8;
      }
   }

   public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port) throws IOException {
      SSLContext ctx = createSSLContext(options, true);
      SSLSocket socket = (SSLSocket)ctx.getSocketFactory().createSocket(address, port);

      try {
         prepareSocket(socket, options);
         return socket;
      } catch (IllegalArgumentException var6) {
         socket.close();
         throw var6;
      }
   }

   public static SSLSocket getSocket(EncryptionOptions options) throws IOException {
      SSLContext ctx = createSSLContext(options, true);
      SSLSocket socket = (SSLSocket)ctx.getSocketFactory().createSocket();

      try {
         prepareSocket(socket, options);
         return socket;
      } catch (IllegalArgumentException var4) {
         socket.close();
         throw var4;
      }
   }

   private static void prepareSocket(SSLServerSocket serverSocket, EncryptionOptions options) {
      String[] suites = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
      if(options.require_endpoint_verification) {
         SSLParameters sslParameters = serverSocket.getSSLParameters();
         sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
         serverSocket.setSSLParameters(sslParameters);
      }

      serverSocket.setEnabledCipherSuites(suites);
      serverSocket.setNeedClientAuth(options.require_client_auth);
   }

   private static void prepareSocket(SSLSocket socket, EncryptionOptions options) {
      String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
      if(options.require_endpoint_verification) {
         SSLParameters sslParameters = socket.getSSLParameters();
         sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
         socket.setSSLParameters(sslParameters);
      }

      socket.setEnabledCipherSuites(suites);
   }

   public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException {
      InputStream tsf = null;
      InputStream ksf = null;

      SSLContext ctx;
      try {
         ctx = SSLContext.getInstance(options.protocol);
         TrustManager[] trustManagers = null;
         KeyStore ks;
         if(buildTruststore) {
            tsf = Files.newInputStream(Paths.get(options.truststore, new String[0]), new OpenOption[0]);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(options.algorithm);
            ks = KeyStore.getInstance(options.store_type);
            ks.load(tsf, options.truststore_password.toCharArray());
            tmf.init(ks);
            trustManagers = tmf.getTrustManagers();
         }

         ksf = Files.newInputStream(Paths.get(options.keystore, new String[0]), new OpenOption[0]);
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(options.algorithm);
         ks = KeyStore.getInstance(options.store_type);
         ks.load(ksf, options.keystore_password.toCharArray());
         if(!checkedExpiry) {
            Enumeration aliases = ks.aliases();

            while(aliases.hasMoreElements()) {
               String alias = (String)aliases.nextElement();
               if(ks.getCertificate(alias).getType().equals("X.509")) {
                  Date expires = ((X509Certificate)ks.getCertificate(alias)).getNotAfter();
                  if(expires.before(new Date())) {
                     logger.warn("Certificate for {} expired on {}", alias, expires);
                  }
               }
            }

            checkedExpiry = true;
         }

         kmf.init(ks, options.keystore_password.toCharArray());
         ctx.init(kmf.getKeyManagers(), trustManagers, (SecureRandom)null);
      } catch (Exception var14) {
         throw new IOException("Error creating the initializing the SSL Context", var14);
      } finally {
         FileUtils.closeQuietly((Closeable)tsf);
         FileUtils.closeQuietly((Closeable)ksf);
      }

      return ctx;
   }

   public static String[] filterCipherSuites(String[] supported, String[] desired) {
      if(Arrays.equals(supported, desired)) {
         return desired;
      } else {
         List<String> ldesired = Arrays.asList(desired);
         ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);
         String[] ret = (String[])Iterables.toArray(Iterables.filter(ldesired, Predicates.in(ssupported)), String.class);
         if(desired.length > ret.length && logger.isWarnEnabled()) {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
         }

         return ret;
      }
   }
}
