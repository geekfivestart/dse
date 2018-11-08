package com.datastax.bdp.node.transport;

import com.datastax.bdp.transport.common.DseReloadableTrustManagerProvider;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.SSLFactory;

public class SSLOptions {
   public final SSLContext sslContext;
   public final String[] cipherSuites;
   public final boolean requireClientAuth;

   public SSLOptions(SSLContext sslContext, String[] cipherSuites, boolean requireClientAuth) {
      this.sslContext = sslContext;
      this.cipherSuites = cipherSuites;
      this.requireClientAuth = requireClientAuth;
   }

   public final SslHandler createServerSslHandler() {
      SSLEngine sslEngine = this.sslContext.createSSLEngine();
      sslEngine.setUseClientMode(false);
      sslEngine.setEnabledCipherSuites(this.cipherSuites);
      sslEngine.setNeedClientAuth(this.requireClientAuth);
      return new SslHandler(sslEngine);
   }

   public final SslHandler createClientSslHandler() {
      SSLEngine sslEngine = this.sslContext.createSSLEngine();
      sslEngine.setUseClientMode(true);
      sslEngine.setEnabledCipherSuites(this.cipherSuites);
      return new SslHandler(sslEngine);
   }

   public static Optional<SSLOptions> getDefaultForInterNode() {
      ServerEncryptionOptions opts = DatabaseDescriptor.getServerEncryptionOptions();
      return opts.internode_encryption != InternodeEncryption.none?Optional.of(getDefault(opts)):Optional.empty();
   }

   public static Optional<SSLOptions> getDefaultForRPC() {
      ClientEncryptionOptions opts = DatabaseDescriptor.getClientEncryptionOptions();
      return opts.enabled?Optional.of(getDefault(opts)):Optional.empty();
   }

   public static SSLOptions getDefault(EncryptionOptions opts) {
      try {
         DseReloadableTrustManagerProvider.maybeInstall();
         SSLContext sslContext = SSLFactory.createSSLContext(opts, true);
         return new SSLOptions(sslContext, opts.cipher_suites, opts.require_client_auth);
      } catch (IOException var2) {
         throw new ConfigurationException("Failed to initialize SSL", var2);
      }
   }
}
