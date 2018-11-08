package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.config.ClientConfigurationFactory;
import com.datastax.bdp.transport.client.SaslClientDigestCallbackHandler;
import com.datastax.bdp.transport.common.SaslProperties;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseJavaDriverAuthProvider extends DseGSSAPIAuthProvider {
   private static final Logger logger = LoggerFactory.getLogger(DseJavaDriverAuthProvider.class);
   private final ClientConfiguration clientConf;
   private final Token token;
   private final boolean useDigest;

   public DseJavaDriverAuthProvider() {
      this(ClientConfigurationFactory.getClientConfiguration());
   }

   public DseJavaDriverAuthProvider(ClientConfiguration config) {
      this(config, getDelegateToken());
   }

   public DseJavaDriverAuthProvider(ClientConfiguration config, Token token) {
      super(config.getSaslProtocolName());
      this.clientConf = config;
      this.token = token;
      this.useDigest = token != null;
   }

   public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException {
      return (Authenticator)(this.useDigest?new DseJavaDriverAuthProvider.DigestAuthenticator(host, this.clientConf, this.token):super.newAuthenticator(host, authenticator));
   }

   public static Token<? extends TokenIdentifier> getDelegateToken() {
      try {
         Iterator var0 = UserGroupInformation.getCurrentUser().getTokens().iterator();

         while(var0.hasNext()) {
            Token<?> token = (Token)var0.next();
            if(token.getKind().equals(CassandraDelegationTokenIdentifier.CASSANDRA_DELEGATION_KIND)) {
               return token;
            }
         }
      } catch (Exception var2) {
         logger.info("Failed to obtain delegation token", var2);
      }

      return null;
   }

   private static class DigestAuthenticator extends BaseDseAuthenticator {
      private static final String[] SUPPORTED_MECHANISMS = new String[]{"DIGEST-MD5"};
      private static final byte[] MECHANISM;
      private static final byte[] SERVER_INITIAL_CHALLENGE;
      private static final byte[] EMPTY_BYTE_ARRAY;
      private SaslClient saslClient;

      public DigestAuthenticator(InetSocketAddress host, ClientConfiguration clientConf, Token<?> token) {
         try {
            this.saslClient = Sasl.createSaslClient(SUPPORTED_MECHANISMS, (String)null, (String)null, "default", SaslProperties.defaultProperties(clientConf), new SaslClientDigestCallbackHandler(token));
         } catch (SaslException var5) {
            throw new RuntimeException(var5);
         }
      }

      byte[] getMechanism() {
         return (byte[])MECHANISM.clone();
      }

      public byte[] evaluateChallenge(byte[] challenge) {
         if(Arrays.equals(SERVER_INITIAL_CHALLENGE, challenge)) {
            if(!this.saslClient.hasInitialResponse()) {
               return EMPTY_BYTE_ARRAY;
            }

            challenge = EMPTY_BYTE_ARRAY;
         }

         if(this.saslClient.isComplete()) {
            return null;
         } else {
            try {
               return this.saslClient.evaluateChallenge(challenge);
            } catch (SaslException var3) {
               throw new RuntimeException(var3);
            }
         }
      }

      static {
         MECHANISM = "DIGEST-MD5".getBytes(StandardCharsets.UTF_8);
         SERVER_INITIAL_CHALLENGE = "DIGEST-MD5-START".getBytes(StandardCharsets.UTF_8);
         EMPTY_BYTE_ARRAY = new byte[0];
      }
   }
}
