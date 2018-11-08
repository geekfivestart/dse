package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.transport.common.SaslProperties;
import com.datastax.bdp.util.DseUtil;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InClusterAuthProvider implements AuthProvider {
   private static final Logger LOGGER = LoggerFactory.getLogger(InClusterAuthProvider.class);
   private final InClusterAuthenticator.Credentials credentials;
   private final ClientConfiguration clientConfiguration;

   public InClusterAuthProvider(InClusterAuthenticator.Credentials credentials, ClientConfiguration clientConfiguration) {
      this.credentials = credentials;
      this.clientConfiguration = clientConfiguration;
   }

   public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException {
      LOGGER.info("Creating new authenticator for host: {}, and authenticator: {}", host, authenticator);
      return new InClusterAuthProvider.DigestAuthenticator(this.clientConfiguration, this.credentials);
   }

   static class SaslClientCallbackHandler implements CallbackHandler {
      private static final Logger LOGGER = LoggerFactory.getLogger(InClusterAuthProvider.SaslClientCallbackHandler.class);
      private final InClusterAuthenticator.Credentials credentials;

      public SaslClientCallbackHandler(InClusterAuthenticator.Credentials credentials) {
         this.credentials = credentials;
      }

      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
         Optional<NameCallback> cbName = DseUtil.firstInstanceOf(callbacks, NameCallback.class);
         Optional<PasswordCallback> cbPassword = DseUtil.firstInstanceOf(callbacks, PasswordCallback.class);
         Optional<RealmCallback> cbRealm = DseUtil.firstInstanceOf(callbacks, RealmCallback.class);
         cbName.ifPresent((nc) -> {
            LOGGER.debug("Setting encoded username {}: {}", this.credentials.id.username, this.credentials.getIdString());
            nc.setName(this.credentials.getIdString());
         });
         cbPassword.ifPresent((pc) -> {
            LOGGER.debug("Setting encoded password");
            pc.setPassword(this.credentials.getPasswordChars());
         });
         cbRealm.ifPresent((rc) -> {
            LOGGER.debug("Setting realm: {}", rc.getDefaultText());
            rc.setText(rc.getDefaultText());
         });
      }
   }

   private static class DigestAuthenticator extends BaseDseAuthenticator {
      private SaslClient saslClient;

      public DigestAuthenticator(ClientConfiguration clientConf, InClusterAuthenticator.Credentials credentials) {
         try {
            this.saslClient = Sasl.createSaslClient(new String[]{"DIGEST-MD5"}, (String)null, (String)null, "default", SaslProperties.defaultProperties(clientConf), new InClusterAuthProvider.SaslClientCallbackHandler(credentials));
         } catch (SaslException var4) {
            throw new RuntimeException(var4);
         }
      }

      byte[] getMechanism() {
         return SaslMechanism.INCLUSTER.mechanism_bytes;
      }

      public byte[] evaluateChallenge(byte[] challenge) {
         if(Arrays.equals(SaslMechanism.INCLUSTER.response, challenge)) {
            if(!this.saslClient.hasInitialResponse()) {
               return this.EMPTY_BYTE_ARRAY;
            }

            challenge = this.EMPTY_BYTE_ARRAY;
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
   }
}
