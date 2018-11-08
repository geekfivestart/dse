package com.datastax.bdp.cassandra.auth;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import java.net.InetSocketAddress;

public class InProcessAuthProvider implements AuthProvider {
   public InProcessAuthProvider() {
   }

   public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException {
      return new InProcessAuthProvider.InProcessAuthenticator();
   }

   private static class InProcessAuthenticator extends BaseDseAuthenticator {
      private InProcessAuthenticator() {
      }

      byte[] getMechanism() {
         return SaslMechanism.INPROCESS.mechanism_bytes;
      }

      public byte[] evaluateChallenge(byte[] bytes) {
         return DseAuthenticator.InProcessSaslNegotiator.generateOneTimeToken();
      }

      public void onAuthenticationSuccess(byte[] bytes) {
      }
   }
}
