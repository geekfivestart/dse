package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AllowAllAuthenticator implements IAuthenticator {
   private static final IAuthenticator.SaslNegotiator AUTHENTICATOR_INSTANCE = new AllowAllAuthenticator.Negotiator();

   public AllowAllAuthenticator() {
   }

   public boolean requireAuthentication() {
      return false;
   }

   public Set<IResource> protectedResources() {
      return Collections.emptySet();
   }

   public void validateConfiguration() throws ConfigurationException {
   }

   public void setup() {
   }

   public IAuthenticator.SaslNegotiator newSaslNegotiator(InetAddress clientAddress) {
      return AUTHENTICATOR_INSTANCE;
   }

   public AuthenticatedUser legacyAuthenticate(Map<String, String> credentialsData) {
      return AuthenticatedUser.ANONYMOUS_USER;
   }

   private static class Negotiator implements IAuthenticator.SaslNegotiator {
      private Negotiator() {
      }

      public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
         return null;
      }

      public boolean isComplete() {
         return true;
      }

      public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
         return AuthenticatedUser.ANONYMOUS_USER;
      }
   }
}
