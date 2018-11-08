package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface IAuthenticator {
   default IAuthenticator.TransitionalMode getTransitionalMode() {
      return IAuthenticator.TransitionalMode.DISABLED;
   }

   default <T extends IAuthenticator> T implementation() {
      return this;
   }

   default <T extends IAuthenticator> boolean isImplementationOf(Class<T> implClass) {
      return implClass.isAssignableFrom(this.implementation().getClass());
   }

   boolean requireAuthentication();

   Set<? extends IResource> protectedResources();

   void validateConfiguration() throws ConfigurationException;

   void setup();

   IAuthenticator.SaslNegotiator newSaslNegotiator(InetAddress var1);

   AuthenticatedUser legacyAuthenticate(Map<String, String> var1) throws AuthenticationException;

   public interface SaslNegotiator {
      byte[] evaluateResponse(byte[] var1) throws AuthenticationException;

      boolean isComplete();

      AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;
   }

   public static enum TransitionalMode {
      DISABLED {
         public boolean mapAllNonSuperuserToAnonymous() {
            return false;
         }

         public boolean failedAuthenticationMapsToAnonymous() {
            return false;
         }

         public boolean missingCredentialsMapsToAnonymous() {
            return false;
         }
      },
      PERMISSIVE {
         public boolean mapAllNonSuperuserToAnonymous() {
            return true;
         }

         public boolean failedAuthenticationMapsToAnonymous() {
            return true;
         }

         public boolean missingCredentialsMapsToAnonymous() {
            return true;
         }
      },
      NORMAL {
         public boolean mapAllNonSuperuserToAnonymous() {
            return false;
         }

         public boolean failedAuthenticationMapsToAnonymous() {
            return true;
         }

         public boolean missingCredentialsMapsToAnonymous() {
            return true;
         }
      },
      STRICT {
         public boolean mapAllNonSuperuserToAnonymous() {
            return false;
         }

         public boolean failedAuthenticationMapsToAnonymous() {
            return false;
         }

         public boolean missingCredentialsMapsToAnonymous() {
            return true;
         }
      };

      private TransitionalMode() {
      }

      public abstract boolean mapAllNonSuperuserToAnonymous();

      public abstract boolean failedAuthenticationMapsToAnonymous();

      public abstract boolean missingCredentialsMapsToAnonymous();
   }
}
