package org.apache.cassandra.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PasswordAuthenticator implements IAuthenticator {
   private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);
   public static final String USERNAME_KEY = "username";
   public static final String PASSWORD_KEY = "password";
   private static final byte NUL = 0;

   public PasswordAuthenticator() {
   }

   public boolean requireAuthentication() {
      return true;
   }

   protected static boolean checkpw(String password, String hash) {
      try {
         return BCrypt.checkpw(password, hash);
      } catch (Exception var3) {
         logger.warn("Error: invalid password hash encountered, rejecting user", var3);
         return false;
      }
   }

   private AuthenticatedUser authenticate(String username, String password) throws AuthenticationException {
      String hash = (String)DatabaseDescriptor.getAuthManager().getCredentials(username).blockingGet();
      if(hash != null && !hash.isEmpty() && checkpw(password, hash)) {
         return new AuthenticatedUser(username);
      } else {
         throw new AuthenticationException(String.format("Provided username %s and/or password are incorrect", new Object[]{username}));
      }
   }

   public Set<DataResource> protectedResources() {
      return ImmutableSet.of(DataResource.table("system_auth", "roles"));
   }

   public void validateConfiguration() throws ConfigurationException {
   }

   public void setup() {
   }

   static void checkValidCredentials(Map<String, String> credentials) throws AuthenticationException {
      String username = (String)credentials.get("username");
      if(username != null && !username.isEmpty()) {
         String password = (String)credentials.get("password");
         if(password == null || password.isEmpty()) {
            throw new AuthenticationException(String.format("Required key '%s' is missing for provided username %s", new Object[]{"password", username}));
         }
      } else {
         throw new AuthenticationException(String.format("Required key '%s' is missing", new Object[]{"username"}));
      }
   }

   public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException {
      checkValidCredentials(credentials);
      return this.authenticate((String)credentials.get("username"), (String)credentials.get("password"));
   }

   public IAuthenticator.SaslNegotiator newSaslNegotiator(InetAddress clientAddress) {
      return new PasswordAuthenticator.PlainTextSaslAuthenticator(this::authenticate);
   }

   @VisibleForTesting
   static class PlainTextSaslAuthenticator implements IAuthenticator.SaslNegotiator {
      private final BiFunction<String, String, AuthenticatedUser> authFunction;
      private boolean complete = false;
      private String username;
      private String password;

      PlainTextSaslAuthenticator(BiFunction<String, String, AuthenticatedUser> authFunction) {
         this.authFunction = authFunction;
      }

      public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
         this.decodeCredentials(clientResponse);
         this.complete = true;
         return null;
      }

      public boolean isComplete() {
         return this.complete;
      }

      public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
         if(!this.complete) {
            throw new AuthenticationException("SASL negotiation not complete");
         } else {
            return (AuthenticatedUser)this.authFunction.apply(this.username, this.password);
         }
      }

      private void decodeCredentials(byte[] bytes) throws AuthenticationException {
         PasswordAuthenticator.logger.trace("Decoding credentials from client token");
         byte[] user = null;
         byte[] pass = null;
         int end = bytes.length;

         for(int i = bytes.length - 1; i >= 0; --i) {
            if(bytes[i] == 0) {
               if(pass == null) {
                  pass = Arrays.copyOfRange(bytes, i + 1, end);
               } else if(user == null) {
                  user = Arrays.copyOfRange(bytes, i + 1, end);
               }

               end = i;
            }
         }

         if(pass == null) {
            throw new AuthenticationException("Password must not be null");
         } else if(user == null) {
            throw new AuthenticationException("Authentication ID must not be null");
         } else {
            this.username = new String(user, StandardCharsets.UTF_8);
            this.password = new String(pass, StandardCharsets.UTF_8);
         }
      }
   }
}
