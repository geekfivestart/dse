package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLoginModule implements LoginModule {
   private static final Logger logger = LoggerFactory.getLogger(CassandraLoginModule.class);
   private Subject subject;
   private CallbackHandler callbackHandler;
   private boolean succeeded = false;
   private boolean commitSucceeded = false;
   private String username;
   private char[] password;
   private CassandraPrincipal principal;

   public CassandraLoginModule() {
   }

   public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
      this.subject = subject;
      this.callbackHandler = callbackHandler;
   }

   public boolean login() throws LoginException {
      if(this.callbackHandler == null) {
         logger.info("No CallbackHandler available for authentication");
         throw new LoginException("Authentication failed");
      } else {
         NameCallback nc = new NameCallback("username: ");
         PasswordCallback pc = new PasswordCallback("password: ", false);

         try {
            this.callbackHandler.handle(new Callback[]{nc, pc});
            this.username = nc.getName();
            char[] tmpPassword = pc.getPassword();
            if(tmpPassword == null) {
               tmpPassword = new char[0];
            }

            this.password = Arrays.copyOf(tmpPassword, tmpPassword.length);
            pc.clearPassword();
         } catch (UnsupportedCallbackException | IOException var5) {
            logger.info("Unexpected exception processing authentication callbacks", var5);
            throw new LoginException("Authentication failed");
         }

         try {
            this.authenticate();
         } catch (AuthenticationException var4) {
            this.succeeded = false;
            this.cleanUpInternalState();
            throw new FailedLoginException(var4.getMessage());
         }

         this.succeeded = true;
         return true;
      }
   }

   private void authenticate() {
      if(!StorageService.instance.isAuthSetupComplete()) {
         throw new AuthenticationException("Cannot login as server authentication setup is not yet completed");
      } else {
         IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
         Map<String, String> credentials = new HashMap();
         credentials.put("username", this.username);
         credentials.put("password", String.valueOf(this.password));
         PasswordAuthenticator.checkValidCredentials(credentials);
         AuthenticatedUser user = authenticator.legacyAuthenticate(credentials);
         if(!user.isAnonymous() && !user.isSystem()) {
            if(!((Boolean)DatabaseDescriptor.getAuthManager().canLogin(user.getLoginRole()).blockingGet()).booleanValue()) {
               throw new AuthenticationException(user.getName() + " is not permitted to log in");
            }
         } else {
            throw new AuthenticationException(String.format("Invalid user %s", new Object[]{user.getName()}));
         }
      }
   }

   public boolean commit() throws LoginException {
      if(!this.succeeded) {
         return false;
      } else {
         this.principal = new CassandraPrincipal(this.username);
         if(!this.subject.getPrincipals().contains(this.principal)) {
            this.subject.getPrincipals().add(this.principal);
         }

         this.cleanUpInternalState();
         this.commitSucceeded = true;
         return true;
      }
   }

   public boolean abort() throws LoginException {
      if(!this.succeeded) {
         return false;
      } else {
         if(!this.commitSucceeded) {
            this.succeeded = false;
            this.cleanUpInternalState();
            this.principal = null;
         } else {
            this.logout();
         }

         return true;
      }
   }

   public boolean logout() throws LoginException {
      this.subject.getPrincipals().remove(this.principal);
      this.succeeded = false;
      this.cleanUpInternalState();
      this.principal = null;
      return true;
   }

   private void cleanUpInternalState() {
      this.username = null;
      if(this.password != null) {
         for(int i = 0; i < this.password.length; ++i) {
            this.password[i] = 32;
         }

         this.password = null;
      }

   }
}
