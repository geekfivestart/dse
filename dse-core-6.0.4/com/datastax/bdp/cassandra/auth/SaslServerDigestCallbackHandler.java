package com.datastax.bdp.cassandra.auth;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SaslServerDigestCallbackHandler implements CallbackHandler {
   private static final Logger logger = LoggerFactory.getLogger(SaslServerDigestCallbackHandler.class);
   private final CassandraDelegationTokenSecretManager tokenSecretManager;

   @Inject
   SaslServerDigestCallbackHandler(CassandraDelegationTokenSecretManager secretManagerProvider) {
      this.tokenSecretManager = secretManagerProvider;
   }

   public void handle(Callback[] callbacks) throws InvalidToken, UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      Callback[] var5 = callbacks;
      int var6 = callbacks.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         Callback callback = var5[var7];
         if(callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback)callback;
         } else if(callback instanceof NameCallback) {
            nc = (NameCallback)callback;
         } else if(callback instanceof PasswordCallback) {
            pc = (PasswordCallback)callback;
         } else if(!(callback instanceof RealmCallback)) {
            throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
         }
      }

      if(pc != null) {
         assert nc != null;

         byte[] tokenStr = Base64.decodeBase64(nc.getDefaultName().getBytes());
         CassandraDelegationTokenIdentifier dt = new CassandraDelegationTokenIdentifier();

         try {
            dt.readFields(new DataInputStream(new ByteArrayInputStream(tokenStr)));
         } catch (IOException var10) {
            throw (InvalidToken)(new InvalidToken("Can't de-serialize tokenIdentifier")).initCause(var10.getCause());
         }

         byte[] password;
         try {
            password = this.tokenSecretManager.retrievePassword(dt);
         } catch (IOException var9) {
            throw (InvalidToken)(new InvalidToken("Invalid password")).initCause(var9.getCause());
         }

         if(logger.isDebugEnabled()) {
            logger.debug("SASL server DIGEST-MD5 callback: setting password for client: " + dt.getUser());
         }

         pc.setPassword((new String(Base64.encodeBase64(password))).toCharArray());
      }

      if(ac != null) {
         String authid = ac.getAuthenticationID();
         String authzid = ac.getAuthorizationID();
         if(authid.equals(authzid)) {
            ac.setAuthorized(true);
         } else {
            ac.setAuthorized(false);
         }

         if(ac.isAuthorized()) {
            if(logger.isDebugEnabled()) {
               logger.debug("SASL server DIGEST-MD5 callback: setting canonicalized client ID: " + authzid);
            }

            ac.setAuthorizedID(authzid);
         }
      }

   }
}
