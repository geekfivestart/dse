package com.datastax.bdp.transport.client;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslClientDigestCallbackHandler implements CallbackHandler {
   private static final Logger logger = LoggerFactory.getLogger(SaslClientDigestCallbackHandler.class);
   private final String userName;
   private final char[] userPassword;

   public SaslClientDigestCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = new String(Base64.encodeBase64(token.getIdentifier()));
      this.userPassword = (new String(Base64.encodeBase64(token.getPassword()))).toCharArray();
   }

   public <T extends Callback> T findCallback(Callback[] callbacks, Class<T> cls) {
      Callback[] var3 = callbacks;
      int var4 = callbacks.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         Callback callback = var3[var5];
         if(cls.isInstance(callback)) {
            return (Callback)cls.cast(callback);
         }
      }

      return null;
   }

   public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = (NameCallback)this.findCallback(callbacks, NameCallback.class);
      PasswordCallback pc = (PasswordCallback)this.findCallback(callbacks, PasswordCallback.class);
      RealmCallback rc = (RealmCallback)this.findCallback(callbacks, RealmCallback.class);
      if(nc != null) {
         if(logger.isDebugEnabled()) {
            logger.debug("SASL client callback: setting username: " + this.userName);
         }

         nc.setName(this.userName);
      }

      if(pc != null) {
         if(logger.isDebugEnabled()) {
            logger.debug("SASL client callback: setting userPassword");
         }

         pc.setPassword(this.userPassword);
      }

      if(rc != null) {
         if(logger.isDebugEnabled()) {
            logger.debug("SASL client callback: setting realm: " + rc.getDefaultText());
         }

         rc.setText(rc.getDefaultText());
      }

   }
}
