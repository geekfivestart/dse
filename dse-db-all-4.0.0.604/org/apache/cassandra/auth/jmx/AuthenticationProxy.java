package org.apache.cassandra.auth.jmx;

import com.google.common.annotations.VisibleForTesting;
import java.security.AccessController;
import java.security.PrivilegedAction;
import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AuthenticationProxy implements JMXAuthenticator {
   private static Logger logger = LoggerFactory.getLogger(AuthenticationProxy.class);
   private final String loginConfigName;

   public AuthenticationProxy(String loginConfigName) {
      if(loginConfigName == null) {
         throw new ConfigurationException("JAAS login configuration missing for JMX authenticator setup");
      } else {
         this.loginConfigName = loginConfigName;
      }
   }

   public Subject authenticate(Object credentials) {
      AuthenticationProxy.JMXCallbackHandler callbackHandler = new AuthenticationProxy.JMXCallbackHandler(credentials);

      try {
         LoginContext loginContext = new LoginContext(this.loginConfigName, callbackHandler);
         loginContext.login();
         Subject subject = loginContext.getSubject();
         if(!subject.isReadOnly()) {
            AccessController.doPrivileged((PrivilegedAction<Void>)() -> {
               subject.setReadOnly();
               return null;
            });
         }

         return subject;
      } catch (LoginException var5) {
         logger.trace("Authentication exception", var5);
         throw new SecurityException("Authentication error", var5);
      }
   }

   @VisibleForTesting
   public static final class JMXCallbackHandler implements CallbackHandler {
      private char[] username;
      private char[] password;

      public JMXCallbackHandler(Object credentials) {
         if(credentials instanceof String[]) {
            String[] strings = (String[])((String[])credentials);
            if(strings[0] != null) {
               this.username = strings[0].toCharArray();
            }

            if(strings[1] != null) {
               this.password = strings[1].toCharArray();
            }
         }

      }

      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
         for(int i = 0; i < callbacks.length; ++i) {
            if(callbacks[i] instanceof NameCallback) {
               ((NameCallback)callbacks[i]).setName(this.username == null?null:new String(this.username));
            } else {
               if(!(callbacks[i] instanceof PasswordCallback)) {
                  throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback: " + callbacks[i].getClass().getName());
               }

               ((PasswordCallback)callbacks[i]).setPassword(this.password == null?null:this.password);
            }
         }

      }
   }
}
