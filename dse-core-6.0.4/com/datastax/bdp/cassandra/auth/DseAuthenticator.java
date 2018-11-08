package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.cassandra.audit.AuditLoggerUtils;
import com.datastax.bdp.cassandra.auth.negotiators.ProxyAuthenticatedUser;
import com.datastax.bdp.config.ClientConfigurationFactory;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.datastax.bdp.db.audit.IAuditLogger;
import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.transport.common.SaslProperties;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.transport.server.DigestAuthUtils;
import com.datastax.bdp.transport.server.KerberosServerUtils;
import com.datastax.bdp.util.DseUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.IAuthenticator.SaslNegotiator;
import org.apache.cassandra.auth.IAuthenticator.TransitionalMode;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseAuthenticator implements IAuthenticator {
   private static Logger logger = LoggerFactory.getLogger(DseAuthenticator.class);
   public static final String SASL_DEFAULT_REALM = "default";
   private static final String DEFAULT_SUPER_USER_NAME = "cassandra";
   protected static final String KEYTAB_UNREADABLE_ERROR = "The dse service keytab at this location %s either doesn't exist or cannot be read by the dse service";
   protected boolean enabled;
   protected AuthenticationScheme defaultScheme;
   protected Set<AuthenticationScheme> allowedSchemes = new LinkedHashSet();
   protected TransitionalMode transitionalMode;
   private PasswordAuthenticator internalAuthenticator;
   private boolean schemePermissions;
   private IAuditLogger auditLogger;

   public DseAuthenticator() {
      this.transitionalMode = TransitionalMode.DISABLED;
      this.schemePermissions = false;
      this.enabled = DseConfig.isAuthenticationEnabled();
   }

   public DseAuthenticator(boolean enabled) {
      this.transitionalMode = TransitionalMode.DISABLED;
      this.schemePermissions = false;
      this.enabled = enabled;
   }

   static AuthenticatedUser proxy(AuthenticatedUser user, String proxiedUserName) {
      return new ProxyAuthenticatedUser(user, new AuthenticatedUser(proxiedUserName));
   }

   public TransitionalMode getTransitionalMode() {
      return this.transitionalMode;
   }

   public boolean requireAuthentication() {
      return this.enabled;
   }

   public Set<IResource> protectedResources() {
      return Collections.emptySet();
   }

   public void validateConfiguration() throws ConfigurationException {
      this.defaultScheme = AuthenticationScheme.valueOf(DseConfig.getDefaultAuthenticationScheme().toUpperCase());
      this.allowedSchemes.add(this.defaultScheme);
      this.allowedSchemes.add(AuthenticationScheme.INPROCESS);
      this.allowedSchemes.add(AuthenticationScheme.INCLUSTER);
      Iterator var1 = DseConfig.getOtherAuthenticationSchemes().iterator();

      while(var1.hasNext()) {
         String scheme = (String)var1.next();
         this.allowedSchemes.add(AuthenticationScheme.valueOf(scheme.toUpperCase()));
      }

      if(this.usingScheme(AuthenticationScheme.KERBEROS) && DseConfig.isAllowDigestWithKerberos()) {
         this.allowedSchemes.add(AuthenticationScheme.TOKEN);
      }

      this.transitionalMode = TransitionalMode.valueOf(DseConfig.getAuthenticationTransitionalMode().toUpperCase());
      if(this.enabled) {
         if(this.usingScheme(AuthenticationScheme.KERBEROS)) {
            this.validateKeytab();
         }

         if(this.usingScheme(AuthenticationScheme.LDAP)) {
            LdapUtils.instance.validateAuthenticationConfiguration();
         }
      }

   }

   protected void validateKeytab() throws ConfigurationException {
      if(DseConfig.getDseServiceKeytab() == null || !(new File(DseConfig.getDseServiceKeytab())).canRead()) {
         throw new ConfigurationException(String.format("The dse service keytab at this location %s either doesn't exist or cannot be read by the dse service", new Object[]{DseConfig.getDseServiceKeytab()}));
      }
   }

   public void setup() {
      if(this.usingScheme(AuthenticationScheme.LDAP)) {
         LdapUtils.instance.setupAuthentication();
      }

      if(this.usingScheme(AuthenticationScheme.INTERNAL)) {
         this.internalAuthenticator = new PasswordAuthenticator();
         this.internalAuthenticator.setup();
      }

      this.schemePermissions = DseConfig.isAuthenticationSchemePermissions() && DatabaseDescriptor.getAuthorizer().isImplementationOf(DseAuthorizer.class);
      this.auditLogger = DatabaseDescriptor.getAuditLogger();
   }

   public boolean isKerberosDefaultScheme() {
      return this.enabled && this.defaultScheme == AuthenticationScheme.KERBEROS;
   }

   public boolean isKerberosEnabled() {
      return this.enabled && this.usingScheme(AuthenticationScheme.KERBEROS);
   }

   public boolean isLdapAuthEnabled() {
      return this.enabled && this.usingScheme(AuthenticationScheme.LDAP);
   }

   public boolean isPlainTextAuthEnabled() {
      return this.enabled && (this.usingScheme(AuthenticationScheme.INTERNAL) || this.usingScheme(AuthenticationScheme.LDAP));
   }

   public boolean isInternalAuthEnabled() {
      return this.enabled && this.usingScheme(AuthenticationScheme.INTERNAL);
   }

   public boolean isDigestAuthEnabled() {
      return this.enabled && this.usingScheme(AuthenticationScheme.TOKEN);
   }

   private boolean usingScheme(AuthenticationScheme scheme) {
      return this.allowedSchemes.contains(scheme);
   }

   public SaslNegotiator newSaslNegotiator(InetAddress clientAddress) {
      return new DseAuthenticator.UnifiedSaslNegotiator(clientAddress);
   }

   private boolean forceAnonymous(String user, boolean passwordSupplied) {
      return this.getTransitionalMode().mapAllNonSuperuserToAnonymous() && !user.equals("cassandra")?true:this.getTransitionalMode().missingCredentialsMapsToAnonymous() && StringUtils.isEmpty(user) && !passwordSupplied;
   }

   public static Credentials decodeHttpBasicCredentials(String base64encodedCredentials) {
      String[] credsArray;
      String username;
      try {
         username = new String(Base64.decodeBase64(base64encodedCredentials));
         credsArray = username.split(":");
      } catch (Exception var4) {
         logger.debug("Failed to decode credentials from Base64");
         throw new DseAuthenticationException();
      }

      if(credsArray.length != 2) {
         throw new DseAuthenticationException();
      } else {
         username = credsArray[0];
         String password = credsArray[1];
         if(StringUtils.isNotBlank(username) && StringUtils.isBlank(password)) {
            throw new DseAuthenticationException(username);
         } else {
            return new Credentials(username, password);
         }
      }
   }

   public AuthenticatedUser plainTextAuthenticate(Credentials credentials, DseAuthenticator.AuthenticationState state) throws DseAuthenticationException {
      Iterator var3 = (state.legacy?Collections.singleton(this.defaultScheme):this.allowedSchemes).iterator();

      while(var3.hasNext()) {
         AuthenticationScheme scheme = (AuthenticationScheme)var3.next();

         try {
            state.actualScheme = scheme;
            switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$AuthenticationScheme[scheme.ordinal()]) {
            case 1:
               return this.internalAuthenticator.legacyAuthenticate(credentials.toMap());
            case 2:
               return LdapUtils.instance.authenticate(credentials);
            }
         } catch (AuthenticationException var6) {
            ;
         }
      }

      throw new DseAuthenticationException(credentials.authorizationUser, credentials.authenticationUser);
   }

   public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws DseAuthenticationException {
      if(!this.enabled) {
         return AuthenticatedUser.ANONYMOUS_USER;
      } else {
         Credentials creds = new Credentials(credentials);
         DseAuthenticator.AuthenticationState authenticationState = new DseAuthenticator.AuthenticationState(null);
         AuthenticatedUser user = null;
         if(this.forceAnonymous(creds.authenticationUser, !StringUtils.isEmpty(creds.password))) {
            return AuthenticatedUser.ANONYMOUS_USER;
         } else {
            try {
               if(StringUtils.isNotEmpty(creds.authenticationUser)) {
                  if(StringUtils.isNotEmpty(creds.password)) {
                     user = this.plainTextAuthenticate(creds, authenticationState);
                  } else if(this.usingScheme(AuthenticationScheme.KERBEROS)) {
                     authenticationState.actualScheme = AuthenticationScheme.KERBEROS;
                     user = KerberosServerUtils.getUserFromAuthzId(creds.authenticationUser);
                  }
               }

               if(user == null) {
                  throw new DseAuthenticationException(creds.authenticationUser);
               } else {
                  this.checkPermissions(user, authenticationState.actualScheme);
                  return user;
               }
            } catch (AuthenticationException var6) {
               if(this.getTransitionalMode().failedAuthenticationMapsToAnonymous()) {
                  return AuthenticatedUser.ANONYMOUS_USER;
               } else {
                  throw var6;
               }
            }
         }
      }
   }

   public void checkPermissions(AuthenticatedUser user, AuthenticationScheme authenticationScheme) throws DseAuthenticationException {
      AuthenticatedUser authenticationUser = user instanceof ProxyAuthenticatedUser?((ProxyAuthenticatedUser)user).authenticatedUser:user;
      UserRolesAndPermissions userRolesAndPermissions = (UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(authenticationUser));
      if(!userRolesAndPermissions.isSuper() && this.schemePermissions && !this.getTransitionalMode().failedAuthenticationMapsToAnonymous() && !authenticationUser.isAnonymous()) {
         if(!userRolesAndPermissions.hasPermission(AuthenticationSchemeResource.scheme(authenticationScheme), CorePermission.EXECUTE)) {
            throw new DseAuthenticationException(user.getName(), user.getAuthenticatedName());
         }
      }
   }

   private SaslNegotiator getSaslNegotiatorForScheme(DseAuthenticator.AuthenticationState state) throws DseAuthenticationException {
      if(state.selectedScheme.saslMechanism == SaslMechanism.PLAIN) {
         if(!DatabaseDescriptor.getClientEncryptionOptions().enabled) {
            String method = DseConfig.getPlainTextWithoutSsl();
            if(method.equals("block")) {
               throw new DseAuthenticationException();
            }

            if(method.equals("warn")) {
               logger.warn("Plain text authentication without client / server encryption is strongly discouraged");
            }
         }

         return new DseAuthenticator.PlainTextSaslNegotiator(state);
      } else {
         return (SaslNegotiator)(state.selectedScheme.saslMechanism == SaslMechanism.DIGEST?new DseAuthenticator.DigestMD5SaslNegotiator(state):(state.selectedScheme.saslMechanism == SaslMechanism.INPROCESS?new DseAuthenticator.InProcessSaslNegotiator():(state.selectedScheme.saslMechanism == SaslMechanism.INCLUSTER?new DseAuthenticator.InClusterSaslNegotiator():new DseAuthenticator.GSSAPISaslNegotiator(state))));
      }
   }

   private class AuthenticationState {
      public AuthenticationScheme selectedScheme;
      public AuthenticationScheme actualScheme;
      public boolean legacy;
      public InetAddress clientAddress;

      private AuthenticationState() {
         this.legacy = false;
      }
   }

   protected static class InProcessSaslNegotiator implements SaslNegotiator {
      private boolean complete = false;
      private static final int CACHE_VALIDITY_IN_S = 5;
      private static final Cache<UUID, UUID> uuidCache;

      protected InProcessSaslNegotiator() {
      }

      public static byte[] generateOneTimeToken() {
         UUID uuid = UUIDGen.getTimeUUID();
         uuidCache.put(uuid, uuid);
         return UUIDSerializer.instance.serialize(uuid).array();
      }

      public byte[] evaluateResponse(byte[] bytes) throws AuthenticationException {
         UUID uuid = UUIDSerializer.instance.deserialize(ByteBuffer.wrap(bytes));
         if(uuid != null && uuidCache.getIfPresent(uuid) != null) {
            uuidCache.invalidate(uuid);
            this.complete = true;
            return null;
         } else {
            throw new DseAuthenticationException("Failed to login. Please re-try.");
         }
      }

      public boolean isComplete() {
         return this.complete;
      }

      public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
         return AuthenticatedUser.INPROC_USER;
      }

      static {
         uuidCache = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.SECONDS).build();
      }
   }

   protected class InClusterSaslNegotiator extends DseAuthenticator.DseSaslNegotiator {
      private final SaslServer saslServer;

      public InClusterSaslNegotiator() {
         super();

         try {
            this.saslServer = Sasl.createSaslServer("DIGEST-MD5", (String)null, "default", SaslProperties.defaultProperties(ClientConfigurationFactory.getClientConfiguration()), new DseAuthenticator.InClusterSaslNegotiator.InClusterCallbackHandler());
         } catch (SaslException var3) {
            DseAuthenticator.logger.error("Error initialising SASL server", var3);
            throw new RuntimeException(var3);
         }
      }

      public AuthenticatedUser getAuthenticatedUser() throws DseAuthenticationException {
         InClusterAuthenticator.TokenId tokenId = InClusterAuthenticator.TokenId.compose(this.saslServer.getAuthorizationID());
         return new AuthenticatedUser(tokenId.username);
      }

      public byte[] evaluateResponse(byte[] token) throws DseAuthenticationException {
         try {
            DseAuthenticator.logger.debug("Evaluating input token {}", token == null?"null":Integer.valueOf(token.length));
            return this.saslServer.evaluateResponse(token);
         } catch (SaslException var3) {
            throw new DseAuthenticationException();
         }
      }

      public boolean isComplete() {
         return this.saslServer.isComplete();
      }

      public class InClusterCallbackHandler extends DseAuthenticator.DseSaslNegotiator.DseSaslCallbackHandler {
         private DigestTokensManager digestTokensManager = (DigestTokensManager)DseInjector.get().getInstance(DigestTokensManager.class);

         public InClusterCallbackHandler() {
            super();
         }

         protected void handleNamePasswordCallback(Optional<NameCallback> nameCallback, Optional<PasswordCallback> passwordCallback) throws IOException {
            passwordCallback.ifPresent((pc) -> {
               byte[] decomposedId = Base64.decodeBase64((String)nameCallback.flatMap((name) -> {
                  return Optional.ofNullable(name.getDefaultName());
               }).orElseThrow(DseAuthenticationException::<init>));
               Optional<Pair<byte[], Long>> decomposedPassword = this.digestTokensManager.getPasswordById(decomposedId, ConsistencyLevel.LOCAL_QUORUM);
               if(!decomposedPassword.isPresent()) {
                  throw new DseAuthenticationException(InClusterAuthenticator.TokenId.compose(decomposedId).username);
               } else {
                  char[] password = InClusterAuthenticator.Credentials.getPasswordChars((byte[])((Pair)decomposedPassword.get()).left);
                  DseAuthenticator.logger.debug("Read password of {}", InClusterAuthenticator.TokenId.compose(decomposedId).username);
                  pc.setPassword(password);
               }
            });
         }
      }
   }

   protected class DigestMD5SaslNegotiator extends DseAuthenticator.DseExternalSaslNegotiator {
      private final SaslServer saslServer;
      private CassandraDelegationTokenIdentifier tokenId;

      public DigestMD5SaslNegotiator(DseAuthenticator.AuthenticationState state) {
         super();
         state.actualScheme = AuthenticationScheme.KERBEROS;

         try {
            this.saslServer = Sasl.createSaslServer(SaslMechanism.DIGEST.mechanism, (String)null, "default", SaslProperties.defaultProperties(ClientConfigurationFactory.getClientConfiguration()), new DseAuthenticator.DigestMD5SaslNegotiator.DigestMD5CallbackHandler(null));
         } catch (SaslException var4) {
            DseAuthenticator.logger.error("Error initialising SASL server", var4);
            throw new RuntimeException(var4);
         }
      }

      protected String getAuthenticationUser() {
         return KerberosServerUtils.getUserNameFromAuthzId(this.tokenId.getRealUser().toString());
      }

      protected String getAuthorizationUser() {
         return KerberosServerUtils.getUserNameFromAuthzId(this.tokenId.getOwner().toString());
      }

      protected boolean passwordSupplied() {
         return true;
      }

      public byte[] evaluateResponse(byte[] token) throws DseAuthenticationException {
         try {
            DseAuthenticator.logger.debug("Evaluating input token {}", token == null?"null":Integer.valueOf(token.length));
            return this.saslServer.evaluateResponse(token);
         } catch (SaslException var3) {
            throw new DseAuthenticationException();
         }
      }

      public boolean isComplete() {
         return this.saslServer.isComplete();
      }

      private class DigestMD5CallbackHandler extends DseAuthenticator.DseSaslNegotiator.DseSaslCallbackHandler {
         private final CassandraDelegationTokenSecretManager tokenSecretManager;

         private DigestMD5CallbackHandler() {
            super();
            this.tokenSecretManager = (CassandraDelegationTokenSecretManager)DseInjector.get().getInstance(CassandraDelegationTokenSecretManager.class);
         }

         protected void handleAuthorizeCallback(Optional<AuthorizeCallback> authorizeCallback) throws IOException {
            AuthorizeCallback tokenAuthorizeCallback = new AuthorizeCallback(DigestMD5SaslNegotiator.this.getAuthenticationUser(), DigestMD5SaslNegotiator.this.getAuthorizationUser());
            super.handleAuthorizeCallback(Optional.of(tokenAuthorizeCallback));
            if(tokenAuthorizeCallback.isAuthorized()) {
               super.handleAuthorizeCallback(authorizeCallback);
            } else {
               authorizeCallback.ifPresent((cb) -> {
                  cb.setAuthorized(false);
               });
            }

         }

         protected void handleNamePasswordCallback(Optional<NameCallback> nameCallback, Optional<PasswordCallback> passwordCallback) throws IOException {
            if(passwordCallback.isPresent()) {
               if(!nameCallback.isPresent()) {
                  throw new IOException("Password callback without name callback?  Something is very wrong.");
               }

               CassandraDelegationTokenIdentifier dt;
               try {
                  dt = DigestAuthUtils.getCassandraDTIdentifier(Base64.decodeBase64(((NameCallback)nameCallback.get()).getDefaultName().getBytes()));
               } catch (IOException var6) {
                  throw (InvalidToken)(new InvalidToken("Can't de-serialize tokenIdentifier")).initCause(var6.getCause());
               }

               DigestMD5SaslNegotiator.this.tokenId = dt;

               try {
                  ((PasswordCallback)passwordCallback.get()).setPassword((new String(Base64.encodeBase64(this.tokenSecretManager.retrievePassword(dt)))).toCharArray());
                  DseAuthenticator.logger.debug("Setting DIGEST-MD5 password for client {}", dt);
               } catch (IOException var5) {
                  throw (InvalidToken)(new InvalidToken("Invalid password")).initCause(var5.getCause());
               }
            }

         }
      }
   }

   protected class GSSAPISaslNegotiator extends DseAuthenticator.DseExternalSaslNegotiator {
      private final Subject serverIdentity;
      private final SaslServer saslServer;
      private DseAuthenticator.AuthenticationState state;
      private String authenticatedUser;

      public GSSAPISaslNegotiator(DseAuthenticator.AuthenticationState state) {
         super();
         this.state = state;
         this.state.actualScheme = AuthenticationScheme.KERBEROS;

         String identityPrincipalName;
         try {
            identityPrincipalName = DseConfig.getDseServicePrincipal().asLocal();
            if(DseAuthenticator.logger.isTraceEnabled()) {
               DseAuthenticator.logger.trace("[gssapi-negotiator] service principal = {}", identityPrincipalName);
            }

            KerberosServerUtils.validateServicePrincipal(identityPrincipalName);
            this.serverIdentity = KerberosServerUtils.loginServer(identityPrincipalName);
         } catch (Exception var5) {
            DseAuthenticator.logger.error("Error obtaining subject for server identity", var5);
            throw new RuntimeException(var5);
         }

         identityPrincipalName = ((Principal)this.serverIdentity.getPrincipals().iterator().next()).getName();
         if(DseAuthenticator.logger.isTraceEnabled()) {
            DseAuthenticator.logger.trace("[gssapi-negotiator] service identity principal name = {}", identityPrincipalName);
         }

         final ServicePrincipal servicePrincipal = new ServicePrincipal(identityPrincipalName);
         this.saslServer = (SaslServer)Subject.doAs(this.serverIdentity, new PrivilegedAction<SaslServer>() {
            public SaslServer run() {
               try {
                  return Sasl.createSaslServer(SaslMechanism.GSSAPI.mechanism, servicePrincipal.service, servicePrincipal.host, SaslProperties.defaultProperties(ClientConfigurationFactory.getClientConfiguration()), GSSAPISaslNegotiator.this.new DseGSSCallbackHandler());
               } catch (SaslException var2) {
                  DseAuthenticator.logger.error("Error initialising SASL server", var2);
                  throw new RuntimeException(var2);
               }
            }
         });
      }

      protected String getAuthenticationUser() {
         return this.authenticatedUser == null?this.getAuthorizationUser():KerberosServerUtils.getUserNameFromAuthzId(this.authenticatedUser);
      }

      protected String getAuthorizationUser() {
         return KerberosServerUtils.getUserNameFromAuthzId(this.saslServer.getAuthorizationID());
      }

      protected boolean passwordSupplied() {
         return true;
      }

      public byte[] evaluateResponse(final byte[] token) throws DseAuthenticationException {
         try {
            return (byte[])Subject.doAs(this.serverIdentity, new PrivilegedExceptionAction<byte[]>() {
               public byte[] run() throws SaslException {
                  try {
                     return GSSAPISaslNegotiator.this.saslServer.evaluateResponse(token);
                  } catch (SaslException var2) {
                     DseAuthenticator.logger.debug("Kerberos sasl exception", var2);
                     throw new DseAuthenticationException("unknown");
                  }
               }
            });
         } catch (PrivilegedActionException var3) {
            throw new DseAuthenticationException();
         }
      }

      public boolean isComplete() {
         return this.saslServer.isComplete();
      }

      protected class DseGSSCallbackHandler extends DseAuthenticator.DseSaslNegotiator.DseSaslCallbackHandler {
         protected DseGSSCallbackHandler() {
            super();
         }

         public void handleAuthorizeCallback(Optional<AuthorizeCallback> authorizeCallback) throws IOException {
            super.handleAuthorizeCallback(authorizeCallback);
            authorizeCallback.ifPresent((authCb) -> {
               GSSAPISaslNegotiator.this.authenticatedUser = authCb.getAuthenticationID();
            });
         }
      }
   }

   protected class PlainTextSaslNegotiator extends DseAuthenticator.DseExternalSaslNegotiator {
      private final DseAuthenticator.AuthenticationState state;
      private boolean complete = false;
      private Credentials credentials;

      public PlainTextSaslNegotiator(DseAuthenticator.AuthenticationState state) {
         super();
         this.state = state;
      }

      protected String getAuthenticationUser() {
         return this.credentials.authenticationUser;
      }

      protected String getAuthorizationUser() {
         return this.credentials.authorizationUser;
      }

      protected boolean passwordSupplied() {
         return !StringUtils.isEmpty(this.credentials.password);
      }

      public byte[] evaluateResponse(byte[] clientResponse) throws DseAuthenticationException {
         int split1 = ArrayUtils.indexOf(clientResponse, 0);
         int split2 = ArrayUtils.indexOf(clientResponse, 0, split1 + 1);
         if(split1 >= 0 && split2 >= 0) {
            this.credentials = new Credentials(new String(clientResponse, split1 + 1, split2 - split1 - 1, StandardCharsets.UTF_8), new String(clientResponse, split2 + 1, clientResponse.length - split2 - 1, StandardCharsets.UTF_8), new String(clientResponse, 0, split1, StandardCharsets.UTF_8));

            try {
               DseAuthenticator.this.plainTextAuthenticate(this.credentials, this.state);
               if(!this.authorize(this.credentials.authenticationUser, this.credentials.authorizationUser)) {
                  this.authenticationError = true;
               }
            } catch (AuthenticationException var5) {
               this.authenticationError = true;
            }

            this.complete = true;
            return null;
         } else {
            throw new DseAuthenticationException();
         }
      }

      public boolean isComplete() {
         return this.complete;
      }
   }

   protected class UnifiedSaslNegotiator implements SaslNegotiator {
      private DseAuthenticator.AuthenticationState authenticationState = DseAuthenticator.this.new AuthenticationState(null);
      private SaslNegotiator selectedNegotiator;

      public UnifiedSaslNegotiator(InetAddress clientAddress) {
         this.authenticationState.clientAddress = clientAddress;
      }

      public byte[] evaluateResponse(byte[] bytes) throws DseAuthenticationException {
         try {
            if(this.selectedNegotiator == null) {
               boolean matchedScheme = false;
               AuthenticationScheme[] var3 = AuthenticationScheme.values();
               int var4 = var3.length;

               for(int var5 = 0; var5 < var4; ++var5) {
                  AuthenticationScheme scheme = var3[var5];
                  if(Arrays.equals(scheme.saslMechanism.mechanism_bytes, bytes)) {
                     matchedScheme = true;
                     if(DseAuthenticator.this.usingScheme(scheme)) {
                        this.authenticationState.selectedScheme = scheme;
                        this.selectedNegotiator = DseAuthenticator.this.getSaslNegotiatorForScheme(this.authenticationState);
                        return scheme.saslMechanism.response;
                     }
                  }
               }

               if(matchedScheme) {
                  throw new DseAuthenticationException();
               }

               this.authenticationState.selectedScheme = DseAuthenticator.this.defaultScheme;
               this.authenticationState.legacy = true;
               this.selectedNegotiator = DseAuthenticator.this.getSaslNegotiatorForScheme(this.authenticationState);
            }

            return this.selectedNegotiator.evaluateResponse(bytes);
         } catch (DseAuthenticationException var7) {
            throw this.maybeRecordFailedAuthentication(this.authenticationState.clientAddress.toString(), var7);
         }
      }

      public boolean isComplete() {
         return this.selectedNegotiator != null && this.selectedNegotiator.isComplete();
      }

      public AuthenticatedUser getAuthenticatedUser() throws DseAuthenticationException {
         try {
            if(this.selectedNegotiator == null) {
               throw new DseAuthenticationException();
            } else {
               AuthenticatedUser user = this.selectedNegotiator.getAuthenticatedUser();
               DseAuthenticator.this.checkPermissions(user, this.authenticationState.actualScheme);
               this.maybeLogAuthentication(user);
               return user;
            }
         } catch (DseAuthenticationException var2) {
            throw this.maybeRecordFailedAuthentication(this.authenticationState.clientAddress.toString(), var2);
         }
      }

      private void maybeLogAuthentication(AuthenticatedUser user) {
         if(DseAuthenticator.this.auditLogger.isEnabled()) {
            try {
               String authenticatedUser = user.getAuthenticatedName();
               String operation = ((Boolean)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().canLogin(user.getLoginRole()))).booleanValue()?"Successful login for user - " + authenticatedUser:"User - " + authenticatedUser + " - does not exist or doesn't have the right to login";
               UserRolesAndPermissions userRolesAndPermissions = (UserRolesAndPermissions)DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(user).blockingGet();
               AuditableEvent event = new AuditableEvent(userRolesAndPermissions, CoreAuditableEventType.LOGIN, this.authenticationState.clientAddress.toString(), operation);
               AuditLoggerUtils.logEventBlocking(DseAuthenticator.this.auditLogger, event);
            } catch (Exception var6) {
               DseAuthenticator.logger.debug("Failed to record the event:", var6);
            }
         }

      }

      private DseAuthenticationException maybeRecordFailedAuthentication(String clientAddress, DseAuthenticationException authException) {
         DseAuthenticator.logger.debug("Failed to authenticate: ", authException);
         if(DseAuthenticator.this.auditLogger.isEnabled()) {
            try {
               UserRolesAndPermissions userRolesAndPermissions = UserRolesAndPermissions.newNormalUserRolesAndPermissions(authException.authorizationUser, authException.authenticationUser, Collections.emptySet(), Collections.emptyMap());
               AuditableEvent event = new AuditableEvent(userRolesAndPermissions, CoreAuditableEventType.LOGIN_ERROR, clientAddress, "Failed login attempt for user - " + authException.authenticationUser);
               AuditLoggerUtils.logEventBlocking(DseAuthenticator.this.auditLogger, event);
            } catch (Exception var5) {
               DseAuthenticator.logger.debug("Failed to record the event:", var5);
            }
         }

         return authException;
      }
   }

   protected abstract class DseExternalSaslNegotiator extends DseAuthenticator.DseSaslNegotiator {
      protected boolean authenticationError = false;

      protected DseExternalSaslNegotiator() {
         super();
      }

      protected abstract String getAuthenticationUser();

      protected abstract String getAuthorizationUser();

      protected abstract boolean passwordSupplied();

      protected boolean authorize(String authenticationUser, String authorizationUser) {
         return !StringUtils.isEmpty(authorizationUser) && !Objects.equals(authenticationUser, authorizationUser)?(((Boolean)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().hasSuperUserStatus(RoleResource.role(authorizationUser)))).booleanValue()?false:((UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(new AuthenticatedUser(authenticationUser)))).hasRolePermission(RoleResource.role(authorizationUser), ProxyPermission.LOGIN)):true;
      }

      public AuthenticatedUser getAuthenticatedUser() {
         String authenticationUser = this.getAuthenticationUser();
         String authorizationUser = this.getAuthorizationUser();
         if(DseAuthenticator.this.forceAnonymous(authenticationUser, this.passwordSupplied())) {
            return AuthenticatedUser.ANONYMOUS_USER;
         } else if(this.authenticationError) {
            if(DseAuthenticator.this.getTransitionalMode().failedAuthenticationMapsToAnonymous()) {
               return AuthenticatedUser.ANONYMOUS_USER;
            } else {
               throw new DseAuthenticationException(StringUtils.isEmpty(authorizationUser)?authenticationUser:authorizationUser, authenticationUser);
            }
         } else {
            return (AuthenticatedUser)(!StringUtils.isEmpty(authorizationUser) && !Objects.equals(authenticationUser, authorizationUser)?new ProxyAuthenticatedUser(authenticationUser, authorizationUser):new AuthenticatedUser(authenticationUser));
         }
      }
   }

   protected abstract class DseSaslNegotiator implements SaslNegotiator {
      protected DseSaslNegotiator() {
      }

      protected boolean authorize(String authenticationUser, String authorizationUser) {
         return authenticationUser == null || Objects.equals(authenticationUser, authorizationUser);
      }

      protected class DseSaslCallbackHandler implements CallbackHandler {
         protected DseSaslCallbackHandler() {
         }

         protected void handleNamePasswordCallback(Optional<NameCallback> nameCallback, Optional<PasswordCallback> passwordCallback) throws IOException {
         }

         protected void handleAuthorizeCallback(Optional<AuthorizeCallback> authorizeCallback) throws IOException {
            authorizeCallback.ifPresent((authorizeCb) -> {
               String authenticationUser = authorizeCb.getAuthenticationID();
               String authorizationUser = authorizeCb.getAuthorizationID();
               authorizeCb.setAuthorized(DseSaslNegotiator.this.authorize(authenticationUser, authorizationUser));
               if(authorizeCb.isAuthorized()) {
                  authorizeCb.setAuthorizedID(authorizationUser);
                  DseAuthenticator.logger.debug("Allowing login for {} as {} via {}", new Object[]{authenticationUser, authorizationUser, this.getClass().getSimpleName()});
               }

            });
         }

         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            this.handleNamePasswordCallback(DseUtil.firstInstanceOf(callbacks, NameCallback.class), DseUtil.firstInstanceOf(callbacks, PasswordCallback.class));
            this.handleAuthorizeCallback(DseUtil.firstInstanceOf(callbacks, AuthorizeCallback.class));
         }
      }
   }
}
