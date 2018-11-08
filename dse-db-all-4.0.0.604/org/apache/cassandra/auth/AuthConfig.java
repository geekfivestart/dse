package org.apache.cassandra.auth;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AuthConfig {
   private static final Logger logger = LoggerFactory.getLogger(AuthConfig.class);
   private static boolean initialized;

   public AuthConfig() {
   }

   public static void applyAuth() {
      if(!initialized) {
         initialized = true;
         Config conf = DatabaseDescriptor.getRawConfig();
         IAuthenticator authenticator = new AllowAllAuthenticator();
         if(conf.authenticator != null) {
            authenticator = FBUtilities.newAuthenticator(conf.authenticator);
         }

         DatabaseDescriptor.setAuthenticator((IAuthenticator)authenticator);
         IAuthorizer authorizer = new AllowAllAuthorizer();
         if(conf.authorizer != null) {
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);
         }

         Object roleManager;
         if(conf.role_manager != null) {
            roleManager = FBUtilities.newRoleManager(conf.role_manager);
         } else {
            roleManager = new CassandraRoleManager();
         }

         if(authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager)) {
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);
         } else {
            if(conf.internode_authenticator != null) {
               DatabaseDescriptor.setInternodeAuthenticator((IInternodeAuthenticator)FBUtilities.construct(conf.internode_authenticator, "internode_authenticator"));
            }

            ((IAuthenticator)authenticator).validateConfiguration();
            ((IAuthorizer)authorizer).validateConfiguration();
            ((IRoleManager)roleManager).validateConfiguration();
            DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();
            if(!((IAuthenticator)authenticator).requireAuthentication() && ((IAuthorizer)authorizer).requireAuthorization()) {
               throw new ConfigurationException(conf.authenticator + " does not currently require authentication, so it can't be used with " + conf.authorizer + " which does currently require authorization.  You need to either choose new classes or update their configurations so they are compatible.");
            } else {
               DatabaseDescriptor.setAuthManager(new AuthManager((IRoleManager)roleManager, (IAuthorizer)authorizer));
               IAuthenticator.TransitionalMode authenticatorTransitionalMode = DatabaseDescriptor.getAuthenticator().getTransitionalMode();
               IAuthorizer.TransitionalMode authorizerTransitionalMode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();
               if(DatabaseDescriptor.isSystemKeyspaceFilteringEnabled()) {
                  if(!DatabaseDescriptor.getAuthorizer().requireAuthorization()) {
                     logger.error("In order to use system keyspace filtering, an authorizer that requires authorization must be configured.");
                     throw new ConfigurationException("In order to use system keyspace filtering, an authorizer that requires authorization must be configured.");
                  }

                  if(!DatabaseDescriptor.getAuthenticator().requireAuthentication()) {
                     logger.error("In order to use system keyspace filtering, an authenticator that requires authentication must be configured.");
                     throw new ConfigurationException("In order to use system keyspace filtering, an authenticator that requires authentication must be configured.");
                  }

                  if(authorizerTransitionalMode != IAuthorizer.TransitionalMode.DISABLED || authenticatorTransitionalMode != IAuthenticator.TransitionalMode.DISABLED) {
                     logger.warn("It is not recommended to enable system-keyspace-filtering in combination with transitional authentication or authorization.");
                  }

                  logger.info("System keyspaces filtering enabled.");
               } else {
                  logger.info("System keyspaces filtering not enabled.");
               }

               if(authenticatorTransitionalMode != IAuthenticator.TransitionalMode.DISABLED && authorizerTransitionalMode == IAuthorizer.TransitionalMode.DISABLED) {
                  logger.warn("Authorizer {} transitional-mode set to ‘disabled’ in combination with Authenticator {}transitional-mode not set to ‘disabled’.  This is probably not intended. Consider configuring authorizer transitional mode ‘normal’ or ’strict, as ‘disabled’ will reject all privileges for anonymous and all users mapped to anonymous by the Authenticator. Refer to the documentation about transitional authentication and authorization.", DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName(), DatabaseDescriptor.getAuthenticator().implementation().getClass().getSimpleName());
               }

            }
         }
      }
   }
}
