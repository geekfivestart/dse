package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.util.MapBuilder;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.TrustManagerFactory;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils.WouldBlockException;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapAuthenticationException;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.name.Rdn;
import org.apache.directory.ldap.client.api.DefaultPoolableLdapConnectionFactory;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.apache.directory.ldap.client.api.ValidatingPoolableLdapConnectionFactory;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LdapUtils implements LdapUtilsMXBean {
   private static final Logger logger = LoggerFactory.getLogger(LdapUtils.class);
   private static final Cache<String, Dn> searchCache = initSearchCache();
   private static final Cache<String, LdapUtils.CachedCredentials> credentialsCache = initCredentialsCache();
   private static final int GENSALT_LOG2_ROUNDS = 10;
   public static LdapUtils instance = new LdapUtils();
   private static final int MAX_RETRIES = Integer.parseInt(System.getProperty("dse.ldap.max_retries", "3"));
   private boolean anonymousSearch;
   private String userSearchBase;
   private String userSearchFilter;
   private String userMemberOfAttribute;
   private LdapUtils.GroupSearchType groupSearchType;
   private String groupSearchBase;
   private String groupSearchFilter;
   private String groupNameAttribute;
   private LdapConnectionPool connectionPool;
   private long connectionTimeout;

   private LdapUtils() {
   }

   public void validateAuthenticationConfiguration() throws ConfigurationException {
      this.validateConfiguration(this::validateAuthenticationConfiguration);
   }

   public void validateGroupConfiguration() throws ConfigurationException {
      this.validateConfiguration((errors) -> {
         this.validateAuthenticationConfiguration(errors);
         this.validateGroupConfiguration(errors);
      });
   }

   private void validateConfiguration(Consumer<List> validator) {
      List<String> errors = new ArrayList();
      validator.accept(errors);
      if(!errors.isEmpty()) {
         throw new ConfigurationException((String)errors.stream().collect(Collectors.joining(", ")));
      }
   }

   private void validateAuthenticationConfiguration(List<String> errors) {
      if(Strings.isNullOrEmpty(DseConfig.getLdapServerHost())) {
         errors.add("ldap_options.server_host must be set to use LDAP functionality");
      }

      if(Strings.isNullOrEmpty(DseConfig.getLdapUserSearchBase())) {
         errors.add("ldap_options.user_search_base must be set to a valid DN");
      }

      if(Strings.isNullOrEmpty(DseConfig.getLdapUserSearchFilter())) {
         errors.add("ldap_options.user_search_filter must be set to a valid LDAP search filter");
      }

   }

   private void validateGroupConfiguration(List<String> errors) {
      if(LdapUtils.GroupSearchType.valueOf(DseConfig.getLdapGroupSearchType().toUpperCase()) == LdapUtils.GroupSearchType.DIRECTORY_SEARCH) {
         if(Strings.isNullOrEmpty(DseConfig.getLdapGroupSearchBase())) {
            errors.add("ldap_options.group_search_base must be set to do group directory searches");
         }

         if(Strings.isNullOrEmpty(DseConfig.getLdapGroupSearchFilter())) {
            errors.add("ldap_options.group_search_filter must be set to do group directory searches");
         }
      } else if(Strings.isNullOrEmpty(DseConfig.getLdapUserMemberOfAttribute())) {
         errors.add("ldap_options.user_memberof_attribute must be set to do group memberof searches");
      }

      if(Strings.isNullOrEmpty(DseConfig.getLdapGroupNameAttribute())) {
         errors.add("ldap_options.group_name_attribute must be set to get group information for a user");
      }

   }

   public void setup() {
      if(this.connectionPool == null) {
         this.userSearchBase = DseConfig.getLdapUserSearchBase();
         this.userSearchFilter = DseConfig.getLdapUserSearchFilter();
         this.userMemberOfAttribute = DseConfig.getLdapUserMemberOfAttribute();
         this.groupSearchType = LdapUtils.GroupSearchType.valueOf(DseConfig.getLdapGroupSearchType().toUpperCase());
         this.groupSearchBase = DseConfig.getLdapGroupSearchBase();
         this.groupSearchFilter = DseConfig.getLdapGroupSearchFilter();
         this.groupNameAttribute = DseConfig.getLdapGroupNameAttribute();
         LdapConnectionConfig config = new LdapConnectionConfig();
         config.setLdapHost(DseConfig.getLdapServerHost());
         config.setLdapPort(DseConfig.getLdapServerPort());
         config.setUseSsl(DseConfig.isLdapUseSSL());
         config.setUseTls(DseConfig.isLdapUseTls());
         if((DseConfig.isLdapUseSSL() || DseConfig.isLdapUseTls()) && StringUtils.isNotEmpty(DseConfig.getLdapTruststorePath())) {
            try {
               FileInputStream fis = new FileInputStream(DseConfig.getLdapTruststorePath());
               Throwable var3 = null;

               try {
                  char[] password = StringUtils.isEmpty(DseConfig.getLdapTruststorePassword())?null:DseConfig.getLdapTruststorePassword().toCharArray();
                  KeyStore ks = KeyStore.getInstance(DseConfig.getLdapTruststoreType());
                  ks.load(fis, password);
                  TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                  tmf.init(ks);
                  config.setTrustManagers(tmf.getTrustManagers());
               } catch (Throwable var15) {
                  var3 = var15;
                  throw var15;
               } finally {
                  if(fis != null) {
                     if(var3 != null) {
                        try {
                           fis.close();
                        } catch (Throwable var14) {
                           var3.addSuppressed(var14);
                        }
                     } else {
                        fis.close();
                     }
                  }

               }
            } catch (Exception var17) {
               throw new AssertionError("Failed to initialize trust manager in LdapConnectionConfig", var17);
            }
         }

         this.anonymousSearch = StringUtils.isEmpty(DseConfig.getLdapSearchDn());
         if(!this.anonymousSearch) {
            config.setName(DseConfig.getLdapSearchDn());
            config.setCredentials(DseConfig.getLdapSearchPassword());
         }

         String factoryType = System.getProperty("dse.ldap.connection.factory", "default");
         PoolableObjectFactory<LdapConnection> factory = factoryType.equals("validating")?new ValidatingPoolableLdapConnectionFactory(config):new DefaultPoolableLdapConnectionFactory(config);
         this.connectionPool = new LdapConnectionPool((PoolableObjectFactory)factory);
         this.connectionPool.setMaxActive(DseConfig.getLdapConnectionPoolMaxActive());
         this.connectionPool.setMaxIdle(DseConfig.getLdapConnectionPoolMaxIdle());
         this.connectionTimeout = Long.getLong("dse.ldap.connection.timeout.ms", 30000L).longValue();
         this.connectionPool.setMinIdle(Integer.getInteger("dse.ldap.pool.min.idle", 0).intValue());
         String exhaustedAction = System.getProperty("dse.ldap.pool.exhausted.action", "block");
         byte exhaustedValue = exhaustedAction.equals("fail")?0:(exhaustedAction.equals("grow")?2:1);
         this.connectionPool.setWhenExhaustedAction((byte)exhaustedValue);
         this.connectionPool.setMaxWait(Long.getLong("dse.ldap.pool.max.wait", 30000L).longValue());
         this.connectionPool.setTestOnBorrow(Boolean.parseBoolean(System.getProperty("dse.ldap.pool.test.borrow", "false")));
         this.connectionPool.setTestOnReturn(Boolean.parseBoolean(System.getProperty("dse.ldap.pool.test.return", "true")));
         this.connectionPool.setTestWhileIdle(Boolean.parseBoolean(System.getProperty("dse.ldap.pool.test.idle", "false")));
         this.connectionPool.setTimeBetweenEvictionRunsMillis(Long.getLong("dse.ldap.pool.time.between.evictions", -1L).longValue());
         this.connectionPool.setNumTestsPerEvictionRun(Integer.getInteger("dse.ldap.pool.num.tests.per.eviction", 3).intValue());
         this.connectionPool.setMinEvictableIdleTimeMillis(Long.getLong("dse.ldap.pool.min.evictable.idle.time.ms", 1800000L).longValue());
         this.connectionPool.setSoftMinEvictableIdleTimeMillis(Long.getLong("dse.ldap.pool.soft.min.evictable.idle.time.ms", -1L).longValue());
         if(logger.isTraceEnabled()) {
            StringBuilder builder = new StringBuilder("[ldap-configuration] Ldap configured using the following settings\n");
            builder.append(String.format("Host: %s\n", new Object[]{config.getLdapHost()}));
            builder.append(String.format("Port: %s\n", new Object[]{Integer.valueOf(config.getLdapPort())}));
            builder.append(String.format("Use SSL: %s\n", new Object[]{Boolean.valueOf(config.isUseSsl())}));
            builder.append(String.format("Use TLS: %s\n", new Object[]{Boolean.valueOf(config.isUseTls())}));
            builder.append(String.format("Anonymous bind: %s\n", new Object[]{Boolean.valueOf(this.anonymousSearch)}));
            builder.append(String.format("User search base: %s\n", new Object[]{this.userSearchBase}));
            builder.append(String.format("User search filter: %s\n", new Object[]{this.userSearchFilter}));
            builder.append(String.format("Group search type: %s\n", new Object[]{this.groupSearchType}));
            builder.append(String.format("Group search base: %s\n", new Object[]{this.groupSearchBase}));
            builder.append(String.format("Group search filter: %s\n", new Object[]{this.groupSearchFilter}));
            builder.append(String.format("Group name attribute: %s\n", new Object[]{this.groupNameAttribute}));
            builder.append(String.format("User memberof attribute: %s\n", new Object[]{this.userMemberOfAttribute}));
            builder.append(String.format("Poolable connection factory: %s\n", new Object[]{factory.getClass().getCanonicalName()}));
            builder.append(String.format("Max active: %s\n", new Object[]{Integer.valueOf(this.connectionPool.getMaxActive())}));
            builder.append(String.format("Max idle: %s\n", new Object[]{Integer.valueOf(this.connectionPool.getMaxIdle())}));
            builder.append(String.format("Min idle: %s\n", new Object[]{Integer.valueOf(this.connectionPool.getMaxIdle())}));
            builder.append(String.format("Exhausted action: %s\n", new Object[]{Byte.valueOf(this.connectionPool.getWhenExhaustedAction())}));
            builder.append(String.format("Max wait: %s\n", new Object[]{Long.valueOf(this.connectionPool.getMaxWait())}));
            builder.append(String.format("Test on borrow: %s\n", new Object[]{Boolean.valueOf(this.connectionPool.getTestOnBorrow())}));
            builder.append(String.format("Test on return: %s\n", new Object[]{Boolean.valueOf(this.connectionPool.getTestOnReturn())}));
            builder.append(String.format("Test while idle: %s\n", new Object[]{Boolean.valueOf(this.connectionPool.getTestWhileIdle())}));
            builder.append(String.format("Time between eviction runs: %s\n", new Object[]{Long.valueOf(this.connectionPool.getTimeBetweenEvictionRunsMillis())}));
            builder.append(String.format("Number of tests per eviction run: %s\n", new Object[]{Integer.valueOf(this.connectionPool.getNumTestsPerEvictionRun())}));
            builder.append(String.format("Minimum evictable idle time: %s\n", new Object[]{Long.valueOf(this.connectionPool.getMinEvictableIdleTimeMillis())}));
            builder.append(String.format("Soft minimum evictable idle time: %s\n", new Object[]{Long.valueOf(this.connectionPool.getSoftMinEvictableIdleTimeMillis())}));
            logger.trace(builder.toString());
         }
      }

   }

   public void setupAuthentication() {
      this.setup();
      JMX.registerMBean(this, JMX.Type.CORE, MapBuilder.immutable().withKeys(new String[]{"name"}).withValues(new String[]{"LdapAuthenticator"}).build());
   }

   public AuthenticatedUser authenticate(Credentials credentials) throws AuthenticationException {
      if(!StringUtils.isEmpty(credentials.authenticationUser) && !StringUtils.isEmpty(credentials.password)) {
         if(credentialsCache == null) {
            return this.doAuthentication(credentials.authenticationUser, credentials.password, MAX_RETRIES);
         } else {
            LdapUtils.CachedCredentials cachedCredentials = (LdapUtils.CachedCredentials)credentialsCache.getIfPresent(credentials.authenticationUser);
            if(cachedCredentials == null || !BCrypt.checkpw(credentials.password, cachedCredentials.hashedPassword)) {
               credentialsCache.invalidate(credentials.authenticationUser);
               AuthenticatedUser authenticatedUser = this.doAuthentication(credentials.authenticationUser, credentials.password, MAX_RETRIES);
               cachedCredentials = new LdapUtils.CachedCredentials(BCrypt.hashpw(credentials.password, BCrypt.gensalt(10)), authenticatedUser);
               credentialsCache.put(credentials.authenticationUser, cachedCredentials);
            }

            return cachedCredentials.user;
         }
      } else {
         logger.error("Either the username or password were null or zero length");
         throw new DseAuthenticationException(credentials.authorizationUser, credentials.authenticationUser);
      }
   }

   private AuthenticatedUser doAuthentication(String username, String password, int maxRetries) {
      AuthenticationException t = null;
      int i = 0;

      while(i <= maxRetries) {
         try {
            if(i > 0 && logger.isTraceEnabled()) {
               logger.trace("[ldap-authenticate] attempting authentication retry: " + i);
            }

            return this.doAuthentication(username, password);
         } catch (AuthenticationException var7) {
            if(t == null) {
               t = var7;
            } else {
               t.addSuppressed(var7);
            }

            Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
            ++i;
         }
      }

      assert t != null;

      throw t;
   }

   private AuthenticatedUser doAuthentication(String username, String password) throws AuthenticationException {
      Dn userDn = searchCache == null?null:(Dn)searchCache.getIfPresent(username);
      boolean userDnInCache = true;
      if(userDn == null) {
         if(logger.isTraceEnabled()) {
            logger.trace("[ldap-authenticate] username: " + username + " not found in cache");
         }

         userDn = this.fetchUserDn(username);
         if(userDn == null) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-authenticate] ERROR - could not find userDN for username: " + username);
            }

            throw new DseAuthenticationException(username);
         }

         userDnInCache = false;
      }

      try {
         if(this.userCanBind(userDn, password)) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-authenticate] SUCCESS - username: " + username + ", userDN: " + userDn);
            }

            if(!userDnInCache && searchCache != null) {
               searchCache.put(username, userDn);
            }

            return new AuthenticatedUser(username);
         }

         if(logger.isTraceEnabled()) {
            logger.trace("[ldap-authenticate] FAILURE - bind failed for username: " + username + ", userDN: " + userDn);
         }

         if(userDnInCache) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-authenticate] - userDN was in cache so looking for new DN");
            }

            Dn newDn = this.fetchUserDn(username);
            if(newDn == null) {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-authenticate] ERROR - could not find userDN for username: " + username);
               }

               throw new DseAuthenticationException(username);
            }

            if(!newDn.equals(userDn)) {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-authenticate] new userDN: " + newDn + " does not match cached userDN: " + userDn + " so attempting to bind again");
               }

               if(this.userCanBind(newDn, password)) {
                  if(logger.isTraceEnabled()) {
                     logger.trace("[ldap-authenticate] SUCCESS - username: " + username + ", userDN: " + userDn);
                  }

                  if(searchCache != null) {
                     searchCache.put(username, newDn);
                  }

                  return new AuthenticatedUser(username);
               }
            }
         }
      } catch (RuntimeException var7) {
         if(logger.isTraceEnabled()) {
            logger.trace("[ldap-authenticate] ERROR - unexpected error for username: " + username, var7);
         }

         if(searchCache != null) {
            searchCache.invalidate(username);
         }

         AuthenticationException exception = new AuthenticationException(username);
         exception.initCause(var7);
         throw exception;
      }

      if(searchCache != null) {
         searchCache.invalidate(username);
      }

      if(logger.isTraceEnabled()) {
         logger.trace("[ldap-authenticate] ERROR - username: " + username + " failed to authenticate");
      }

      throw new DseAuthenticationException(username);
   }

   private boolean userCanBind(Dn userDn, String password) {
      if(TPC.isTPCThread()) {
         throw new WouldBlockException("Binding LDAP user would block TPC thread");
      } else {
         LdapConnection connection = null;
         LdapException t = null;

         try {
            connection = this.connectionPool.getConnection();
            connection.setTimeOut(this.connectionTimeout);
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-bind] userDN: " + userDn + " connection: " + connection);
            }

            connection.bind(userDn, password);
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-bind] SUCCESS - bind succcessful for userDN: " + userDn);
            }

            boolean var5 = true;
            return var5;
         } catch (LdapAuthenticationException var10) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-bind] ERROR - bind failed for userDN: " + userDn, var10);
            }
         } catch (RuntimeException | LdapException var11) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-bind] ERROR - bind failed for userDN: " + userDn, var11);
            }

            t = var11;
         } finally {
            this.freeConnection(connection, t);
         }

         return false;
      }
   }

   private Dn fetchUserDn(String username) throws AuthenticationException {
      if(TPC.isTPCThread()) {
         throw new WouldBlockException("Fetching user from LDAP would block TPC thread");
      } else {
         LdapConnection connection = null;
         DseAuthenticationException t = null;

         label146: {
            Dn var6;
            try {
               connection = this.connectionPool.getConnection();
               connection.setTimeOut(this.connectionTimeout);
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user] username: " + username + " connection: " + connection);
               }

               if(this.anonymousSearch) {
                  if(logger.isTraceEnabled()) {
                     logger.trace("[ldap-fetch-user] anonymous bind to connection");
                  }

                  connection.anonymousBind();
               } else {
                  if(logger.isTraceEnabled()) {
                     logger.trace("[ldap-fetch-user] bind to connection");
                  }

                  connection.bind();
               }

               String userSearchFilterStr = MessageFormat.format(this.userSearchFilter, new Object[]{username});
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user] user_search_base: " + this.userSearchBase + ", user_search_filter: " + userSearchFilterStr);
               }

               EntryCursor cursor = connection.search(this.userSearchBase, userSearchFilterStr, SearchScope.SUBTREE, new String[0]);
               if(!cursor.next()) {
                  break label146;
               }

               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user] found entry for username: " + username);
               }

               var6 = ((Entry)cursor.get()).getDn();
            } catch (LdapAuthenticationException var11) {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user] ERROR - failed to fetch username: " + username, var11);
               }
               break label146;
            } catch (LdapException | RuntimeException | CursorException var12) {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user] ERROR - failed to fetch username: " + username, var12);
               }

               AuthenticationException exception = new DseAuthenticationException("LDAP authentication failure");
               exception.initCause(var12);
               t = exception;
               throw exception;
            } finally {
               this.freeConnection(connection, t);
            }

            return var6;
         }

         if(logger.isTraceEnabled()) {
            logger.trace("[ldap-fetch-user] ERROR - failed to fetch username: " + username);
         }

         throw new DseAuthenticationException("Username and/or password are incorrect");
      }
   }

   public List<String> fetchUserGroups(String username) {
      if(TPC.isTPCThread()) {
         throw new WouldBlockException("Fetching groups from LDAP would block TPC thread");
      } else {
         List<String> groups = new ArrayList();
         LdapConnection connection = null;
         CursorException t = null;

         try {
            connection = this.connectionPool.getConnection();
            connection.setTimeOut(this.connectionTimeout);
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-fetch-user-groups] username: " + username + " connection: " + connection);
            }

            if(this.anonymousSearch) {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] anonymous bind to connection");
               }

               connection.anonymousBind();
            } else {
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] bind to connection");
               }

               connection.bind();
            }

            String userSearchFilterStr;
            EntryCursor cursor;
            String groupName;
            if(this.groupSearchType == LdapUtils.GroupSearchType.DIRECTORY_SEARCH) {
               userSearchFilterStr = MessageFormat.format(this.userSearchFilter, new Object[]{username});
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] performing directory search for groups - fetching username: " + username + " with user_search_base: " + this.userSearchBase + ", user_search_filter: " + userSearchFilterStr);
               }

               cursor = connection.search(this.userSearchBase, userSearchFilterStr, SearchScope.SUBTREE, new String[0]);
               if(cursor.next()) {
                  Dn userDn = ((Entry)cursor.get()).getDn();
                  String groupSearchFilterStr = MessageFormat.format(this.groupSearchFilter, new Object[]{userDn.toString()});
                  if(logger.isTraceEnabled()) {
                     logger.trace("[ldap-fetch-user-groups] looking for groups for userDN: " + userDn + " with group_search_base: " + this.groupSearchBase + ", group_search_filter: " + groupSearchFilterStr);
                  }

                  cursor = connection.search(this.groupSearchBase, MessageFormat.format(this.groupSearchFilter, new Object[]{userDn.toString()}), SearchScope.SUBTREE, new String[0]);

                  while(cursor.next()) {
                     Entry entry = (Entry)cursor.get();
                     if(entry.containsAttribute(new String[]{this.groupNameAttribute})) {
                        groupName = entry.get(this.groupNameAttribute).getString();
                        if(logger.isTraceEnabled()) {
                           logger.trace("[ldap-fetch-user-groups] found group: " + groupName + " for username: " + username);
                        }

                        groups.add(groupName);
                     } else if(logger.isTraceEnabled()) {
                        logger.trace("[ldap-fetch-user-groups] ERROR - group entry: " + entry.getDn() + " does not contain group_name_attribute: " + this.groupNameAttribute);
                     }
                  }
               } else if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] FAILURE - could not find user entry for username: " + username);
               }
            } else {
               userSearchFilterStr = MessageFormat.format(this.userSearchFilter, new Object[]{username});
               if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] performing member-of lookup for groups - fetching username: " + username + " with user_search_base: " + this.userSearchBase + ", user_search_filter: " + userSearchFilterStr);
               }

               cursor = connection.search(this.userSearchBase, userSearchFilterStr, SearchScope.SUBTREE, new String[]{this.userMemberOfAttribute});
               if(cursor.next()) {
                  Entry entry = (Entry)cursor.get();
                  if(entry.containsAttribute(new String[]{this.userMemberOfAttribute})) {
                     Iterator var19 = entry.get(this.userMemberOfAttribute).iterator();

                     while(var19.hasNext()) {
                        Value value = (Value)var19.next();
                        groupName = null;
                        Iterator var11 = (new Dn(new String[]{value.toString()})).iterator();

                        while(var11.hasNext()) {
                           Rdn rdn = (Rdn)var11.next();
                           if(rdn.getType().equalsIgnoreCase(this.groupNameAttribute)) {
                              groupName = rdn.getValue();
                           }
                        }

                        if(!Strings.isNullOrEmpty(groupName)) {
                           if(logger.isTraceEnabled()) {
                              logger.trace("[ldap-fetch-user-groups] found group: " + groupName + " for username: " + username);
                           }

                           groups.add(groupName);
                        }
                     }
                  }
               } else if(logger.isTraceEnabled()) {
                  logger.trace("[ldap-fetch-user-groups] FAILURE - could not find user entry for username: " + username + " or user entry did not have any memberOf attribute entries");
               }
            }
         } catch (LdapException | RuntimeException | CursorException var16) {
            if(logger.isTraceEnabled()) {
               logger.trace("[ldap-fetch-user-groups] ERROR - failed to fetch groups for username: " + username, var16);
            }

            t = var16;
         } finally {
            this.freeConnection(connection, t);
         }

         return groups;
      }
   }

   public long getSearchCacheSize() {
      return searchCache == null?-1L:searchCache.size();
   }

   public long getCredentialsCacheSize() {
      return credentialsCache == null?-1L:credentialsCache.size();
   }

   public long getConnectionPoolActive() {
      return (long)this.connectionPool.getNumActive();
   }

   public long getConnectionPoolIdle() {
      return (long)this.connectionPool.getNumIdle();
   }

   public void invalidateSearchCacheAll() {
      if(searchCache != null) {
         searchCache.invalidateAll();
      }

   }

   public void invalidateSearchCache(String username) {
      if(searchCache != null && StringUtils.isNotEmpty(username)) {
         searchCache.invalidate(username);
      }

   }

   public void invalidateCredentialsCacheAll() {
      if(credentialsCache != null) {
         credentialsCache.invalidateAll();
      }

   }

   public void invalidateCredentialsCache(String username) {
      if(credentialsCache != null && StringUtils.isNotEmpty(username)) {
         credentialsCache.invalidate(username);
      }

   }

   private void freeConnection(LdapConnection connection, @Nullable Throwable t) {
      if(connection != null) {
         try {
            if(t != null) {
               this.connectionPool.invalidateObject(connection);
            } else {
               connection.setTimeOut(30000L);
               this.connectionPool.releaseConnection(connection);
            }
         } catch (Exception var4) {
            logger.debug("Failed to release an LDAP connection back to the connection pool", var4);
            if(t != null) {
               t.addSuppressed(var4);
            }
         }
      }

   }

   private static Cache<String, Dn> initSearchCache() {
      int validityPeriod = DseConfig.getLdapSearchValidity();
      return validityPeriod <= 0?null:CacheBuilder.newBuilder().expireAfterWrite((long)validityPeriod, TimeUnit.SECONDS).build();
   }

   private static Cache<String, LdapUtils.CachedCredentials> initCredentialsCache() {
      int validityPeriod = DseConfig.getLdapCredentialsValidity();
      return validityPeriod <= 0?null:CacheBuilder.newBuilder().expireAfterWrite((long)validityPeriod, TimeUnit.MILLISECONDS).build();
   }

   private class CachedCredentials {
      final String hashedPassword;
      final AuthenticatedUser user;

      CachedCredentials(String hashedPassword, AuthenticatedUser user) {
         this.hashedPassword = hashedPassword;
         this.user = user;
      }
   }

   private static enum GroupSearchType {
      DIRECTORY_SEARCH,
      MEMBEROF_SEARCH;

      private GroupSearchType() {
      }
   }
}
