package com.datastax.bdp.config;

import com.datastax.bdp.cassandra.auth.DseAuthenticator;
import com.datastax.bdp.cassandra.auth.DseAuthorizer;
import com.datastax.bdp.cassandra.crypto.ConfigDecryptor;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHosts;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfig;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfigurations;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageYamlConfig;
import com.datastax.bdp.db.audit.AuditLoggingOptions;
import com.datastax.bdp.db.audit.CassandraAuditWriter;
import com.datastax.bdp.db.audit.CassandraAuditWriterOptions;
import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.server.ServerId;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.util.DseUtil;
import com.datastax.bdp.util.SSLUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseConfig extends DseConfigYamlLoader {
   public static int MIN_REFRESH_RATE_MS = 10;
   public static final int DEFAULT_HEALTH_REFRESH_RATE_MILLIS = 60000;
   public static final long DEFAULT_RAMP_UP_PERIOD_SECONDS;
   public static final int DROPPED_MUTATION_WINDOW_MINUTES = 30;
   private static final String defaultQop = "auth";
   private static final Logger logger;
   private static final DseConfig.EndpointSnitchParamResolver delegatedSnitch;
   private static final ConfigUtil.StringParamResolver roleManagement;
   private static final ConfigUtil.BooleanParamResolver authenticationEnabled;
   private static final ConfigUtil.StringParamResolver defaultAuthenticationScheme;
   private static final ConfigUtil.SetParamResolver otherAuthenticationSchemes;
   private static final ConfigUtil.BooleanParamResolver authenticationSchemePermissions;
   private static final ConfigUtil.BooleanParamResolver allowDigestWithKerberos;
   private static final ConfigUtil.StringParamResolver plainTextWithoutSsl;
   private static final ConfigUtil.StringParamResolver authenticationTransitionalMode;
   private static final ConfigUtil.BooleanParamResolver authorizationEnabled;
   private static final ConfigUtil.StringParamResolver authorizationTransitionalMode;
   private static final ConfigUtil.BooleanParamResolver rowLevelAuthorizationEnabled;
   private static final ConfigUtil.StringParamResolver dseServiceKeyTabFile;
   private static final ConfigUtil.StringParamResolver saslQop;
   private static final DseConfig.ServicePrincipalParamResolver dseServicePrincipal;
   private static final DseConfig.ServicePrincipalParamResolver httpKrbprincipal;
   private static final ConfigUtil.StringParamResolver ldapServerHost;
   private static final ConfigUtil.IntParamResolver ldapServerPort;
   private static final ConfigUtil.BooleanParamResolver ldapUseSSL;
   private static final ConfigUtil.BooleanParamResolver ldapUseTls;
   private static final ConfigUtil.StringParamResolver ldapSslTruststorePath;
   private static final ConfigUtil.StringParamResolver ldapSslTruststorePassword;
   private static final ConfigUtil.StringParamResolver ldapSslTruststoreType;
   private static final ConfigUtil.StringParamResolver ldapSearchDn;
   private static final ConfigUtil.StringParamResolver ldapSearchPassword;
   private static final ConfigUtil.StringParamResolver ldapUserSearchBase;
   private static final ConfigUtil.StringParamResolver ldapUserSearchFilter;
   private static final ConfigUtil.StringParamResolver ldapUserMemberOfAttribute;
   private static final ConfigUtil.StringParamResolver ldapGroupSearchType;
   private static final ConfigUtil.StringParamResolver ldapGroupSearchBase;
   private static final ConfigUtil.StringParamResolver ldapGroupSearchFilter;
   private static final ConfigUtil.StringParamResolver ldapGroupNameAttribute;
   private static final ConfigUtil.IntParamResolver ldapCredentialsValidity;
   private static final ConfigUtil.IntParamResolver ldapSearchValidity;
   private static final ConfigUtil.IntParamResolver ldapConnectionPoolMaxActive;
   private static final ConfigUtil.IntParamResolver ldapConnectionPoolMaxIdle;
   private static final ConfigUtil.FileParamResolver systemKeyDirectory;
   private static final ConfigUtil.IntParamResolver maxMemoryToLockMB;
   private static final ConfigUtil.DoubleParamResolver maxMemoryToLockFraction;
   private static final long SYSTEM_MEMORY_MB;
   private static final ConfigUtil.BooleanParamResolver cqlSlowLogEnabled;
   private static final ConfigUtil.DoubleParamResolver cqlSlowLogThreshold;
   private static final ConfigUtil.IntParamResolver cqlSlowLogTTL;
   private static final ConfigUtil.IntParamResolver cqlSlowLogMinimumSamples;
   private static final ConfigUtil.BooleanParamResolver cqlSlowLogSkipWritingToDB;
   private static final ConfigUtil.IntParamResolver cqlSlowLogNumSlowestQueries;
   private static final ConfigUtil.BooleanParamResolver sparkClusterInfoEnabled;
   private static final ConfigUtil.IntParamResolver sparkClusterInfoRefreshRate;
   private static final ConfigUtil.BooleanParamResolver sparkApplicationInfoEnabled;
   private static final ConfigUtil.IntParamResolver sparkApplicationInfoRefreshRate;
   private static final ConfigUtil.BooleanParamResolver cqlSystemInfoEnabled;
   private static final ConfigUtil.IntParamResolver cqlSystemInfoRefreshRate;
   private static final ConfigUtil.BooleanParamResolver resourceLatencyTrackingEnabled;
   private static final ConfigUtil.IntParamResolver resourceLatencyRefreshRate;
   private static final ConfigUtil.BooleanParamResolver dbSummaryStatsEnabled;
   private static final ConfigUtil.IntParamResolver dbSummaryStatsRefreshRate;
   private static final ConfigUtil.BooleanParamResolver clusterSummaryStatsEnabled;
   private static final ConfigUtil.IntParamResolver clusterSummaryStatsRefreshRate;
   private static final ConfigUtil.BooleanParamResolver histogramDataTablesEnabled;
   private static final ConfigUtil.IntParamResolver histogramDataTablesRefreshRate;
   private static final ConfigUtil.IntParamResolver histogramDataTablesRetentionCount;
   private static final ConfigUtil.BooleanParamResolver userLatencyTrackingEnabled;
   private static final ConfigUtil.IntParamResolver userLatencyRefreshRate;
   private static final ConfigUtil.IntParamResolver userLatencyTopStatsLimit;
   private static final ConfigUtil.IntParamResolver userLatencyAsyncWriters;
   private static final ConfigUtil.IntParamResolver userLatencyBackpressureThreshold;
   private static final ConfigUtil.IntParamResolver userLatencyFlushTimeout;
   private static final ConfigUtil.BooleanParamResolver userLatencyTrackingQuantiles;
   private static final ConfigUtil.BooleanParamResolver leaseMetricsEnabled;
   private static final ConfigUtil.IntParamResolver leaseMetricsRefreshRate;
   private static final ConfigUtil.IntParamResolver leaseMetricsTtl;
   private static final ConfigUtil.IntParamResolver nodeHealthRefreshRate;
   private static final ConfigUtil.IntParamResolver uptimeRampUpPeriod;
   private static final ConfigUtil.IntParamResolver droppedMutationWindow;
   private static final ConfigUtil.IntParamResolver graphEventsTtl;
   private static volatile ConfigUtil.StringParamResolver auditLogCassWriterMode;
   private static final ConfigUtil.StringParamResolver cqlSolrQueryPaging;
   private static final ConfigUtil.IntParamResolver leaseNettyServerPort;
   private static volatile ConfigUtil.StringParamResolver auditLogCassConsistency;
   public static final int DEFAULT_PERF_MAX_THREADS = 32;
   private static volatile ConfigUtil.IntParamResolver performanceMaxThreads;
   public static final int DEFAULT_PERF_CORE_THREADS = 4;
   private static volatile ConfigUtil.IntParamResolver performanceCoreThreads;
   public static final int DEFAULT_PERF_QUEUE_CAPACITY = 32000;
   private static volatile ConfigUtil.IntParamResolver performanceQueueCapacity;
   private static volatile ConfigUtil.StringParamResolver serverId;
   private static volatile ConfigUtil.IntParamResolver auditLogCassBatchSize;
   private static volatile ConfigUtil.IntParamResolver auditLogCassFlushTime;
   private static volatile ConfigUtil.IntParamResolver auditLogCassQueueSize;
   private static volatile ConfigUtil.IntParamResolver auditLogCassDayPartitionMillis;
   private static final ConfigUtil.IntParamResolver internodeMessagingPort;
   private static final ConfigUtil.IntParamResolver internodeMessagingFrameLength;
   private static final ConfigUtil.IntParamResolver internodeMessagingServerAcceptorThreads;
   private static final ConfigUtil.IntParamResolver internodeMessagingServerWorkerThreads;
   private static final ConfigUtil.IntParamResolver internodeMessagingClientMaxConnections;
   private static final ConfigUtil.IntParamResolver internodeMessagingClientWorkerThreads;
   private static final ConfigUtil.IntParamResolver internodeMessagingClientHandshakeTimeout;
   private static final ConfigUtil.IntParamResolver internodeMessagingClientRequestTimeout;

   public DseConfig() {
   }

   public static AuditLoggingOptions getauditLoggingOptions() {
      return config.audit_logging_options;
   }

   public static void init() {
      logger.info("CQL slow log is {}enabled", ((Boolean)cqlSlowLogEnabled.get()).booleanValue()?"":"not ");
      logger.info("CQL system info tables are {}enabled", ((Boolean)cqlSystemInfoEnabled.get()).booleanValue()?"":"not ");
      logger.info("Resource level latency tracking is {}enabled", ((Boolean)resourceLatencyTrackingEnabled.get()).booleanValue()?"":"not ");
      logger.info("Database summary stats are {}enabled", ((Boolean)dbSummaryStatsEnabled.get()).booleanValue()?"":"not ");
      logger.info("Cluster summary stats are {}enabled", ((Boolean)clusterSummaryStatsEnabled.get()).booleanValue()?"":"not ");
      logger.info("Histogram data tables are {}enabled", ((Boolean)histogramDataTablesEnabled.get()).booleanValue()?"":"not ");
      logger.info("User level latency tracking is {}enabled", ((Boolean)userLatencyTrackingEnabled.get()).booleanValue()?"":"not ");
      logger.info("Spark cluster info tables are {}enabled", ((Boolean)sparkClusterInfoEnabled.get()).booleanValue()?"":"not ");
      if(isLdapAuthEnabled()) {
         logger.info("ldap authentication is enabled");
      }

      if(isKerberosEnabled()) {
         logger.info("kerberos is enabled.");
      }

      if(DatabaseDescriptor.getClientEncryptionOptions().enabled && isKerberosEnabled() && !((String)saslQop.get()).equals("auth")) {
         logger.warn("Configuring auth-int or auth-conf for the QOP is redundant with enabling SSL.  auth-conf + SSL means that the data will be encrypted twice!");
      }

      if(getSystemInfoEncryptionOptions().isEnabled() && DatabaseDescriptor.getCommitLogCompression() != null) {
         throw new ConfigurationRuntimeException("commit log encryption cannot be used with user configured commit log compression");
      } else if(DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && !DatabaseDescriptor.getAuthorizer().isImplementationOf(AllowAllAuthorizer.class) && !DatabaseDescriptor.getAuthorizer().isImplementationOf(DseAuthorizer.class)) {
         throw new ConfigurationRuntimeException(String.format("%s is incompatible with the DseAuthenticator", new Object[]{DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName()}));
      } else {
         logger.info("Cql solr query paging is: " + (String)cqlSolrQueryPaging.get());
         int threadsPerCore = DseConstants.THREADS_PER_CPU_CORE;
         int totalThreads = FBUtilities.getAvailableProcessors();
         logger.info("This instance appears to have {} {} per CPU core and {} total CPU {}.", new Object[]{Integer.valueOf(threadsPerCore), threadsPerCore == 1?"thread":"threads", Integer.valueOf(totalThreads), totalThreads == 1?"thread":"threads"});

         try {
            getSSLContext();
         } catch (Exception var3) {
            throw new ConfigurationException("Failed to initialize SSLContext: " + var3.getMessage(), var3);
         }

         TieredStorageConfigurations.checkConfig();
         logger.info("Server ID:" + (String)serverId.get());
      }
   }

   public static String getServerId() {
      return (String)serverId.get();
   }

   public static IEndpointSnitch getDelegatedSnitch() {
      return (IEndpointSnitch)delegatedSnitch.get();
   }

   public static String getRoleManagement() {
      return (String)roleManagement.get();
   }

   public static boolean isAuthenticationEnabled() {
      return ((Boolean)authenticationEnabled.get()).booleanValue();
   }

   public static String getDefaultAuthenticationScheme() {
      return (String)defaultAuthenticationScheme.get();
   }

   public static Set<String> getOtherAuthenticationSchemes() {
      return (Set)otherAuthenticationSchemes.get();
   }

   public static boolean isAuthenticationSchemePermissions() {
      return ((Boolean)authenticationSchemePermissions.get()).booleanValue();
   }

   public static boolean isAllowDigestWithKerberos() {
      return ((Boolean)allowDigestWithKerberos.get()).booleanValue();
   }

   public static String getPlainTextWithoutSsl() {
      return (String)plainTextWithoutSsl.get();
   }

   public static String getAuthenticationTransitionalMode() {
      return (String)authenticationTransitionalMode.get();
   }

   public static boolean isAuthorizationEnabled() {
      return ((Boolean)authorizationEnabled.get()).booleanValue();
   }

   public static boolean isRowLevelAuthorizationEnabled() {
      return ((Boolean)rowLevelAuthorizationEnabled.get()).booleanValue();
   }

   public static String getAuthorizationTransitionalMode() {
      return (String)authorizationTransitionalMode.get();
   }

   public static String getDseServiceKeytab() {
      return (String)dseServiceKeyTabFile.get();
   }

   public static String getSaslQop() {
      return (String)saslQop.get();
   }

   public static ServicePrincipal getDseServicePrincipal() {
      return (ServicePrincipal)dseServicePrincipal.get();
   }

   public static ServicePrincipal getHttpKrbprincipal() {
      return (ServicePrincipal)httpKrbprincipal.get();
   }

   public static boolean isKerberosDefaultScheme() {
      return DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && ((DseAuthenticator)DatabaseDescriptor.getAuthenticator().implementation()).isKerberosDefaultScheme();
   }

   public static boolean isKerberosEnabled() {
      return DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && ((DseAuthenticator)DatabaseDescriptor.getAuthenticator().implementation()).isKerberosEnabled();
   }

   public static boolean isLdapAuthEnabled() {
      return DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && ((DseAuthenticator)DatabaseDescriptor.getAuthenticator().implementation()).isLdapAuthEnabled();
   }

   public static boolean isPlainTextAuthEnabled() {
      return DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(PasswordAuthenticator.class) || DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && ((DseAuthenticator)DatabaseDescriptor.getAuthenticator().implementation()).isPlainTextAuthEnabled();
   }

   public static boolean isInternalAuthEnabled() {
      return DatabaseDescriptor.getAuthenticator() != null && DatabaseDescriptor.getAuthenticator().isImplementationOf(PasswordAuthenticator.class) || DatabaseDescriptor.getAuthenticator().isImplementationOf(DseAuthenticator.class) && ((DseAuthenticator)DatabaseDescriptor.getAuthenticator().implementation()).isInternalAuthEnabled();
   }

   public static String getLdapServerHost() {
      return (String)ldapServerHost.get();
   }

   public static int getLdapServerPort() {
      return ((Integer)ldapServerPort.get()).intValue();
   }

   public static boolean isLdapUseSSL() {
      return ((Boolean)ldapUseSSL.get()).booleanValue();
   }

   public static boolean isLdapUseTls() {
      return ((Boolean)ldapUseTls.get()).booleanValue();
   }

   public static String getLdapTruststorePath() {
      return (String)ldapSslTruststorePath.get();
   }

   public static String getLdapTruststorePassword() {
      return (String)ldapSslTruststorePassword.get();
   }

   public static String getLdapTruststoreType() {
      return (String)ldapSslTruststoreType.get();
   }

   public static String getLdapSearchDn() {
      return (String)ldapSearchDn.get();
   }

   public static String getLdapSearchPassword() {
      return (String)ldapSearchPassword.get();
   }

   public static String getLdapUserSearchBase() {
      return (String)ldapUserSearchBase.get();
   }

   public static String getLdapUserSearchFilter() {
      return (String)ldapUserSearchFilter.get();
   }

   public static String getLdapUserMemberOfAttribute() {
      return (String)ldapUserMemberOfAttribute.get();
   }

   public static String getLdapGroupSearchType() {
      return (String)ldapGroupSearchType.get();
   }

   public static String getLdapGroupSearchBase() {
      return (String)ldapGroupSearchBase.get();
   }

   public static String getLdapGroupSearchFilter() {
      return (String)ldapGroupSearchFilter.get();
   }

   public static String getLdapGroupNameAttribute() {
      return (String)ldapGroupNameAttribute.get();
   }

   public static int getLdapCredentialsValidity() {
      return ((Integer)ldapCredentialsValidity.get()).intValue();
   }

   public static int getLdapSearchValidity() {
      return ((Integer)ldapSearchValidity.get()).intValue();
   }

   public static int getLdapConnectionPoolMaxActive() {
      return ((Integer)ldapConnectionPoolMaxActive.get()).intValue();
   }

   public static int getLdapConnectionPoolMaxIdle() {
      return ((Integer)ldapConnectionPoolMaxIdle.get()).intValue();
   }

   public static int getGraphEventsTtl() {
      return ((Integer)graphEventsTtl.get()).intValue();
   }

   public static boolean isSslEnabled() {
      return DatabaseDescriptor.getClientEncryptionOptions().enabled;
   }

   public static boolean isSslOptional() {
      return DatabaseDescriptor.getClientEncryptionOptions().optional;
   }

   public static String getSslKeystorePath() {
      return DatabaseDescriptor.getClientEncryptionOptions().keystore != null?DseUtil.makeAbsolute(DatabaseDescriptor.getClientEncryptionOptions().keystore):null;
   }

   public static String getSslKeystorePassword() {
      return DatabaseDescriptor.getClientEncryptionOptions().keystore_password;
   }

   public static String getSslKeystoreType() {
      return DatabaseDescriptor.getClientEncryptionOptions().store_type;
   }

   public static String getSslTruststorePath() {
      return DatabaseDescriptor.getClientEncryptionOptions().truststore != null?DseUtil.makeAbsolute(DatabaseDescriptor.getClientEncryptionOptions().truststore):null;
   }

   public static String getSslTruststorePassword() {
      return DatabaseDescriptor.getClientEncryptionOptions().truststore_password;
   }

   public static String getSslTruststoreType() {
      return DatabaseDescriptor.getClientEncryptionOptions().store_type;
   }

   public static String getSslProtocol() {
      return DatabaseDescriptor.getClientEncryptionOptions().protocol;
   }

   public static boolean isClientAuthRequired() {
      return DatabaseDescriptor.getClientEncryptionOptions().require_client_auth;
   }

   public static SSLContext getSSLContext() throws Exception {
      org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions options = DatabaseDescriptor.getClientEncryptionOptions();
      if(!options.enabled) {
         return null;
      } else {
         KeyManagerFactory kmf = resolveKeyManagerFactorySafely();
         TrustManagerFactory tmf = resolveTrustManagerFactorySafely();
         return SSLUtil.initSSLContext(tmf, kmf, options.protocol);
      }
   }

   /** @deprecated */
   @Deprecated
   public static SSLContext getSSLContext(EncryptionOptions options) throws Exception {
      TrustManagerFactory tmf = SSLUtil.initTrustManagerFactory(DseUtil.makeAbsolute(options.truststore), options.store_type, options.truststore_password);
      KeyManagerFactory kmf = SSLUtil.initKeyManagerFactory(DseUtil.makeAbsolute(options.keystore), options.store_type, options.keystore_password, options.keystore_password);
      return SSLUtil.initSSLContext(tmf, kmf, options.protocol);
   }

   public static SslContext getNettySSLContext(boolean forServer) throws Exception {
      org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions options = DatabaseDescriptor.getClientEncryptionOptions();
      if(!options.enabled) {
         return null;
      } else {
         KeyManagerFactory kmf = resolveKeyManagerFactorySafely();
         TrustManagerFactory tmf = resolveTrustManagerFactorySafely();
         SslContextBuilder builder = forServer?SslContextBuilder.forServer(kmf):SslContextBuilder.forClient().keyManager(kmf);
         if(null == tmf) {
            KeyStore trustStore = SSLUtil.makeTrustStoreFromKeyStore(getSslKeystorePath(), options.store_type, options.keystore_password);
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
         }

         builder.trustManager(tmf);
         if(isClientAuthRequired()) {
            builder.clientAuth(ClientAuth.REQUIRE);
         }

         builder.sslProvider(SslProvider.JDK);
         return builder.build();
      }
   }

   public static SSLContext getSSLContextForInProcessClients() throws Exception {
      org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions options = DatabaseDescriptor.getClientEncryptionOptions();
      if(!options.enabled) {
         return null;
      } else {
         KeyManagerFactory kmf = resolveKeyManagerFactorySafely();
         TrustManagerFactory tmf = resolveTrustManagerFactorySafely();
         if(tmf == null) {
            logger.warn("Could not initialize truststore for internal clients. Falling back to automatic truststore generation.");
            KeyStore trustStore = SSLUtil.makeTrustStoreFromKeyStore(getSslKeystorePath(), options.store_type, options.keystore_password);
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
         }

         return SSLUtil.initSSLContext(tmf, kmf, options.protocol);
      }
   }

   private static KeyManagerFactory resolveKeyManagerFactorySafely() throws IOException, GeneralSecurityException {
      org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions options = DatabaseDescriptor.getClientEncryptionOptions();
      String keystorePath = getSslKeystorePath();
      if(keystorePath == null) {
         throw new RuntimeException("Client encryption is enabled but keystore path is missing. Please configure client_encryption options properly in cassandra.yaml");
      } else if(Files.notExists(Paths.get(keystorePath, new String[0]), new LinkOption[0])) {
         throw new RuntimeException(String.format("Client encryption is enabled but the given keystore file %s does not exist. Please configure client_encryption options properly in cassandra.yaml", new Object[]{keystorePath}));
      } else {
         return SSLUtil.initKeyManagerFactory(keystorePath, options.store_type, options.keystore_password, options.keystore_password);
      }
   }

   private static TrustManagerFactory resolveTrustManagerFactorySafely() throws IOException, GeneralSecurityException {
      org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions options = DatabaseDescriptor.getClientEncryptionOptions();
      TrustManagerFactory tmf = null;
      String truststorePath = getSslTruststorePath();
      if(isClientAuthRequired() && truststorePath == null) {
         throw new RuntimeException("Client authentication through SSL is enabled but truststore path is missing. Please configure client_encryption options properly in cassandra.yaml");
      } else if(isClientAuthRequired() && Files.notExists(Paths.get(truststorePath, new String[0]), new LinkOption[0])) {
         throw new RuntimeException(String.format("Client authentication through SSL is enabled but the given truststore file %s does not exist. Please configure client_encryption options properly in cassandra.yaml", new Object[]{truststorePath}));
      } else if(truststorePath != null && Files.notExists(Paths.get(truststorePath, new String[0]), new LinkOption[0])) {
         logger.warn("Client encryption is enabled but the given truststore file {} does not exist. This is not critical but some client applications may not work properly.", truststorePath);
         return null;
      } else {
         return truststorePath != null && Files.exists(Paths.get(truststorePath, new String[0]), new LinkOption[0])?SSLUtil.initTrustManagerFactory(getSslTruststorePath(), options.store_type, options.truststore_password):null;
      }
   }

   public static String[] getCipherSuites() {
      return !DatabaseDescriptor.getClientEncryptionOptions().enabled?null:DatabaseDescriptor.getClientEncryptionOptions().cipher_suites;
   }

   public static String getCipherSuitesAsString() {
      return !DatabaseDescriptor.getClientEncryptionOptions().enabled?null:Joiner.on(",").join(DatabaseDescriptor.getClientEncryptionOptions().cipher_suites);
   }

   public static boolean performanceObjectsEnabled() {
      return getCqlSlowLogEnabled() || cqlSystemInfoEnabled() || resourceLatencyTrackingEnabled() || dbSummaryStatsEnabled() || clusterSummaryStatsEnabled() || histogramDataTablesStatsEnabled() || userLatencyTrackingEnabled() || sparkClusterInfoEnabled() || sparkApplicationInfoEnabled();
   }

   public static int getPerformanceMaxThreads() {
      return ((Integer)performanceMaxThreads.get()).intValue();
   }

   public static int getPerformanceCoreThreads() {
      return ((Integer)performanceCoreThreads.get()).intValue();
   }

   public static int getPerformanceQueueCapacity() {
      return ((Integer)performanceQueueCapacity.get()).intValue();
   }

   public static boolean getCqlSlowLogEnabled() {
      return ((Boolean)cqlSlowLogEnabled.get()).booleanValue();
   }

   public static double getCqlSlowLogThreshold() {
      return ((Double)cqlSlowLogThreshold.get()).doubleValue();
   }

   public static int getCqlSlowLogMinimumSamples() {
      return ((Integer)cqlSlowLogMinimumSamples.get()).intValue();
   }

   public static int getCqlSlowLogTTL() {
      return ((Integer)cqlSlowLogTTL.get()).intValue();
   }

   public static boolean getCqlSlowLogSkipWritingToDB() {
      return ((Boolean)cqlSlowLogSkipWritingToDB.get()).booleanValue();
   }

   public static int getCqlSlowLogNumSlowestQueries() {
      return ((Integer)cqlSlowLogNumSlowestQueries.get()).intValue();
   }

   public static boolean cqlSystemInfoEnabled() {
      return ((Boolean)cqlSystemInfoEnabled.get()).booleanValue();
   }

   public static boolean sparkClusterInfoEnabled() {
      return ((Boolean)sparkClusterInfoEnabled.get()).booleanValue();
   }

   public static int getSparkClusterInfoRefreshRate() {
      return ((Integer)sparkClusterInfoRefreshRate.get()).intValue();
   }

   public static boolean sparkApplicationInfoEnabled() {
      return ((Boolean)sparkApplicationInfoEnabled.get()).booleanValue();
   }

   public static int getSparkApplicationInfoRefreshRate() {
      return ((Integer)sparkApplicationInfoRefreshRate.get()).intValue();
   }

   public static boolean sparkApplicationInfoDriverSink() {
      return config.spark_application_info_options.driver.sink;
   }

   public static boolean sparkApplicationInfoDriverConnectorSource() {
      return config.spark_application_info_options.driver.connectorSource;
   }

   public static boolean sparkApplicationInfoDriverJvmSource() {
      return config.spark_application_info_options.driver.jvmSource;
   }

   public static boolean sparkApplicationInfoExecutorSink() {
      return config.spark_application_info_options.executor.sink;
   }

   public static boolean sparkApplicationInfoExecutorConnectorSource() {
      return config.spark_application_info_options.executor.connectorSource;
   }

   public static boolean sparkApplicationInfoExecutorJvmSource() {
      return config.spark_application_info_options.executor.jvmSource;
   }

   public static boolean sparkApplicationInfoDriverStateSource() {
      return config.spark_application_info_options.driver.stateSource;
   }

   public static int getCqlSystemInfoRefreshRate() {
      return ((Integer)cqlSystemInfoRefreshRate.get()).intValue();
   }

   public static boolean resourceLatencyTrackingEnabled() {
      return ((Boolean)resourceLatencyTrackingEnabled.get()).booleanValue();
   }

   public static int getResourceLatencyRefreshRate() {
      return ((Integer)resourceLatencyRefreshRate.get()).intValue();
   }

   public static boolean dbSummaryStatsEnabled() {
      return ((Boolean)dbSummaryStatsEnabled.get()).booleanValue();
   }

   public static int getDbSummaryStatsRefreshRate() {
      return ((Integer)dbSummaryStatsRefreshRate.get()).intValue();
   }

   public static boolean clusterSummaryStatsEnabled() {
      return ((Boolean)clusterSummaryStatsEnabled.get()).booleanValue();
   }

   public static int getClusterSummaryStatsRefreshRate() {
      return ((Integer)clusterSummaryStatsRefreshRate.get()).intValue();
   }

   public static boolean histogramDataTablesStatsEnabled() {
      return ((Boolean)histogramDataTablesEnabled.get()).booleanValue();
   }

   public static int getHistogramDataTablesRefreshRate() {
      return ((Integer)histogramDataTablesRefreshRate.get()).intValue();
   }

   public static int getHistogramDataTablesRetentionCount() {
      return ((Integer)histogramDataTablesRetentionCount.get()).intValue();
   }

   public static boolean userLatencyTrackingEnabled() {
      return ((Boolean)userLatencyTrackingEnabled.get()).booleanValue();
   }

   public static int getUserLatencyRefreshRate() {
      return ((Integer)userLatencyRefreshRate.get()).intValue();
   }

   public static int getUserLatencyTopStatsLimit() {
      return ((Integer)userLatencyTopStatsLimit.get()).intValue();
   }

   public static int getUserLatencyAsyncWriters() {
      return ((Integer)userLatencyAsyncWriters.get()).intValue();
   }

   public static int getUserLatencyBackpressureThreshold() {
      return ((Integer)userLatencyBackpressureThreshold.get()).intValue();
   }

   public static int getUserLatencyFlushTimeout() {
      return ((Integer)userLatencyFlushTimeout.get()).intValue();
   }

   public static boolean leaseMetricsEnabled() {
      return ((Boolean)leaseMetricsEnabled.get()).booleanValue();
   }

   public static int getLeaseMetricsRefreshRate() {
      return ((Integer)leaseMetricsRefreshRate.get()).intValue();
   }

   public static int getLeaseMetricsTtl() {
      return ((Integer)leaseMetricsTtl.get()).intValue();
   }

   public static String getConfigEncryptionKeyName() {
      return config.config_encryption_key_name;
   }

   public static boolean getConfigEncryptionActive() {
      return config.config_encryption_active;
   }

   public static boolean resourceLatencyTrackingQuantiles() {
      return ((Boolean)userLatencyTrackingQuantiles.get()).booleanValue();
   }

   public static int getNodeHealthRefreshRate() {
      return ((Integer)nodeHealthRefreshRate.get()).intValue();
   }

   public static int getUptimeRampUpPeriod() {
      return ((Integer)uptimeRampUpPeriod.get()).intValue();
   }

   public static int getDroppedMutationWindow() {
      return ((Integer)droppedMutationWindow.get()).intValue();
   }

   public static String getCqlSolrQueryPaging() {
      return (String)cqlSolrQueryPaging.get();
   }

   private static void applyConfig() throws ConfigurationException {
      KmipHosts.init();
      systemKeyDirectory.withRawParam(config.system_key_directory).check();
      systemKeyDirectory.setUsedDuringInitialization(true);
      maxMemoryToLockMB.withRawParam(config.max_memory_to_lock_mb).check();
      maxMemoryToLockFraction.withRawParam(config.max_memory_to_lock_fraction).check();
      delegatedSnitch.withRawParam(config.delegated_snitch).check();
      if(config.role_management_options != null) {
         roleManagement.withRawParam(config.role_management_options.mode).check();
      }

      if(config.authentication_options != null) {
         authenticationEnabled.withRawParam(config.authentication_options.enabled).check();
         defaultAuthenticationScheme.withRawParam(config.authentication_options.default_scheme).check();
         otherAuthenticationSchemes.withRawParam(config.authentication_options.other_schemes).check();
         authenticationSchemePermissions.withRawParam(config.authentication_options.scheme_permissions).check();
         allowDigestWithKerberos.withRawParam(config.authentication_options.allow_digest_with_kerberos).check();
         plainTextWithoutSsl.withRawParam(config.authentication_options.plain_text_without_ssl).check();
         authenticationTransitionalMode.withRawParam(config.authentication_options.transitional_mode).check();
      }

      if(config.authorization_options != null) {
         authorizationEnabled.withRawParam(config.authorization_options.enabled).check();
         authorizationTransitionalMode.withRawParam(config.authorization_options.transitional_mode).check();
         rowLevelAuthorizationEnabled.withRawParam(config.authorization_options.allow_row_level_security).check();
      }

      if(config.kerberos_options != null) {
         dseServiceKeyTabFile.withRawParam(config.kerberos_options.keytab).check();
         dseServicePrincipal.withRawParam(config.kerberos_options.service_principal).check();
         httpKrbprincipal.withRawParam(config.kerberos_options.http_principal).check();
         saslQop.withRawParam(config.kerberos_options.qop).check();
      }

      if(config.cql_slow_log_options != null) {
         cqlSlowLogEnabled.withRawParam(Boolean.valueOf(config.cql_slow_log_options.enabled)).check();
         cqlSlowLogThreshold.withRawParam(config.cql_slow_log_options.threshold).check();
         cqlSlowLogTTL.withRawParam(config.cql_slow_log_options.ttl_seconds).check();
         cqlSlowLogMinimumSamples.withRawParam(config.cql_slow_log_options.minimum_samples).check();
         cqlSlowLogSkipWritingToDB.withRawParam(Boolean.valueOf(config.cql_slow_log_options.skip_writing_to_db)).check();
         cqlSlowLogNumSlowestQueries.withRawParam(config.cql_slow_log_options.num_slowest_queries).check();
      }

      if(config.ldap_options != null) {
         ldapServerHost.withRawParam(config.ldap_options.server_host).check();
         ldapServerPort.withRawParam(config.ldap_options.server_port).check();
         ldapUseSSL.withRawParam(Boolean.valueOf(config.ldap_options.use_ssl)).check();
         ldapUseTls.withRawParam(Boolean.valueOf(config.ldap_options.use_tls)).check();
         if(config.ldap_options.use_ssl || config.ldap_options.use_tls) {
            ldapSslTruststorePath.withRawParam(config.ldap_options.truststore_path).check();

            try {
               ldapSslTruststorePassword.withRawParam(ConfigDecryptor.getInstance().decryptIfActive(config.ldap_options.truststore_password)).check();
            } catch (IOException var2) {
               throw new ConfigurationException(var2.getLocalizedMessage());
            }

            ldapSslTruststoreType.withRawParam(config.ldap_options.truststore_type).check();
         }

         ldapSearchDn.withRawParam(config.ldap_options.search_dn).check();

         try {
            ldapSearchPassword.withRawParam(ConfigDecryptor.getInstance().decryptIfActive(config.ldap_options.search_password)).check();
         } catch (IOException var1) {
            throw new ConfigurationException(var1.getLocalizedMessage());
         }

         ldapUserSearchBase.withRawParam(config.ldap_options.user_search_base).check();
         ldapUserSearchFilter.withRawParam(config.ldap_options.user_search_filter).check();
         ldapUserMemberOfAttribute.withRawParam(config.ldap_options.user_memberof_attribute).check();
         ldapGroupSearchType.withRawParam(config.ldap_options.group_search_type).check();
         ldapGroupSearchBase.withRawParam(config.ldap_options.group_search_base).check();
         ldapGroupSearchFilter.withRawParam(config.ldap_options.group_search_filter).check();
         ldapGroupNameAttribute.withRawParam(config.ldap_options.group_name_attribute).check();
         ldapCredentialsValidity.withRawParam(config.ldap_options.credentials_validity_in_ms).check();
         ldapSearchValidity.withRawParam(config.ldap_options.search_validity_in_seconds).check();
         ldapConnectionPoolMaxActive.withRawParam(config.ldap_options.connection_pool.max_active).check();
         ldapConnectionPoolMaxIdle.withRawParam(config.ldap_options.connection_pool.max_idle).check();
      }

      if(config.graph_events != null) {
         graphEventsTtl.withRawParam(config.graph_events.ttl_seconds).check();
      }

      sparkClusterInfoEnabled.withRawParam(Boolean.valueOf(config.spark_cluster_info_options.enabled)).check();
      sparkClusterInfoRefreshRate.withRawParam(config.spark_cluster_info_options.refresh_rate_ms).check();
      sparkApplicationInfoEnabled.withRawParam(Boolean.valueOf(config.spark_application_info_options.enabled)).check();
      sparkApplicationInfoRefreshRate.withRawParam(config.spark_application_info_options.refresh_rate_ms).check();
      performanceCoreThreads.withRawParam(config.performance_core_threads).check();
      performanceMaxThreads.withRawParam(config.performance_max_threads).check();
      performanceQueueCapacity.withRawParam(config.performance_queue_capacity).check();
      cqlSystemInfoEnabled.withRawParam(Boolean.valueOf(config.cql_system_info_options.enabled)).check();
      cqlSystemInfoRefreshRate.withRawParam(config.cql_system_info_options.refresh_rate_ms).check();
      resourceLatencyTrackingEnabled.withRawParam(Boolean.valueOf(config.resource_level_latency_tracking_options.enabled)).check();
      resourceLatencyRefreshRate.withRawParam(config.resource_level_latency_tracking_options.refresh_rate_ms).check();
      dbSummaryStatsEnabled.withRawParam(Boolean.valueOf(config.db_summary_stats_options.enabled)).check();
      dbSummaryStatsRefreshRate.withRawParam(config.db_summary_stats_options.refresh_rate_ms).check();
      clusterSummaryStatsEnabled.withRawParam(Boolean.valueOf(config.cluster_summary_stats_options.enabled)).check();
      clusterSummaryStatsRefreshRate.withRawParam(config.cluster_summary_stats_options.refresh_rate_ms).check();
      histogramDataTablesEnabled.withRawParam(Boolean.valueOf(config.histogram_data_options.enabled)).check();
      histogramDataTablesRefreshRate.withRawParam(config.histogram_data_options.refresh_rate_ms).check();
      histogramDataTablesRetentionCount.withRawParam(config.histogram_data_options.retention_count).check();
      userLatencyTrackingEnabled.withRawParam(Boolean.valueOf(config.user_level_latency_tracking_options.enabled)).check();
      userLatencyRefreshRate.withRawParam(config.user_level_latency_tracking_options.refresh_rate_ms).check();
      userLatencyTopStatsLimit.withRawParam(config.user_level_latency_tracking_options.top_stats_limit).check();
      userLatencyBackpressureThreshold.withRawParam(config.user_level_latency_tracking_options.backpressure_threshold).check();
      userLatencyFlushTimeout.withRawParam(config.user_level_latency_tracking_options.flush_timeout_ms).check();
      userLatencyTrackingQuantiles.withRawParam(Boolean.valueOf(config.user_level_latency_tracking_options.quantiles)).check();
      leaseMetricsEnabled.withRawParam(Boolean.valueOf(config.lease_metrics_options.enabled));
      leaseMetricsRefreshRate.withRawParam(config.lease_metrics_options.refresh_rate_ms);
      leaseMetricsTtl.withRawParam(config.lease_metrics_options.ttl_seconds);
      nodeHealthRefreshRate.withRawParam(config.node_health_options.refresh_rate_ms).check();
      uptimeRampUpPeriod.withRawParam(config.node_health_options.uptime_ramp_up_period_seconds).check();
      droppedMutationWindow.withRawParam(config.node_health_options.dropped_mutation_window_minutes).check();
      cqlSolrQueryPaging.withRawParam(config.cql_solr_query_paging).check();
      leaseNettyServerPort.withRawParam(config.lease_netty_server_port).check();
      if(config.audit_logging_options.enabled && config.audit_logging_options.logger.contains(CassandraAuditWriter.class.getSimpleName())) {
         CassandraAuditWriterOptions opts = config.audit_logging_options.cassandra_audit_writer_options;
         auditLogCassWriterMode.withRawParam(opts.mode).check();
         auditLogCassConsistency.withRawParam(opts.write_consistency).check();
         auditLogCassBatchSize.withRawParam(opts.batch_size).check();
         auditLogCassFlushTime.withRawParam(opts.flush_time).check();
         auditLogCassQueueSize.withRawParam(opts.queue_size).check();
         auditLogCassDayPartitionMillis.withRawParam(opts.day_partition_millis).check();
      }

      internodeMessagingPort.withRawParam(config.internode_messaging_options.port).check();
      internodeMessagingFrameLength.withRawParam(config.internode_messaging_options.frame_length_in_mb).check();
      internodeMessagingServerAcceptorThreads.withRawParam(config.internode_messaging_options.server_acceptor_threads).check();
      internodeMessagingServerWorkerThreads.withRawParam(config.internode_messaging_options.server_worker_threads).check();
      internodeMessagingClientMaxConnections.withRawParam(config.internode_messaging_options.client_max_connections).check();
      internodeMessagingClientWorkerThreads.withRawParam(config.internode_messaging_options.client_worker_threads).check();
      internodeMessagingClientHandshakeTimeout.withRawParam(config.internode_messaging_options.handshake_timeout_seconds).check();
      internodeMessagingClientRequestTimeout.withRawParam(config.internode_messaging_options.client_request_timeout_seconds).check();
      serverId.withRawParam(config.server_id).check();
   }

   public static File getSystemKeyDirectory() {
      return (File)systemKeyDirectory.get();
   }

   public static long getMaxMemoryToLockBytes() {
      long retVal = 0L;
      if(((Integer)maxMemoryToLockMB.get()).intValue() != 0) {
         retVal = (long)((Integer)maxMemoryToLockMB.get()).intValue() * 1024L * 1024L;
      } else {
         retVal = (long)(((Double)maxMemoryToLockFraction.get()).doubleValue() * (double)(SYSTEM_MEMORY_MB * 1024L * 1024L));
      }

      return retVal;
   }

   public static double getMaxMemoryToLockFraction() {
      return ((Double)maxMemoryToLockFraction.get()).doubleValue();
   }

   public static int getMaxMemoryToLockMBAsRaw() {
      return ((Integer)maxMemoryToLockMB.get()).intValue();
   }

   public static boolean getAuditLoggingEnabled() {
      return config.audit_logging_options.enabled;
   }

   public static String getAuditLoggerName() {
      return config.audit_logging_options.logger;
   }

   public static int getAuditLoggerRetentionTime() {
      return config.audit_logging_options.retention_time;
   }

   public static String getAuditLoggerCassMode() {
      return config.audit_logging_options.cassandra_audit_writer_options.mode;
   }

   public static int getAuditLoggerCassAsyncQueueSize() {
      return ((Integer)auditLogCassQueueSize.get()).intValue();
   }

   public static int getAuditCassFlushTime() {
      return ((Integer)auditLogCassFlushTime.get()).intValue();
   }

   public static int getAuditCassBatchSize() {
      return ((Integer)auditLogCassBatchSize.get()).intValue();
   }

   public static ConsistencyLevel getAuditCassConsistencyLevel() {
      return ConsistencyLevel.valueOf(config.audit_logging_options.cassandra_audit_writer_options.write_consistency);
   }

   public static String getAuditCassDroppedLogLocation() {
      return config.audit_logging_options.cassandra_audit_writer_options.dropped_event_log;
   }

   public static Set<String> getAuditIncludedCategories() {
      return commaSeparatedStrToSet(config.audit_logging_options.included_categories);
   }

   public static Set<String> getAuditExcludedCategories() {
      return commaSeparatedStrToSet(config.audit_logging_options.excluded_categories);
   }

   public static Set<String> getAuditIncludedKeyspaces() {
      return commaSeparatedStrToSet(config.audit_logging_options.included_keyspaces);
   }

   public static Set<String> getAuditExcludedKeyspaces() {
      return commaSeparatedStrToSet(config.audit_logging_options.excluded_keyspaces);
   }

   public static Set<String> getAuditIncludedRoles() {
      return commaSeparatedStrToSet(config.audit_logging_options.included_roles);
   }

   public static Set<String> getAuditExcludedRoles() {
      return commaSeparatedStrToSet(config.audit_logging_options.excluded_roles);
   }

   public static int getAuditDayPartitionMillis() {
      return ((Integer)auditLogCassDayPartitionMillis.get()).intValue();
   }

   private static Set<String> commaSeparatedStrToSet(String valueString) {
      if(valueString == null) {
         return new HashSet();
      } else if(valueString.trim().length() == 0) {
         return new HashSet();
      } else {
         Set<String> values = new HashSet();
         String[] var2 = valueString.split(",");
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String value = var2[var4];
            if(value.trim().length() > 0) {
               values.add(value.trim());
            }
         }

         return values;
      }
   }

   public static SystemTableEncryptionOptions.Wrapper getSystemInfoEncryptionOptions() {
      return config.system_info_encryption.getWrapper();
   }

   public static boolean isAdvancedReplicationEnabled() {
      return config.advanced_replication_options.enabled;
   }

   public static boolean isConfigDriverPwdEncryptionEnabled() {
      return config.advanced_replication_options.conf_driver_password_encryption_enabled;
   }

   public static String getAdvRepSecurityBasePath() {
      return config.advanced_replication_options.security_base_path;
   }

   public static String getAdvRepBaseDirectory() {
      return DseConfigUtil.getAdvancedReplicationDirectory(config.advanced_replication_options.advanced_replication_directory, DatabaseDescriptor::getCDCLogLocation);
   }

   public static Path getAdvRepReplicationLogPath() {
      return Paths.get(getAdvRepBaseDirectory(), new String[]{"replication_log"});
   }

   public static String getAdvRepReplicationLogDirectory() {
      return getAdvRepReplicationLogPath().toString();
   }

   public static Path getAdvRepAuditLogPath() {
      return Paths.get(getAdvRepBaseDirectory(), new String[]{"auditlog"});
   }

   public static String getAdvRepAuditLogDirectory() {
      return getAdvRepAuditLogPath().toString();
   }

   public static Path getAdvRepInvalidQueriesLogPath() {
      return Paths.get(getAdvRepBaseDirectory(), new String[]{"invalid_queries"});
   }

   public static String getAdvRepInvalidQueriesLogDirectory() {
      return getAdvRepInvalidQueriesLogPath().toString();
   }

   private static void closeQuietly(Closeable c) {
      try {
         if(c != null) {
            c.close();
         }
      } catch (Exception var2) {
         logger.warn("Failed closing " + c, var2);
      }

   }

   public static Map<String, KmipHostOptions> getKmipHosts() {
      return ImmutableMap.copyOf(config.kmip_hosts);
   }

   public static Map<String, TieredStorageConfig> getTieredStorageOptions() {
      Map<String, TieredStorageConfig> configMap = new HashMap();
      Iterator var1 = config.tiered_storage_options.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<String, TieredStorageYamlConfig> entry = (Entry)var1.next();
         configMap.put(entry.getKey(), TieredStorageConfig.fromYamlConfig((TieredStorageYamlConfig)entry.getValue()));
      }

      return ImmutableMap.copyOf(configMap);
   }

   public static int getInternodeMessagingPort() {
      return ((Integer)internodeMessagingPort.get()).intValue();
   }

   public static int getInternodeMessagingFrameLength() {
      return ((Integer)internodeMessagingFrameLength.get()).intValue();
   }

   public static int getInternodeMessagingServerAcceptorThreads() {
      return ((Integer)internodeMessagingServerAcceptorThreads.get()).intValue();
   }

   public static int getInternodeMessagingServerWorkerThreads() {
      return ((Integer)internodeMessagingServerWorkerThreads.get()).intValue();
   }

   public static int getInternodeMessagingClientMaxConnections() {
      return ((Integer)internodeMessagingClientMaxConnections.get()).intValue();
   }

   public static int getInternodeMessagingClientWorkerThreads() {
      return ((Integer)internodeMessagingClientWorkerThreads.get()).intValue();
   }

   public static int getInternodeMessagingClientHandshakeTimeout() {
      return ((Integer)internodeMessagingClientHandshakeTimeout.get()).intValue();
   }

   public static int getInternodeMessagingClientRequestTimeout() {
      return ((Integer)internodeMessagingClientRequestTimeout.get()).intValue();
   }

   static {
      DEFAULT_RAMP_UP_PERIOD_SECONDS = TimeUnit.HOURS.toSeconds(3L);
      logger = LoggerFactory.getLogger(DseConfig.class);
      delegatedSnitch = new DseConfig.EndpointSnitchParamResolver("delegated_snitch");
      roleManagement = (new ConfigUtil.StringParamResolver("role_management_options.mode", "internal")).withAllowedValues(new String[]{"internal", "ldap"});
      authenticationEnabled = new ConfigUtil.BooleanParamResolver("authentication_options.enabled", Boolean.valueOf(false));
      defaultAuthenticationScheme = (new ConfigUtil.StringParamResolver("authentication_options.default_scheme", "internal")).withAllowedValues(new String[]{"internal", "ldap", "kerberos"});
      otherAuthenticationSchemes = (new ConfigUtil.SetParamResolver("authentication_options.other_schemes", Collections.emptySet())).withAllowedValues(new String[]{"internal", "ldap", "kerberos"});
      authenticationSchemePermissions = new ConfigUtil.BooleanParamResolver("authentication_options.scheme_permissions", Boolean.valueOf(false));
      allowDigestWithKerberos = new ConfigUtil.BooleanParamResolver("authentication_options.allow_digest_with_kerberos", Boolean.valueOf(true));
      plainTextWithoutSsl = (new ConfigUtil.StringParamResolver("authentication_options.plain_text_without_ssl", "warn")).withAllowedValues(new String[]{"block", "warn", "allow"});
      authenticationTransitionalMode = (new ConfigUtil.StringParamResolver("authentication_options.transitional_mode", "disabled")).withAllowedValues(new String[]{"disabled", "permissive", "normal", "strict"});
      authorizationEnabled = new ConfigUtil.BooleanParamResolver("authorization_options.enabled", Boolean.valueOf(false));
      authorizationTransitionalMode = (new ConfigUtil.StringParamResolver("authorization_options.transitional_mode", "disabled")).withAllowedValues(new String[]{"disabled", "normal", "strict"});
      rowLevelAuthorizationEnabled = new ConfigUtil.BooleanParamResolver("authorization_options.allow_row_level_security", Boolean.valueOf(false));
      dseServiceKeyTabFile = new ConfigUtil.StringParamResolver("kerberos_options.keytab", (String)null);
      saslQop = (new ConfigUtil.StringParamResolver("kerberos_options.qop", "auth")).withAllowedValues(new String[]{"auth", "auth-int", "auth-conf"});
      dseServicePrincipal = new DseConfig.ServicePrincipalParamResolver("kerberos_options.service_principal", (ServicePrincipal)null);
      httpKrbprincipal = new DseConfig.ServicePrincipalParamResolver("kerberos_options.http_principal", (ServicePrincipal)null);
      ldapServerHost = new ConfigUtil.StringParamResolver("ldap_options.server_host", (String)null);
      ldapServerPort = new ConfigUtil.IntParamResolver("ldap_options.server_port", Integer.valueOf(389));
      ldapUseSSL = new ConfigUtil.BooleanParamResolver("ldap_options.use_ssl", Boolean.valueOf(false));
      ldapUseTls = new ConfigUtil.BooleanParamResolver("ldap_options.use_tls", Boolean.valueOf(false));
      ldapSslTruststorePath = new ConfigUtil.StringParamResolver("ldap_options.truststore_path", (String)null);
      ldapSslTruststorePassword = new ConfigUtil.StringParamResolver("ldap_options.truststore_password", (String)null);
      ldapSslTruststoreType = new ConfigUtil.StringParamResolver("ldap_options.truststore_type", "jks");
      ldapSearchDn = new ConfigUtil.StringParamResolver("ldap_options.search_dn", (String)null);
      ldapSearchPassword = new ConfigUtil.StringParamResolver("ldap_options.search_password", (String)null);
      ldapUserSearchBase = new ConfigUtil.StringParamResolver("ldap_options.user_search_base", (String)null);
      ldapUserSearchFilter = new ConfigUtil.StringParamResolver("ldap_options.user_search_filter", "(uid={0})");
      ldapUserMemberOfAttribute = new ConfigUtil.StringParamResolver("ldap_options.user_memberof_attribute", "memberof");
      ldapGroupSearchType = (new ConfigUtil.StringParamResolver("ldap_options.group_search_type", "directory_search")).withAllowedValues(new String[]{"directory_search", "memberof_search"});
      ldapGroupSearchBase = new ConfigUtil.StringParamResolver("ldap_options.group_search_base", (String)null);
      ldapGroupSearchFilter = new ConfigUtil.StringParamResolver("ldap_options.group_search_filter", "(uniquemember={0})");
      ldapGroupNameAttribute = new ConfigUtil.StringParamResolver("ldap_options.group_name_attribute", "cn");
      ldapCredentialsValidity = new ConfigUtil.IntParamResolver("ldap_options.credentials_validity_in_ms", Integer.valueOf(0));
      ldapSearchValidity = new ConfigUtil.IntParamResolver("ldap_options.search_validity_in_seconds", Integer.valueOf(0));
      ldapConnectionPoolMaxActive = new ConfigUtil.IntParamResolver("ldap_options.connection_pool.max_active", Integer.valueOf(8));
      ldapConnectionPoolMaxIdle = new ConfigUtil.IntParamResolver("ldap_options.connection_pool.max_idle", Integer.valueOf(8));
      systemKeyDirectory = new ConfigUtil.FileParamResolver("system_key_directory", new File("/etc/dse/conf"));
      maxMemoryToLockMB = (new ConfigUtil.IntParamResolver("max_memory_to_lock_mb", Integer.valueOf(0))).withLowerBound(0);
      maxMemoryToLockFraction = (new ConfigUtil.DoubleParamResolver("max_memory_to_lock_fraction", Double.valueOf(0.2D))).withLowerBound(0.0D).withUpperBound(1.0D);
      SYSTEM_MEMORY_MB = Long.getLong("dse.system_memory_in_mb", 2048L).longValue();
      cqlSlowLogEnabled = new ConfigUtil.BooleanParamResolver("cql_slow_log_options.enabled", CqlSlowLogOptions.ENABLED_DEFAULT);
      cqlSlowLogThreshold = (new ConfigUtil.DoubleParamResolver("cql_slow_log_options.threshold", CqlSlowLogOptions.THRESHOLD_DEFAULT)).withLowerBound(0.0D);
      cqlSlowLogTTL = (new ConfigUtil.IntParamResolver("cql_slow_log_options.ttl_seconds", CqlSlowLogOptions.TTL_DEFAULT)).withLowerBound(0);
      cqlSlowLogMinimumSamples = (new ConfigUtil.IntParamResolver("cql_slow_log_options.minimum_samples", CqlSlowLogOptions.MINIMUM_SAMPLES_DEFAULT)).withLowerBound(1);
      cqlSlowLogSkipWritingToDB = new ConfigUtil.BooleanParamResolver("cql_slow_log_options.skip_writing_to_db", CqlSlowLogOptions.SKIP_WRITING_TO_DB_DEFAULT);
      cqlSlowLogNumSlowestQueries = (new ConfigUtil.IntParamResolver("cql_slow_log_options.num_slowest_queries", CqlSlowLogOptions.NUM_SLOWEST_QUERIES_DEFAULT)).withLowerBound(CqlSlowLogOptions.NUM_SLOWEST_QUERIES_LOWER_BOUND.intValue());
      sparkClusterInfoEnabled = new ConfigUtil.BooleanParamResolver("spark_cluster_info_options.enabled", Boolean.valueOf(false));
      sparkClusterInfoRefreshRate = (new ConfigUtil.IntParamResolver("spark_cluster_info_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(1000);
      sparkApplicationInfoEnabled = new ConfigUtil.BooleanParamResolver("spark_application_info_options.enabled", Boolean.valueOf(false));
      sparkApplicationInfoRefreshRate = (new ConfigUtil.IntParamResolver("spark_application_info_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(1000);
      cqlSystemInfoEnabled = new ConfigUtil.BooleanParamResolver("cql_system_info_options.enabled", Boolean.valueOf(false));
      cqlSystemInfoRefreshRate = (new ConfigUtil.IntParamResolver("cql_system_info.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      resourceLatencyTrackingEnabled = new ConfigUtil.BooleanParamResolver("resource_level_latency_tracking_options.enabled", Boolean.valueOf(false));
      resourceLatencyRefreshRate = (new ConfigUtil.IntParamResolver("resource_level_latency_tracking_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      dbSummaryStatsEnabled = new ConfigUtil.BooleanParamResolver("db_summary_stats_options.enabled", Boolean.valueOf(false));
      dbSummaryStatsRefreshRate = (new ConfigUtil.IntParamResolver("db_summary_stats_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      clusterSummaryStatsEnabled = new ConfigUtil.BooleanParamResolver("cluster_summary_stats_options.enabled", Boolean.valueOf(false));
      clusterSummaryStatsRefreshRate = (new ConfigUtil.IntParamResolver("cluster_summary_stats_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      histogramDataTablesEnabled = new ConfigUtil.BooleanParamResolver("histogram_data_options.enabled", Boolean.valueOf(false));
      histogramDataTablesRefreshRate = (new ConfigUtil.IntParamResolver("histogram_data_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      histogramDataTablesRetentionCount = new ConfigUtil.IntParamResolver("histogram_data_options.retention_count", Integer.valueOf(3));
      userLatencyTrackingEnabled = new ConfigUtil.BooleanParamResolver("user_level_latency_tracking_options.enabled", Boolean.valueOf(false));
      userLatencyRefreshRate = (new ConfigUtil.IntParamResolver("user_level_latency_tracking_options.refresh_rate_ms", Integer.valueOf(10000))).withLowerBound(MIN_REFRESH_RATE_MS);
      userLatencyTopStatsLimit = new ConfigUtil.IntParamResolver("user_level_latency_tracking_options.top_stats_limit", Integer.valueOf(100));
      userLatencyAsyncWriters = new ConfigUtil.IntParamResolver("user_level_latency_tracking_options.async_writers", Integer.valueOf(1));
      userLatencyBackpressureThreshold = new ConfigUtil.IntParamResolver("user_level_latency_tracking_options.backpressure_threshold", Integer.valueOf(1000));
      userLatencyFlushTimeout = new ConfigUtil.IntParamResolver("user_level_latency_tracking_options.flush_timeout_ms", Integer.valueOf(1000));
      userLatencyTrackingQuantiles = new ConfigUtil.BooleanParamResolver("user_level_latency_tracking_options.quantiles");
      leaseMetricsEnabled = new ConfigUtil.BooleanParamResolver("lease_metrics_options.enabled", Boolean.valueOf(false));
      leaseMetricsRefreshRate = (new ConfigUtil.IntParamResolver("lease_metrics_options.refresh_rate_ms", Integer.valueOf(20000))).withLowerBound(10000).withUpperBound(180000);
      leaseMetricsTtl = (new ConfigUtil.IntParamResolver("lease_metrics_options.ttl_seconds", Integer.valueOf(86400))).withLowerBound(0);
      nodeHealthRefreshRate = new ConfigUtil.IntParamResolver("node_health_options.refresh_rate_ms", Integer.valueOf('\uea60'));
      uptimeRampUpPeriod = new ConfigUtil.IntParamResolver("node_health_options.uptime_ramp_up_period_seconds", Integer.valueOf((int)DEFAULT_RAMP_UP_PERIOD_SECONDS));
      droppedMutationWindow = (new ConfigUtil.IntParamResolver("node_health_options.dropped_mutation_window_minutes", Integer.valueOf(30))).withLowerBound(1);
      graphEventsTtl = new ConfigUtil.IntParamResolver("graph_events.ttl_seconds", Integer.valueOf(600));
      auditLogCassWriterMode = (new ConfigUtil.StringParamResolver("audit_logging_options.cassandra_audit_writer_options.mode", "sync")).withAllowedValues(new String[]{"sync", "async"});
      cqlSolrQueryPaging = (new ConfigUtil.StringParamResolver("cql_solr_query_paging", "off")).withAllowedValues(new String[]{"driver", "off"});
      leaseNettyServerPort = (new ConfigUtil.IntParamResolver("lease_netty_server_port", Integer.valueOf(-1))).withLowerBound(0).withUpperBound('\uffff');
      performanceMaxThreads = (new ConfigUtil.IntParamResolver("performance_max_threads", Integer.valueOf(32))).withLowerBound(1);
      performanceCoreThreads = (new ConfigUtil.IntParamResolver("performance_core_threads", Integer.valueOf(4))).withLowerBound(0);
      performanceQueueCapacity = (new ConfigUtil.IntParamResolver("performance_queue_capacity", Integer.valueOf(32000))).withLowerBound(0);
      serverId = new ConfigUtil.StringParamResolver("server_id", ServerId.genID());
      Function<ConsistencyLevel, String> transform = new Function<ConsistencyLevel, String>() {
         public String apply(ConsistencyLevel cl) {
            return cl.name();
         }
      };
      List<String> valueList = Lists.transform(Lists.newArrayList(ConsistencyLevel.values()), transform);
      String[] valueArray = new String[valueList.size()];
      valueList.toArray(valueArray);
      auditLogCassConsistency = (new ConfigUtil.StringParamResolver("audit_logging_options.cassandra_audit_writer_options.write_consistency", "QUORUM")).withAllowedValues(valueArray);
      auditLogCassBatchSize = (new ConfigUtil.IntParamResolver("audit_logging_options.cassandra_audit_writer_options.batch_size", Integer.valueOf(50))).withLowerBound(1);
      auditLogCassFlushTime = (new ConfigUtil.IntParamResolver("audit_logging_options.cassandra_audit_writer_options.flush_time", Integer.valueOf(250))).withLowerBound(1);
      auditLogCassQueueSize = (new ConfigUtil.IntParamResolver("audit_logging_options.cassandra_audit_writer_options.queue_size", Integer.valueOf(30000))).withLowerBound(0);
      auditLogCassDayPartitionMillis = (new ConfigUtil.IntParamResolver("audit_logging_options.cassandra_audit_writer_options.day_partition_millis", Integer.valueOf(3600000))).withLowerBound(1);
      internodeMessagingPort = (new ConfigUtil.IntParamResolver("internode_messaging_options.port")).withLowerBound(1).withUpperBound('\uffff');
      internodeMessagingFrameLength = (new ConfigUtil.IntParamResolver("internode_messaging_options.frame_length_in_mb", Integer.valueOf(256))).withLowerBound(1).withUpperBound('\uffff');
      internodeMessagingServerAcceptorThreads = (new ConfigUtil.IntParamResolver("internode_messaging_options.server_acceptor_threads", Integer.valueOf(InternodeMessaging.SERVER_ACCEPTOR_THREADS))).withLowerBound(1);
      internodeMessagingServerWorkerThreads = (new ConfigUtil.IntParamResolver("internode_messaging_options.server_worker_threads", Integer.valueOf(InternodeMessaging.SERVER_WORKER_THREADS))).withLowerBound(1);
      internodeMessagingClientMaxConnections = (new ConfigUtil.IntParamResolver("internode_messaging_options.client_max_connections", Integer.valueOf(100))).withLowerBound(1);
      internodeMessagingClientWorkerThreads = (new ConfigUtil.IntParamResolver("internode_messaging_options.client_worker_threads", Integer.valueOf(InternodeMessaging.CLIENT_WORKER_THREADS))).withLowerBound(1);
      internodeMessagingClientHandshakeTimeout = (new ConfigUtil.IntParamResolver("internode_messaging_options.handshake_timeout_seconds", Integer.valueOf(10))).withLowerBound(1);
      internodeMessagingClientRequestTimeout = (new ConfigUtil.IntParamResolver("internode_messaging_options.client_request_timeout_seconds", Integer.valueOf(60))).withLowerBound(1);

      try {
         applyConfig();
         enableResolvers(DseConfig.class);
         logger.info("Load of settings is done.");
      } catch (ConfigurationException var3) {
         throw new ExceptionInInitializerError(var3);
      }
   }

   private static class ServicePrincipalParamResolver extends ConfigUtil.ParamResolver<ServicePrincipal> {
      public ServicePrincipalParamResolver(String name, ServicePrincipal defaultValue) {
         super(name, defaultValue);
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty((String)rawParam);
      }

      protected Pair<ServicePrincipal, Boolean> convert(Object rawParam) {
         try {
            return Pair.of(new ServicePrincipal(((String)rawParam).trim()), Boolean.valueOf(false));
         } catch (RuntimeException var3) {
            return (Pair)this.fail(String.format("%s is not a valid ServicePrincipal.", new Object[]{rawParam}));
         }
      }

      protected Pair<ServicePrincipal, Boolean> validateOrGetDefault(Pair<ServicePrincipal, Boolean> value) {
         return value;
      }
   }

   private static class EndpointSnitchParamResolver extends ConfigUtil.ParamResolver<IEndpointSnitch> {
      private IEndpointSnitch snitch;

      public EndpointSnitchParamResolver(String name) {
         super(name);
      }

      protected Pair<IEndpointSnitch, Boolean> convert(Object rawParam) {
         if(this.snitch == null) {
            String delegatedSnitchClassName = DseConfigurationLoader.getInitialEndpointSnitch();
            if(delegatedSnitchClassName == null) {
               DseConfig.logger.error("No snitch specified for DseDelegateSnitch to delegate to. Please set endpoint_snitch in cassandra.yaml to a valid IEndpointSnitch classname");
               throw new ConfigurationRuntimeException("No snitch specified");
            }

            try {
               this.snitch = (IEndpointSnitch)FBUtilities.construct(delegatedSnitchClassName, "snitch");
            } catch (ConfigurationException var4) {
               throw new ConfigurationRuntimeException("Unable to get snitch", var4);
            }
         }

         return Pair.of(this.snitch, Boolean.valueOf(false));
      }

      public void check() {
      }

      protected boolean isNotEmpty(Object rawParam) {
         return true;
      }

      protected Pair<IEndpointSnitch, Boolean> validateOrGetDefault(Pair<IEndpointSnitch, Boolean> value) {
         return value;
      }
   }
}
