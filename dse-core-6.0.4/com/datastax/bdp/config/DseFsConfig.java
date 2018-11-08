package com.datastax.bdp.config;

import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.DseUtil;
import com.google.common.collect.Sets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;

public class DseFsConfig extends DseConfigYamlLoader {
   private static final ConfigUtil.BooleanParamResolver enabled = new ConfigUtil.BooleanParamResolver("dsefs_options.enabled", (Boolean)null);
   private static final ConfigUtil.StringParamResolver keyspaceName = new ConfigUtil.StringParamResolver("dsefs_options.keyspace_name", "dsefs");
   private static final ConfigUtil.StringParamResolver workDirectory = new ConfigUtil.StringParamResolver("dsefs_options.work_dir", "/var/lib/dsefs");
   private static final DseFsDataDirectoriesResolver dataDirectories;
   private static final ConfigUtil.IntParamResolver publicPort;
   private static final ConfigUtil.IntParamResolver privatePort;
   private static final ConfigUtil.IntParamResolver serviceStartupTimeout;
   private static final ConfigUtil.IntParamResolver serviceCloseTimeout;
   private static final ConfigUtil.IntParamResolver serverCloseTimeout;
   private static final ConfigUtil.IntParamResolver compressionFrameMaxSize;
   private static final ConfigUtil.IntParamResolver queryCacheSize;
   private static final ConfigUtil.IntParamResolver queryCacheExpireAfterMillis;
   private static final ConfigUtil.IntParamResolver transactionTimeout;
   private static final ConfigUtil.IntParamResolver conflictRetryDelay;
   private static final ConfigUtil.IntParamResolver conflictRetryCount;
   private static final ConfigUtil.IntParamResolver executionRetryDelay;
   private static final ConfigUtil.IntParamResolver executionRetryCount;
   private static final ConfigUtil.IntParamResolver gossipStartupDelay;
   private static final ConfigUtil.IntParamResolver gossipRoundDelay;
   private static final ConfigUtil.IntParamResolver gossipShutdownDelay;
   private static final ConfigUtil.IntParamResolver restServerRequestTimeout;
   private static final ConfigUtil.IntParamResolver idleConnectionTimeout;
   private static final ConfigUtil.IntParamResolver internodeIdleConnectionTimeout;
   private static final ConfigUtil.IntParamResolver restRequestTimeout;
   private static final ConfigUtil.IntParamResolver restConnectionOpenTimeout;
   private static final ConfigUtil.IntParamResolver restClientCloseTimeout;
   private static final ConfigUtil.IntParamResolver coreMaxConcurrentConnectionsPerHost;
   private static final ConfigUtil.IntParamResolver blockAllocatorOverflowMargin;
   private static final ConfigUtil.DoubleParamResolver blockAllocatorOverflowFactor;

   public DseFsConfig() {
   }

   public static Boolean isEnabled() {
      Boolean enabledOrNull = (Boolean)enabled.get();
      return Boolean.valueOf(enabledOrNull != null?enabledOrNull.booleanValue():DseUtil.getWorkloads().contains(Workload.Analytics));
   }

   public static String getKeyspaceName() {
      return (String)keyspaceName.get();
   }

   public static Path getWorkDirectory() {
      return Paths.get((String)workDirectory.get(), new String[0]);
   }

   public static Set<DseFsDataDirectoryConfig> getDataDirectories() {
      return (Set)dataDirectories.get();
   }

   public static int getPublicPort() {
      return ((Integer)publicPort.get()).intValue();
   }

   public static int getPrivatePort() {
      return ((Integer)privatePort.get()).intValue();
   }

   public static int getRestServerRequestTimeout() {
      return ((Integer)restServerRequestTimeout.get()).intValue();
   }

   public static int getIdleConnectionTimeout() {
      return ((Integer)idleConnectionTimeout.get()).intValue();
   }

   public static int getInternodeIdleConnectionTimeout() {
      return ((Integer)internodeIdleConnectionTimeout.get()).intValue();
   }

   public static int getGossipStartupDelay() {
      return ((Integer)gossipStartupDelay.get()).intValue();
   }

   public static int getGossipRoundDelay() {
      return ((Integer)gossipRoundDelay.get()).intValue();
   }

   public static int getGossipShutdownDelay() {
      return ((Integer)gossipShutdownDelay.get()).intValue();
   }

   public static int getTransactionTimeout() {
      return ((Integer)transactionTimeout.get()).intValue();
   }

   public static int getConflictRetryDelay() {
      return ((Integer)conflictRetryDelay.get()).intValue();
   }

   public static int getConflictRetryCount() {
      return ((Integer)conflictRetryCount.get()).intValue();
   }

   public static int getExecutionRetryDelay() {
      return ((Integer)executionRetryDelay.get()).intValue();
   }

   public static int getExecutionRetryCount() {
      return ((Integer)executionRetryCount.get()).intValue();
   }

   public static int getServiceStartupTimeout() {
      return ((Integer)serviceStartupTimeout.get()).intValue();
   }

   public static int getServiceCloseTimeout() {
      return ((Integer)serviceCloseTimeout.get()).intValue();
   }

   public static int getServerCloseTimeout() {
      return ((Integer)serverCloseTimeout.get()).intValue();
   }

   public static int getRestRequestTimeout() {
      return ((Integer)restRequestTimeout.get()).intValue();
   }

   public static int getRestConnectionOpenTimeout() {
      return ((Integer)restConnectionOpenTimeout.get()).intValue();
   }

   public static int getRestClientCloseTimeout() {
      return ((Integer)restClientCloseTimeout.get()).intValue();
   }

   public static int getCoreMaxConcurrentConnectionsPerHost() {
      return ((Integer)coreMaxConcurrentConnectionsPerHost.get()).intValue();
   }

   public static int getCompressionFrameMaxSize() {
      return ((Integer)compressionFrameMaxSize.get()).intValue();
   }

   public static long getBlockAllocatorOverflowMargin() {
      return (long)(((Integer)blockAllocatorOverflowMargin.get()).intValue() * 1024) * 1024L;
   }

   public static double getBlockAllocatorOverflowFactor() {
      return ((Double)blockAllocatorOverflowFactor.get()).doubleValue();
   }

   public static int getQueryCacheSize() {
      return ((Integer)queryCacheSize.get()).intValue();
   }

   public static int getQueryCacheExpireAfterMillis() {
      return ((Integer)queryCacheExpireAfterMillis.get()).intValue();
   }

   static {
      dataDirectories = new DseFsDataDirectoriesResolver("dsefs_options.data_directories", Sets.newHashSet(new DseFsDataDirectoryConfig[]{DseFsDataDirectoryConfig.DEFAULT_DSEFS_DATA_DIR}));
      publicPort = new ConfigUtil.IntParamResolver("dsefs_options.public_port", Integer.valueOf(5598));
      privatePort = new ConfigUtil.IntParamResolver("dsefs_options.private_port", Integer.valueOf(5599));
      serviceStartupTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.service_startup_timeout_ms", Integer.valueOf(600000))).withLowerBound(0);
      serviceCloseTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.service_close_timeout_ms", Integer.valueOf(600000))).withLowerBound(0);
      serverCloseTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.server_close_timeout_ms", Integer.valueOf(2147483647))).withLowerBound(0);
      compressionFrameMaxSize = (new ConfigUtil.IntParamResolver("dsefs_options.compression_frame_max_size", Integer.valueOf(1048576))).withLowerBound(0);
      queryCacheSize = (new ConfigUtil.IntParamResolver("dsefs_options.query_cache_size", Integer.valueOf(2048))).withLowerBound(0);
      queryCacheExpireAfterMillis = (new ConfigUtil.IntParamResolver("dsefs_options.query_cache_expire_after_ms", Integer.valueOf(2000))).withLowerBound(0);
      transactionTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.transaction_options.transaction_timeout_ms", Integer.valueOf('\uea60'))).withLowerBound(0);
      conflictRetryDelay = (new ConfigUtil.IntParamResolver("dsefs_options.transaction_options.conflict_retry_delay_ms", Integer.valueOf(10))).withLowerBound(0);
      conflictRetryCount = (new ConfigUtil.IntParamResolver("dsefs_options.transaction_options.conflict_retry_count", Integer.valueOf(40))).withLowerBound(0);
      executionRetryDelay = (new ConfigUtil.IntParamResolver("dsefs_options.transaction_options.execution_retry_delay_ms", Integer.valueOf(1000))).withLowerBound(0);
      executionRetryCount = (new ConfigUtil.IntParamResolver("dsefs_options.transaction_options.execution_retry_count", Integer.valueOf(3))).withLowerBound(0);
      gossipStartupDelay = (new ConfigUtil.IntParamResolver("dsefs_options.gossip_options.startup_delay_ms", Integer.valueOf(5000))).withLowerBound(0);
      gossipRoundDelay = (new ConfigUtil.IntParamResolver("dsefs_options.gossip_options.round_delay_ms", Integer.valueOf(2000))).withLowerBound(0);
      gossipShutdownDelay = (new ConfigUtil.IntParamResolver("dsefs_options.gossip_options.shutdown_delay_ms", Integer.valueOf(10000))).withLowerBound(0);
      restServerRequestTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.server_request_timeout_ms", Integer.valueOf(300000))).withLowerBound(0);
      idleConnectionTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.idle_connection_timeout_ms", Integer.valueOf('\uea60'))).withLowerBound(0);
      internodeIdleConnectionTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.internode_idle_connection_timeout_ms", Integer.valueOf(120000))).withLowerBound(0);
      restRequestTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.request_timeout_seconds_ms", Integer.valueOf(330000))).withLowerBound(0);
      restConnectionOpenTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.connection_open_timeout_ms", Integer.valueOf(10000))).withLowerBound(0);
      restClientCloseTimeout = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.client_close_timeout_ms", Integer.valueOf('\uea60'))).withLowerBound(0);
      coreMaxConcurrentConnectionsPerHost = (new ConfigUtil.IntParamResolver("dsefs_options.rest_options.core_max_concurrent_connections_per_host", Integer.valueOf(8))).withLowerBound(0);
      blockAllocatorOverflowMargin = (new ConfigUtil.IntParamResolver("dsefs_options.block_allocator_options.overflow_margin_mb", Integer.valueOf(1024))).withLowerBound(0);
      blockAllocatorOverflowFactor = (new ConfigUtil.DoubleParamResolver("dsefs_options.block_allocator_options.overflow_factor", Double.valueOf(1.05D))).withLowerBound(0.0D);

      try {
         enabled.withRawParam(config.dsefs_options.enabled).check();
         keyspaceName.withRawParam(config.dsefs_options.keyspace_name).check();
         workDirectory.withRawParam(config.dsefs_options.work_dir);
         privatePort.withRawParam(config.dsefs_options.private_port).check();
         publicPort.withRawParam(config.dsefs_options.public_port).check();
         dataDirectories.withRawParam(config.dsefs_options.data_directories).check();
         serviceStartupTimeout.withRawParam(config.dsefs_options.service_startup_timeout_ms).check();
         serviceCloseTimeout.withRawParam(config.dsefs_options.service_close_timeout_ms).check();
         serverCloseTimeout.withRawParam(config.dsefs_options.server_close_timeout_ms).check();
         compressionFrameMaxSize.withRawParam(config.dsefs_options.compression_frame_max_size).check();
         queryCacheSize.withRawParam(config.dsefs_options.query_cache_size).check();
         queryCacheExpireAfterMillis.withRawParam(config.dsefs_options.query_cache_expire_after_ms).check();
         if(config.dsefs_options.transaction_options != null) {
            transactionTimeout.withRawParam(config.dsefs_options.transaction_options.transaction_timeout_ms).check();
            conflictRetryDelay.withRawParam(config.dsefs_options.transaction_options.conflict_retry_delay_ms).check();
            conflictRetryCount.withRawParam(config.dsefs_options.transaction_options.conflict_retry_count).check();
            executionRetryDelay.withRawParam(config.dsefs_options.transaction_options.execution_retry_delay_ms).check();
            executionRetryCount.withRawParam(config.dsefs_options.transaction_options.execution_retry_count).check();
         }

         if(config.dsefs_options.gossip_options != null) {
            gossipStartupDelay.withRawParam(config.dsefs_options.gossip_options.startup_delay_ms).check();
            gossipRoundDelay.withRawParam(config.dsefs_options.gossip_options.round_delay_ms).check();
            gossipShutdownDelay.withRawParam(config.dsefs_options.gossip_options.shutdown_delay_ms).check();
         }

         if(config.dsefs_options.rest_options != null) {
            restServerRequestTimeout.withRawParam(config.dsefs_options.rest_options.server_request_timeout_ms).check();
            restRequestTimeout.withRawParam(config.dsefs_options.rest_options.request_timeout_ms).check();
            restConnectionOpenTimeout.withRawParam(config.dsefs_options.rest_options.connection_open_timeout_ms).check();
            idleConnectionTimeout.withRawParam(config.dsefs_options.rest_options.idle_connection_timeout_ms).check();
            internodeIdleConnectionTimeout.withRawParam(config.dsefs_options.rest_options.internode_idle_connection_timeout_ms).check();
            restClientCloseTimeout.withRawParam(config.dsefs_options.rest_options.client_close_timeout_ms).check();
            coreMaxConcurrentConnectionsPerHost.withRawParam(config.dsefs_options.rest_options.core_max_concurrent_connections_per_host).check();
         }

         if(config.dsefs_options.block_allocator_options != null) {
            blockAllocatorOverflowFactor.withRawParam(config.dsefs_options.block_allocator_options.overflow_factor).check();
            blockAllocatorOverflowMargin.withRawParam(config.dsefs_options.block_allocator_options.overflow_margin_mb).check();
         }

         enableResolvers(DseFsConfig.class);
      } catch (ConfigurationException var1) {
         throw new ExceptionInInitializerError(var1);
      }
   }
}
