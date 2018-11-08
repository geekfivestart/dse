package com.datastax.bdp.config;

import com.datastax.bdp.server.SystemInfo;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysOnSqlConfig extends DseConfigYamlLoader {
   public static final int DEFAULT_THRIFT_PORT = 10000;
   public static final int DEFAULT_WEB_UI_PORT = 9077;
   public static final int DEFAULT_RESERVE_PORT_WAIT_TIME_MS = 100;
   public static final int DEFAULT_ALWAYSON_SQL_STATUS_CHECK_WAIT_TIME_MS = 500;
   public static final String DEFAULT_ALWAYSON_SQL_LOG_DSEFS_DIR = "/spark/log/alwayson_sql";
   public static final String DEFAULT_ALWAYSON_SQL_WORKPOOL = "alwayson_sql";
   public static final String DEFAULT_AUTH_USER = "alwayson_sql";
   public static final int DEFAULT_RUNNER_MAX_ERRORS = 10;
   private static final Logger logger = LoggerFactory.getLogger(AlwaysOnSqlConfig.class);
   private static final ConfigUtil.BooleanParamResolver enabled = new ConfigUtil.BooleanParamResolver("alwayson_sql_options.enabled", Boolean.valueOf(false));
   private static final ConfigUtil.IntParamResolver thriftPort = new ConfigUtil.IntParamResolver("alwayson_sql_options.thrift_port", Integer.valueOf(10000));
   private static final ConfigUtil.IntParamResolver webUIPort = new ConfigUtil.IntParamResolver("alwayson_sql_options.web_ui_port", Integer.valueOf(9077));
   private static final ConfigUtil.IntParamResolver reservePortWaitTime = (new ConfigUtil.IntParamResolver("alwayson_sql_options.reserve_port_wait_time_ms", Integer.valueOf(100))).withLowerBound(0);
   private static final ConfigUtil.IntParamResolver alwaysOnSqlStatusCheckWaitTime = (new ConfigUtil.IntParamResolver("alwayson_sql_options.alwayson_sql_status_check_wait_time_ms", Integer.valueOf(500))).withLowerBound(0);
   private static final ConfigUtil.StringParamResolver logDseFsDir = new ConfigUtil.StringParamResolver("alwayson_sql_options.log_dsefs_dir", "/spark/log/alwayson_sql");
   private static final ConfigUtil.StringParamResolver workPool = new ConfigUtil.StringParamResolver("alwayson_sql_options.workpool", "alwayson_sql");
   private static final ConfigUtil.StringParamResolver authUser = new ConfigUtil.StringParamResolver("alwayson_sql_options.auth_user", "alwayson_sql");
   private static final ConfigUtil.IntParamResolver runnerMaxErrors = (new ConfigUtil.IntParamResolver("alwayson_sql_options.runner_max_errors", Integer.valueOf(10))).withLowerBound(1);

   public AlwaysOnSqlConfig() {
   }

   public static Boolean isEnabled() {
      return Boolean.valueOf(SystemInfo.isSparkNode() && ((Boolean)enabled.get()).booleanValue());
   }

   public static int getThriftPort() {
      return ((Integer)thriftPort.get()).intValue();
   }

   public static int getWebUIPort() {
      return ((Integer)webUIPort.get()).intValue();
   }

   public static int getReservePortWaitTime() {
      return ((Integer)reservePortWaitTime.get()).intValue();
   }

   public static int getAlwaysOnSqlStatusCheckWaitTime() {
      return ((Integer)alwaysOnSqlStatusCheckWaitTime.get()).intValue();
   }

   public static String getLogDseFsDir() {
      return (String)logDseFsDir.get();
   }

   public static String getWorkPool() {
      return (String)workPool.get();
   }

   public static String getAuthUser() {
      return (String)authUser.get();
   }

   public static int getRunnerMaxErrors() {
      return ((Integer)runnerMaxErrors.get()).intValue();
   }

   static {
      try {
         enabled.withRawParam(config.alwayson_sql_options.enabled).check();
         thriftPort.withRawParam(config.alwayson_sql_options.thrift_port).check();
         webUIPort.withRawParam(config.alwayson_sql_options.web_ui_port).check();
         reservePortWaitTime.withRawParam(config.alwayson_sql_options.reserve_port_wait_time_ms).check();
         alwaysOnSqlStatusCheckWaitTime.withRawParam(config.alwayson_sql_options.alwayson_sql_status_check_wait_time_ms).check();
         logDseFsDir.withRawParam(config.alwayson_sql_options.log_dsefs_dir).check();
         workPool.withRawParam(config.alwayson_sql_options.workpool).check();
         authUser.withRawParam(config.alwayson_sql_options.auth_user).check();
         runnerMaxErrors.withRawParam(config.alwayson_sql_options.runner_max_errors).check();
         enableResolvers(AlwaysOnSqlConfig.class);
         if(!SystemInfo.isSparkNode() && ((Boolean)enabled.get()).booleanValue()) {
            throw new RuntimeException("alwayson_sql_options.enabled in dse.yaml is set to true in a non-analytics node");
         }
      } catch (ConfigurationException var1) {
         throw new ExceptionInInitializerError(var1);
      }
   }
}
