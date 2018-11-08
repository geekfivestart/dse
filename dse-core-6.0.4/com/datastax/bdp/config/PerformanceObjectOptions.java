package com.datastax.bdp.config;

public class PerformanceObjectOptions {
   public boolean enabled;
   /** @deprecated */
   @Deprecated
   public String async_writers;
   public String ttl_seconds;

   public PerformanceObjectOptions() {
   }

   public static class UserLevelTrackingOptions extends PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions {
      public String top_stats_limit;
      public String backpressure_threshold;
      public String flush_timeout_ms;
      public boolean quantiles;

      public UserLevelTrackingOptions() {
      }
   }

   public static class HistogramDataTablesOptions extends PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions {
      public String retention_count;

      public HistogramDataTablesOptions() {
      }
   }

   public static class PeriodicallyUpdatedStatsOptions extends PerformanceObjectOptions {
      public String refresh_rate_ms;

      public PeriodicallyUpdatedStatsOptions() {
      }

      public static class SparkAppInfoOptions extends PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions {
         public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions.DriverOptions driver = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions.DriverOptions();
         public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions.ExecutorOptions executor = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions.ExecutorOptions();

         public SparkAppInfoOptions() {
         }

         public static class ExecutorOptions {
            public boolean sink;
            public boolean connectorSource;
            public boolean jvmSource;

            public ExecutorOptions() {
            }
         }

         public static class DriverOptions {
            public boolean sink;
            public boolean connectorSource;
            public boolean jvmSource;
            public boolean stateSource;

            public DriverOptions() {
            }
         }
      }
   }

   public static class ThresholdPerformanceObjectOptions extends PerformanceObjectOptions {
      public String threshold_ms;

      public ThresholdPerformanceObjectOptions() {
      }
   }
}
