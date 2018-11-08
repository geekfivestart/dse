package org.apache.cassandra.db.compaction;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DateTieredCompactionStrategyOptions {
   private static final Logger logger = LoggerFactory.getLogger(DateTieredCompactionStrategyOptions.class);
   protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION;
   /** @deprecated */
   @Deprecated
   protected static final double DEFAULT_MAX_SSTABLE_AGE_DAYS = 365000.0D;
   protected static final long DEFAULT_BASE_TIME_SECONDS = 60L;
   protected static final long DEFAULT_MAX_WINDOW_SIZE_SECONDS;
   protected static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 600;
   protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
   /** @deprecated */
   @Deprecated
   protected static final String MAX_SSTABLE_AGE_KEY = "max_sstable_age_days";
   protected static final String BASE_TIME_KEY = "base_time_seconds";
   protected static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
   protected static final String MAX_WINDOW_SIZE_KEY = "max_window_size_seconds";
   /** @deprecated */
   @Deprecated
   protected final long maxSSTableAge;
   protected final TimeUnit timestampResolution;
   protected final long baseTime;
   protected final long expiredSSTableCheckFrequency;
   protected final long maxWindowSize;

   public DateTieredCompactionStrategyOptions(Map<String, String> options) {
      String optionValue = (String)options.get("timestamp_resolution");
      this.timestampResolution = optionValue == null?DEFAULT_TIMESTAMP_RESOLUTION:TimeUnit.valueOf(optionValue);
      if(this.timestampResolution != DEFAULT_TIMESTAMP_RESOLUTION) {
         logger.warn("Using a non-default timestamp_resolution {} - are you really doing inserts with USING TIMESTAMP <non_microsecond_timestamp> (or driver equivalent)?", this.timestampResolution);
      }

      optionValue = (String)options.get("max_sstable_age_days");
      double fractionalDays = optionValue == null?365000.0D:Double.parseDouble(optionValue);
      this.maxSSTableAge = Math.round(fractionalDays * (double)this.timestampResolution.convert(1L, TimeUnit.DAYS));
      optionValue = (String)options.get("base_time_seconds");
      this.baseTime = this.timestampResolution.convert(optionValue == null?60L:Long.parseLong(optionValue), TimeUnit.SECONDS);
      optionValue = (String)options.get("expired_sstable_check_frequency_seconds");
      this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(optionValue == null?600L:Long.parseLong(optionValue), TimeUnit.SECONDS);
      optionValue = (String)options.get("max_window_size_seconds");
      this.maxWindowSize = this.timestampResolution.convert(optionValue == null?DEFAULT_MAX_WINDOW_SIZE_SECONDS:Long.parseLong(optionValue), TimeUnit.SECONDS);
   }

   public DateTieredCompactionStrategyOptions() {
      this.maxSSTableAge = Math.round(365000.0D * (double)DEFAULT_TIMESTAMP_RESOLUTION.convert(365000L, TimeUnit.DAYS));
      this.timestampResolution = DEFAULT_TIMESTAMP_RESOLUTION;
      this.baseTime = this.timestampResolution.convert(60L, TimeUnit.SECONDS);
      this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(600L, TimeUnit.SECONDS);
      this.maxWindowSize = this.timestampResolution.convert(1L, TimeUnit.DAYS);
   }

   public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException {
      String optionValue = (String)options.get("timestamp_resolution");

      try {
         if(optionValue != null) {
            TimeUnit.valueOf(optionValue);
         }
      } catch (IllegalArgumentException var9) {
         throw new ConfigurationException(String.format("timestamp_resolution %s is not valid", new Object[]{optionValue}));
      }

      optionValue = (String)options.get("max_sstable_age_days");

      try {
         double maxSStableAge = optionValue == null?365000.0D:Double.parseDouble(optionValue);
         if(maxSStableAge < 0.0D) {
            throw new ConfigurationException(String.format("%s must be non-negative: %.2f", new Object[]{"max_sstable_age_days", Double.valueOf(maxSStableAge)}));
         }
      } catch (NumberFormatException var8) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "max_sstable_age_days"}), var8);
      }

      optionValue = (String)options.get("base_time_seconds");

      long maxWindowSize;
      try {
         maxWindowSize = optionValue == null?60L:Long.parseLong(optionValue);
         if(maxWindowSize <= 0L) {
            throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", new Object[]{"base_time_seconds", Long.valueOf(maxWindowSize)}));
         }
      } catch (NumberFormatException var7) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "base_time_seconds"}), var7);
      }

      optionValue = (String)options.get("expired_sstable_check_frequency_seconds");

      try {
         maxWindowSize = optionValue == null?600L:Long.parseLong(optionValue);
         if(maxWindowSize < 0L) {
            throw new ConfigurationException(String.format("%s must not be negative, but was %d", new Object[]{"expired_sstable_check_frequency_seconds", Long.valueOf(maxWindowSize)}));
         }
      } catch (NumberFormatException var6) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "expired_sstable_check_frequency_seconds"}), var6);
      }

      optionValue = (String)options.get("max_window_size_seconds");

      try {
         maxWindowSize = optionValue == null?DEFAULT_MAX_WINDOW_SIZE_SECONDS:Long.parseLong(optionValue);
         if(maxWindowSize < 0L) {
            throw new ConfigurationException(String.format("%s must not be negative, but was %d", new Object[]{"max_window_size_seconds", Long.valueOf(maxWindowSize)}));
         }
      } catch (NumberFormatException var5) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "max_window_size_seconds"}), var5);
      }

      uncheckedOptions.remove("max_sstable_age_days");
      uncheckedOptions.remove("base_time_seconds");
      uncheckedOptions.remove("timestamp_resolution");
      uncheckedOptions.remove("expired_sstable_check_frequency_seconds");
      uncheckedOptions.remove("max_window_size_seconds");
      return uncheckedOptions;
   }

   static {
      DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
      DEFAULT_MAX_WINDOW_SIZE_SECONDS = TimeUnit.SECONDS.convert(1L, TimeUnit.DAYS);
   }
}
