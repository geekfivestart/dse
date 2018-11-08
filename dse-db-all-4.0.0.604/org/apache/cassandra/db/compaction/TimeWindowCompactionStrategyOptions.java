package org.apache.cassandra.db.compaction;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimeWindowCompactionStrategyOptions {
   private static final Logger logger = LoggerFactory.getLogger(TimeWindowCompactionStrategyOptions.class);
   protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION;
   protected static final TimeUnit DEFAULT_COMPACTION_WINDOW_UNIT;
   protected static final int DEFAULT_COMPACTION_WINDOW_SIZE = 1;
   protected static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 600;
   protected static final boolean DEFAULT_SPLIT_DURING_FLUSH = false;
   protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
   protected static final String COMPACTION_WINDOW_UNIT_KEY = "compaction_window_unit";
   protected static final String COMPACTION_WINDOW_SIZE_KEY = "compaction_window_size";
   protected static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
   protected static final String SPLIT_DURING_FLUSH = "split_during_flush";
   protected static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_TABLE_OPTION = "unsafe_aggressive_sstable_expiration";
   protected static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY = "cassandra.allow_unsafe_aggressive_sstable_expiration";
   protected static final boolean ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION;
   protected final int sstableWindowSize;
   protected final TimeUnit sstableWindowUnit;
   protected final TimeUnit timestampResolution;
   protected final long expiredSSTableCheckFrequency;
   protected final boolean splitDuringFlush;
   protected final boolean ignoreOverlaps;
   SizeTieredCompactionStrategyOptions stcsOptions;
   protected static final ImmutableList<TimeUnit> validTimestampTimeUnits;
   protected static final ImmutableList<TimeUnit> validWindowTimeUnits;

   public TimeWindowCompactionStrategyOptions(Map<String, String> options) {
      String optionValue = (String)options.get("timestamp_resolution");
      this.timestampResolution = optionValue == null?DEFAULT_TIMESTAMP_RESOLUTION:TimeUnit.valueOf(optionValue);
      if(this.timestampResolution != DEFAULT_TIMESTAMP_RESOLUTION) {
         logger.warn("Using a non-default timestamp_resolution {} - are you really doing inserts with USING TIMESTAMP <non_microsecond_timestamp> (or driver equivalent)?", this.timestampResolution);
      }

      optionValue = (String)options.get("compaction_window_unit");
      this.sstableWindowUnit = optionValue == null?DEFAULT_COMPACTION_WINDOW_UNIT:TimeUnit.valueOf(optionValue);
      optionValue = (String)options.get("compaction_window_size");
      this.sstableWindowSize = optionValue == null?1:Integer.parseInt(optionValue);
      optionValue = (String)options.get("expired_sstable_check_frequency_seconds");
      this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(optionValue == null?600L:Long.parseLong(optionValue), TimeUnit.SECONDS);
      optionValue = (String)options.get("split_during_flush");
      this.splitDuringFlush = optionValue == null?false:Boolean.parseBoolean(optionValue);
      optionValue = (String)options.get("unsafe_aggressive_sstable_expiration");
      boolean aggressiveExpirationTableOption = Boolean.parseBoolean(optionValue);
      if(aggressiveExpirationTableOption && !ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION) {
         logger.warn("Not enabling agressive SSTable expiration, as the system property 'cassandra.allow_unsafe_aggressive_sstable_expiration' is set to 'false'. Set it to 'true' to enable aggressive SSTable expiration.");
      }

      this.ignoreOverlaps = ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION && aggressiveExpirationTableOption;
      this.stcsOptions = new SizeTieredCompactionStrategyOptions(options);
   }

   public TimeWindowCompactionStrategyOptions() {
      this.sstableWindowUnit = DEFAULT_COMPACTION_WINDOW_UNIT;
      this.timestampResolution = DEFAULT_TIMESTAMP_RESOLUTION;
      this.sstableWindowSize = 1;
      this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(600L, TimeUnit.SECONDS);
      this.splitDuringFlush = false;
      this.ignoreOverlaps = false;
      this.stcsOptions = new SizeTieredCompactionStrategyOptions();
   }

   public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException {
      String optionValue = (String)options.get("timestamp_resolution");

      try {
         if(optionValue != null && !validTimestampTimeUnits.contains(TimeUnit.valueOf(optionValue))) {
            throw new ConfigurationException(String.format("%s is not valid for %s", new Object[]{optionValue, "timestamp_resolution"}));
         }
      } catch (IllegalArgumentException var8) {
         throw new ConfigurationException(String.format("%s is not valid for %s", new Object[]{optionValue, "timestamp_resolution"}));
      }

      optionValue = (String)options.get("compaction_window_unit");

      try {
         if(optionValue != null && !validWindowTimeUnits.contains(TimeUnit.valueOf(optionValue))) {
            throw new ConfigurationException(String.format("%s is not valid for %s", new Object[]{optionValue, "compaction_window_unit"}));
         }
      } catch (IllegalArgumentException var7) {
         throw new ConfigurationException(String.format("%s is not valid for %s", new Object[]{optionValue, "compaction_window_unit"}), var7);
      }

      optionValue = (String)options.get("compaction_window_size");

      try {
         int sstableWindowSize = optionValue == null?1:Integer.parseInt(optionValue);
         if(sstableWindowSize < 1) {
            throw new ConfigurationException(String.format("%d must be greater than 1 for %s", new Object[]{Integer.valueOf(sstableWindowSize), "compaction_window_size"}));
         }
      } catch (NumberFormatException var6) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "compaction_window_size"}), var6);
      }

      optionValue = (String)options.get("expired_sstable_check_frequency_seconds");

      try {
         long expiredCheckFrequency = optionValue == null?600L:Long.parseLong(optionValue);
         if(expiredCheckFrequency < 0L) {
            throw new ConfigurationException(String.format("%s must not be negative, but was %d", new Object[]{"expired_sstable_check_frequency_seconds", Long.valueOf(expiredCheckFrequency)}));
         }
      } catch (NumberFormatException var5) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "expired_sstable_check_frequency_seconds"}), var5);
      }

      optionValue = (String)options.get("unsafe_aggressive_sstable_expiration");
      if(optionValue != null && !optionValue.equalsIgnoreCase("true") && !optionValue.equalsIgnoreCase("false")) {
         throw new ConfigurationException(String.format("%s is not 'true' or 'false' (%s)", new Object[]{"unsafe_aggressive_sstable_expiration", optionValue}));
      } else {
         uncheckedOptions.remove("compaction_window_size");
         uncheckedOptions.remove("compaction_window_unit");
         uncheckedOptions.remove("timestamp_resolution");
         uncheckedOptions.remove("expired_sstable_check_frequency_seconds");
         uncheckedOptions.remove("split_during_flush");
         uncheckedOptions.remove("unsafe_aggressive_sstable_expiration");
         uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
         return uncheckedOptions;
      }
   }

   static {
      DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
      DEFAULT_COMPACTION_WINDOW_UNIT = TimeUnit.DAYS;
      ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = Boolean.getBoolean("cassandra.allow_unsafe_aggressive_sstable_expiration");
      validTimestampTimeUnits = ImmutableList.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS);
      validWindowTimeUnits = ImmutableList.of(TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS);
   }
}
