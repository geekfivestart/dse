package org.apache.cassandra.db.compaction;

import java.util.Map;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class SizeTieredCompactionStrategyOptions {
   protected static final long DEFAULT_MIN_SSTABLE_SIZE = 52428800L;
   protected static final double DEFAULT_BUCKET_LOW = 0.5D;
   protected static final double DEFAULT_BUCKET_HIGH = 1.5D;
   protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
   protected static final String BUCKET_LOW_KEY = "bucket_low";
   protected static final String BUCKET_HIGH_KEY = "bucket_high";
   protected long minSSTableSize;
   protected double bucketLow;
   protected double bucketHigh;

   public SizeTieredCompactionStrategyOptions(Map<String, String> options) {
      String optionValue = (String)options.get("min_sstable_size");
      this.minSSTableSize = optionValue == null?52428800L:Long.parseLong(optionValue);
      optionValue = (String)options.get("bucket_low");
      this.bucketLow = optionValue == null?0.5D:Double.parseDouble(optionValue);
      optionValue = (String)options.get("bucket_high");
      this.bucketHigh = optionValue == null?1.5D:Double.parseDouble(optionValue);
   }

   public SizeTieredCompactionStrategyOptions() {
      this.minSSTableSize = 52428800L;
      this.bucketLow = 0.5D;
      this.bucketHigh = 1.5D;
   }

   private static double parseDouble(Map<String, String> options, String key, double defaultValue) throws ConfigurationException {
      String optionValue = (String)options.get(key);

      try {
         return optionValue == null?defaultValue:Double.parseDouble(optionValue);
      } catch (NumberFormatException var6) {
         throw new ConfigurationException(String.format("%s is not a parsable float for %s", new Object[]{optionValue, key}), var6);
      }
   }

   public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException {
      String optionValue = (String)options.get("min_sstable_size");

      try {
         long minSSTableSize = optionValue == null?52428800L:Long.parseLong(optionValue);
         if(minSSTableSize < 0L) {
            throw new ConfigurationException(String.format("%s must be non negative: %d", new Object[]{"min_sstable_size", Long.valueOf(minSSTableSize)}));
         }
      } catch (NumberFormatException var7) {
         throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{optionValue, "min_sstable_size"}), var7);
      }

      double bucketLow = parseDouble(options, "bucket_low", 0.5D);
      double bucketHigh = parseDouble(options, "bucket_high", 1.5D);
      if(bucketHigh <= bucketLow) {
         throw new ConfigurationException(String.format("%s value (%s) is less than or equal to the %s value (%s)", new Object[]{"bucket_high", Double.valueOf(bucketHigh), "bucket_low", Double.valueOf(bucketLow)}));
      } else {
         uncheckedOptions.remove("min_sstable_size");
         uncheckedOptions.remove("bucket_low");
         uncheckedOptions.remove("bucket_high");
         return uncheckedOptions;
      }
   }
}
