package com.datastax.bdp.cassandra.db.tiered;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeWindowStorageStrategy extends AbstractTimeWindowStorageStrategy {
   private static final Logger logger = LoggerFactory.getLogger(TimeWindowStorageStrategy.class);

   public TimeWindowStorageStrategy(ColumnFamilyStore cfs, String name, TieredStorageConfig config, Map<String, String> options) {
      super(cfs, name, config, options);
   }

   protected List<TieredStorageStrategy.Tier> createTiers(TieredStorageConfig config, Map<String, String> options) {
      List<TieredStorageStrategy.Tier> tiers = new ArrayList(config.tiers.size());
      TimeUnit resolution = options.containsKey("timestamp_resolution")?TimeUnit.valueOf((String)options.get("timestamp_resolution")):DEFAULT_TIMESTAMP_RESOLUTION;
      long[] maxAges = getMaxAges(options);
      int numTiers = Math.min(maxAges.length + 1, config.tiers.size());

      for(int level = 0; level < numTiers; ++level) {
         long maxAge = level < maxAges.length?resolution.convert(maxAges[level], TimeUnit.SECONDS):9223372036854775807L;
         tiers.add(new TimeWindowStorageStrategy.Tier(level, (TieredStorageConfig.Tier)config.tiers.get(level), options, maxAge));
      }

      return tiers;
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      Map<String, String> unchecked = AbstractTimeWindowStorageStrategy.validateOptions(options);
      return TimeWindowCompactionStrategyOptions.validateOptions(options, unchecked);
   }

   class Tier extends AbstractTimeWindowStorageStrategy.Tier {
      public Tier(int level, TieredStorageConfig.Tier config, Map<String, String> options, long maxAge) {
         super(level, config, options, maxAge);
      }

      protected Class<? extends AbstractCompactionStrategy> getDefaultCompactionClass() {
         return TimeWindowCompactionStrategy.class;
      }
   }
}
