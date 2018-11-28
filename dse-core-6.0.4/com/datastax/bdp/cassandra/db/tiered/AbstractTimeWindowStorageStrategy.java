package com.datastax.bdp.cassandra.db.tiered;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTimeWindowStorageStrategy extends TieredStorageStrategy {
   private static final Logger logger = LoggerFactory.getLogger(AbstractTimeWindowStorageStrategy.class);
   static final String TIMESTAMP_RESOLUTION = "timestamp_resolution";
   static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION;
   static final String MAX_TIER_AGES = "max_tier_ages";
   final TimeUnit resolution;

   public AbstractTimeWindowStorageStrategy(ColumnFamilyStore cfs, String name, TieredStorageConfig config, Map<String, String> options) {
      super(cfs, name, config, options);
      this.resolution = options.containsKey("timestamp_resolution")?TimeUnit.valueOf((String)options.get("timestamp_resolution")):DEFAULT_TIMESTAMP_RESOLUTION;
   }

   protected static long[] getMaxAges(Map<String, String> options) {
      String[] stringAges = ((String)options.get("max_tier_ages")).split(",");
      long[] longAges = new long[stringAges.length];

      for(int i = 0; i < stringAges.length; ++i) {
         longAges[i] = Long.parseLong(stringAges[i].trim());
      }

      return longAges;
   }

   protected abstract List<TieredStorageStrategy.Tier> createTiers(TieredStorageConfig var1, Map<String, String> var2);

   protected TieredStorageStrategy.Context newContext() {
      long n = this.now();
      logger.debug("Creating context with ts: {}", Long.valueOf(n));
      return new AbstractTimeWindowStorageStrategy.TimeWindowContext(n);
   }

   protected long now() {
      return this.resolution.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
   }

   @VisibleForTesting
   static long getMinTimestamp(Unfiltered unfiltered) {
      if(unfiltered.kind() != Kind.ROW) {
         throw new IllegalArgumentException("Unsupported Unfiltered kind: " + unfiltered.kind());
      } else if(!(unfiltered instanceof Row)) {
         throw new IllegalArgumentException("Unsupported Unfiltered type: " + unfiltered.getClass().getName());
      } else {
         Row row = (Row)unfiltered;
         LivenessInfo liveness = row.primaryKeyLivenessInfo();
         long livenessTs = liveness.isEmpty()?9223372036854775807L:liveness.timestamp();
         Deletion deletion = row.deletion();
         long deletionTs = deletion.isLive()?9223372036854775807L:deletion.time().markedForDeleteAt();
         long minTimestamp = Math.min(livenessTs, deletionTs);
         Iterator var10 = row.iterator();

         while(true) {
            while(var10.hasNext()) {
               ColumnData cd = (ColumnData)var10.next();
               if(cd instanceof Cell) {
                  Cell cell = (Cell)cd;
                  minTimestamp = Math.min(minTimestamp, cell.timestamp());
               } else if(cd instanceof ComplexColumnData) {
                  ComplexColumnData ccd = (ComplexColumnData)cd;
                  Cell cell;
                  if(ccd.hasCells()) {
                     for(Iterator var13 = ccd.iterator(); var13.hasNext(); minTimestamp = Math.min(minTimestamp, cell.timestamp())) {
                        cell = (Cell)var13.next();
                     }
                  }

                  DeletionTime complexDeletion = ccd.complexDeletion();
                  if(!complexDeletion.isLive()) {
                     minTimestamp = Math.min(minTimestamp, complexDeletion.markedForDeleteAt());
                  }
               }
            }

            return minTimestamp;
         }
      }
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      if(!options.containsKey("max_tier_ages")) {
         throw new ConfigurationException(String.format("%s not specified", new Object[]{"max_tier_ages"}));
      } else {
         long[] maxAges;
         try {
            maxAges = getMaxAges(options);
         } catch (NumberFormatException var9) {
            throw new ConfigurationException(String.format("Invalid max ages value %s (%s)", new Object[]{options.get("max_tier_ages"), var9.getMessage()}));
         }

         if(maxAges.length < 1) {
            throw new ConfigurationException(String.format("at least one ages must be specified for %s", new Object[]{"max_tier_ages"}));
         } else {
            try {
               if(options.containsKey("timestamp_resolution")) {
                  TimeUnit.valueOf((String)options.get("timestamp_resolution"));
               }
            } catch (IllegalArgumentException var10) {
               throw new ConfigurationException(String.format("timestamp_resolution %s is not valid", new Object[]{options.get("timestamp_resolution")}));
            }

            long lastAge = 0L;
            long[] var4 = maxAges;
            int var5 = maxAges.length;

            for(int var6 = 0; var6 < var5; ++var6) {
               long maxAge = var4[var6];
               if(maxAge < 1L) {
                  throw new ConfigurationException(String.format("all max ages specified for %s must be greater than 0", new Object[]{"max_tier_ages"}));
               }

               if(maxAge <= lastAge) {
                  throw new ConfigurationException(String.format("Each tier age must be higher than the last (%s <= %s)", new Object[]{Long.valueOf(maxAge), Long.valueOf(lastAge)}));
               }

               lastAge = maxAge;
            }

            Map<String, String> unchecked = new HashMap(options);
            unchecked.remove("max_tier_ages");
            unchecked.remove("timestamp_resolution");
            return unchecked;
         }
      }
   }

   static {
      DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
   }

   class Tier extends TieredStorageStrategy.Tier {
      private final long maxAge;

      public Tier(int level, TieredStorageConfig.Tier config, Map<String, String> options, long maxAge) {
         super(level, config, options);
         this.maxAge = maxAge;
      }

      public boolean applies(Unfiltered row, TieredStorageStrategy.Context ctx) {
         long age = ((AbstractTimeWindowStorageStrategy.TimeWindowContext)ctx).now - AbstractTimeWindowStorageStrategy.getMinTimestamp(row);
         if(AbstractTimeWindowStorageStrategy.logger.isDebugEnabled()) {
            AbstractTimeWindowStorageStrategy.logger.debug("checking age of {}. {} should be <= {}", new Object[]{row.toString(AbstractTimeWindowStorageStrategy.this.cfs.metadata()), Long.valueOf(TimeUnit.SECONDS.convert(age, AbstractTimeWindowStorageStrategy.this.resolution)), Long.valueOf(TimeUnit.SECONDS.convert(this.maxAge, AbstractTimeWindowStorageStrategy.this.resolution))});
         }

         return age <= this.maxAge;
      }

      public boolean applies(DeletionTime deletion, TieredStorageStrategy.Context ctx) {
         long age = ((AbstractTimeWindowStorageStrategy.TimeWindowContext)ctx).now - deletion.markedForDeleteAt();
         return age <= this.maxAge;
      }

      protected Class<? extends AbstractCompactionStrategy> getDefaultCompactionClass() {
         throw new UnsupportedOperationException("Subclasses must override!");
      }

      protected Map<String, String> cleanCompactionOptions(Map<String, String> options) {
         Map<String, String> copy = new HashMap(super.cleanCompactionOptions(options));
         copy.remove("max_tier_ages");
         return copy;
      }
   }

   static class TimeWindowContext implements TieredStorageStrategy.Context {
      private final long now;

      public TimeWindowContext(long now) {
         this.now = now;
      }
   }
}
