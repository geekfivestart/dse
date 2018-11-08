package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompactionParams {
   private static final Logger logger = LoggerFactory.getLogger(CompactionParams.class);
   public static final int DEFAULT_MIN_THRESHOLD = 4;
   public static final int DEFAULT_MAX_THRESHOLD = 32;
   public static final boolean DEFAULT_ENABLED = true;
   public static final CompactionParams.TombstoneOption DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES;
   public static final Map<String, String> DEFAULT_THRESHOLDS;
   public static final CompactionParams DEFAULT;
   private final Class<? extends AbstractCompactionStrategy> klass;
   private final ImmutableMap<String, String> options;
   private final boolean isEnabled;
   private final CompactionParams.TombstoneOption tombstoneOption;

   private CompactionParams(Class<? extends AbstractCompactionStrategy> klass, Map<String, String> options, boolean isEnabled, CompactionParams.TombstoneOption tombstoneOption) {
      this.klass = klass;
      this.options = ImmutableMap.copyOf(options);
      this.isEnabled = isEnabled;
      this.tombstoneOption = tombstoneOption;
   }

   public static CompactionParams create(Class<? extends AbstractCompactionStrategy> klass, Map<String, String> options) {
      boolean isEnabled = options.containsKey(CompactionParams.Option.ENABLED.toString())?Boolean.parseBoolean((String)options.get(CompactionParams.Option.ENABLED.toString())):true;
      CompactionParams.TombstoneOption tombstoneOption = CompactionParams.TombstoneOption.valueOf(((String)options.getOrDefault(CompactionParams.Option.PROVIDE_OVERLAPPING_TOMBSTONES.toString(), DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES.toString())).toUpperCase());
      Map<String, String> allOptions = new HashMap(options);
      if(supportsThresholdParams(klass)) {
         allOptions.putIfAbsent(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(4));
         allOptions.putIfAbsent(CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(32));
      }

      return new CompactionParams(klass, allOptions, isEnabled, tombstoneOption);
   }

   public static CompactionParams scts(Map<String, String> options) {
      return create(SizeTieredCompactionStrategy.class, options);
   }

   public static CompactionParams lcs(Map<String, String> options) {
      return create(LeveledCompactionStrategy.class, options);
   }

   public int minCompactionThreshold() {
      String threshold = (String)this.options.get(CompactionParams.Option.MIN_THRESHOLD.toString());
      return threshold == null?4:Integer.parseInt(threshold);
   }

   public int maxCompactionThreshold() {
      String threshold = (String)this.options.get(CompactionParams.Option.MAX_THRESHOLD.toString());
      return threshold == null?32:Integer.parseInt(threshold);
   }

   public CompactionParams.TombstoneOption tombstoneOption() {
      return this.tombstoneOption;
   }

   public void validate() {
      try {
         Map<?, ?> unknownOptions = (Map)this.klass.getMethod("validateOptions", new Class[]{Map.class}).invoke((Object)null, new Object[]{this.options});
         if(!unknownOptions.isEmpty()) {
            throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", new Object[]{unknownOptions.keySet(), this.klass.getSimpleName()}));
         }
      } catch (NoSuchMethodException var3) {
         logger.warn("Compaction strategy {} does not have a static validateOptions method. Validation ignored", this.klass.getName());
      } catch (InvocationTargetException var4) {
         if(var4.getTargetException() instanceof ConfigurationException) {
            throw (ConfigurationException)var4.getTargetException();
         }

         Throwable cause = var4.getCause() == null?var4:var4.getCause();
         throw new ConfigurationException(String.format("%s.validateOptions() threw an error: %s %s", new Object[]{this.klass.getName(), cause.getClass().getName(), ((Throwable)cause).getMessage()}), var4);
      } catch (IllegalAccessException var5) {
         throw new ConfigurationException("Cannot access method validateOptions in " + this.klass.getName(), var5);
      }

      String minThreshold = (String)this.options.get(CompactionParams.Option.MIN_THRESHOLD.toString());
      if(minThreshold != null && !StringUtils.isNumeric(minThreshold)) {
         throw new ConfigurationException(String.format("Invalid value %s for '%s' compaction sub-option - must be an integer", new Object[]{minThreshold, CompactionParams.Option.MIN_THRESHOLD}));
      } else {
         String maxThreshold = (String)this.options.get(CompactionParams.Option.MAX_THRESHOLD.toString());
         if(maxThreshold != null && !StringUtils.isNumeric(maxThreshold)) {
            throw new ConfigurationException(String.format("Invalid value %s for '%s' compaction sub-option - must be an integer", new Object[]{maxThreshold, CompactionParams.Option.MAX_THRESHOLD}));
         } else if(this.minCompactionThreshold() > 0 && this.maxCompactionThreshold() > 0) {
            if(this.minCompactionThreshold() <= 1) {
               throw new ConfigurationException(String.format("Min compaction threshold cannot be less than 2 (got %d)", new Object[]{Integer.valueOf(this.minCompactionThreshold())}));
            } else if(this.minCompactionThreshold() > this.maxCompactionThreshold()) {
               throw new ConfigurationException(String.format("Min compaction threshold (got %d) cannot be greater than max compaction threshold (got %d)", new Object[]{Integer.valueOf(this.minCompactionThreshold()), Integer.valueOf(this.maxCompactionThreshold())}));
            }
         } else {
            throw new ConfigurationException("Disabling compaction by setting compaction thresholds to 0 has been removed, set the compaction option 'enabled' to false instead.");
         }
      }
   }

   double defaultBloomFilterFbChance() {
      return this.klass.equals(LeveledCompactionStrategy.class)?0.1D:0.01D;
   }

   public Class<? extends AbstractCompactionStrategy> klass() {
      return this.klass;
   }

   public Map<String, String> options() {
      return this.options;
   }

   public boolean isEnabled() {
      return this.isEnabled;
   }

   public static CompactionParams fromMap(Map<String, String> map) {
      Map<String, String> options = new HashMap(map);
      String className = (String)options.remove(CompactionParams.Option.CLASS.toString());
      if(className == null) {
         throw new ConfigurationException(String.format("Missing sub-option '%s' for the '%s' option", new Object[]{CompactionParams.Option.CLASS, TableParams.Option.COMPACTION}));
      } else {
         return create(classFromName(className), options);
      }
   }

   public static Class<? extends AbstractCompactionStrategy> classFromName(String name) {
      String className = name.contains(".")?name:"org.apache.cassandra.db.compaction." + name;
      Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
      if(!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass)) {
         throw new ConfigurationException(String.format("Compaction strategy class %s is not derived from AbstractReplicationStrategy", new Object[]{className}));
      } else {
         return strategyClass;
      }
   }

   public static boolean supportsThresholdParams(Class<? extends AbstractCompactionStrategy> klass) {
      try {
         Map<String, String> unrecognizedOptions = (Map)klass.getMethod("validateOptions", new Class[]{Map.class}).invoke((Object)null, new Object[]{DEFAULT_THRESHOLDS});
         return unrecognizedOptions.isEmpty();
      } catch (Exception var2) {
         throw Throwables.cleaned(var2);
      }
   }

   public Map<String, String> asMap() {
      Map<String, String> map = new HashMap(this.options());
      map.put(CompactionParams.Option.CLASS.toString(), this.klass.getName());
      return map;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("class", this.klass.getName()).add("options", this.options).toString();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof CompactionParams)) {
         return false;
      } else {
         CompactionParams cp = (CompactionParams)o;
         return this.klass.equals(cp.klass) && this.options.equals(cp.options);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.klass, this.options});
   }

   static {
      DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES = CompactionParams.TombstoneOption.valueOf(System.getProperty("default.provide.overlapping.tombstones", CompactionParams.TombstoneOption.NONE.toString()).toUpperCase());
      DEFAULT_THRESHOLDS = ImmutableMap.of(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(4), CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(32));
      DEFAULT = new CompactionParams(SizeTieredCompactionStrategy.class, DEFAULT_THRESHOLDS, true, DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES);
   }

   public static enum TombstoneOption {
      NONE,
      ROW,
      CELL;

      private TombstoneOption() {
      }
   }

   public static enum Option {
      CLASS,
      ENABLED,
      MIN_THRESHOLD,
      MAX_THRESHOLD,
      PROVIDE_OVERLAPPING_TOMBSTONES;

      private Option() {
      }

      public String toString() {
         return this.name().toLowerCase();
      }
   }
}
