package org.apache.cassandra.db.mos;

import java.util.Map;
import org.apache.cassandra.exceptions.ConfigurationException;

public class MemoryOnlyStrategyOptions {
   public static final String MOS_MINCOMPACTIONTHRESHOLD = "mos_min_threshold";
   public static final String MOS_MAXCOMPACTIONTHRESHOLD = "mos_max_threshold";
   public static final String MOS_MAXACTIVECOMPACTIONS = "mos_max_active_compactions";
   public static final int MOS_DEFAULT_MINCOMPACTIONTHRESHOLD = 2;
   public static final int MOS_DEFAULT_MAXCOMPACTIONTHRESHOLD = 32;
   public static final int MOS_DEFAULT_MAXACTIVECOMPACTIONS = 100;
   public final int minCompactionThreshold;
   public final int maxCompactionThreshold;
   public final int maxActiveCompactions;

   public MemoryOnlyStrategyOptions(Map<String, String> options) {
      try {
         this.minCompactionThreshold = getIntegerOption(options, "mos_min_threshold", 2);
         this.maxCompactionThreshold = getIntegerOption(options, "mos_max_threshold", 32);
         this.maxActiveCompactions = getIntegerOption(options, "mos_max_active_compactions", 100);
      } catch (ConfigurationException var3) {
         throw new RuntimeException(var3);
      }
   }

   public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException {
      int minThreshold = getIntegerOption(options, "mos_min_threshold", 2);
      int maxThreshold = getIntegerOption(options, "mos_max_threshold", 32);
      int maxActiveCompactions = getIntegerOption(options, "mos_max_active_compactions", 100);
      if(minThreshold < 2) {
         throw new ConfigurationException(String.format("'%s' must be at least 2!", new Object[]{"mos_min_threshold"}));
      } else if(maxThreshold < 2) {
         throw new ConfigurationException(String.format("'%s' must be at least 2!", new Object[]{"mos_max_threshold"}));
      } else if(minThreshold > maxThreshold) {
         throw new ConfigurationException(String.format("'%s' (%d) is greater than '%s' (%d)!", new Object[]{"mos_min_threshold", Integer.valueOf(minThreshold), "mos_max_threshold", Integer.valueOf(maxThreshold)}));
      } else if(maxActiveCompactions <= 0) {
         throw new ConfigurationException(String.format("'%s' must be greater than 0, or your table will never compact!", new Object[]{"mos_max_active_compactions"}));
      } else {
         uncheckedOptions.remove("mos_min_threshold");
         uncheckedOptions.remove("mos_max_threshold");
         uncheckedOptions.remove("mos_max_active_compactions");
         return uncheckedOptions;
      }
   }

   private static int getIntegerOption(Map<String, String> options, String optionName, int defaultValue) throws ConfigurationException {
      if(options.containsKey(optionName)) {
         try {
            return Integer.parseInt((String)options.get(optionName));
         } catch (NumberFormatException var4) {
            throw new ConfigurationException(String.format("Option '%s' must be a number, not '%s'", new Object[]{optionName, options.get(optionName)}));
         }
      } else {
         return defaultValue;
      }
   }
}
