package com.datastax.bdp.cassandra.db.tiered;

import java.util.Map;

public interface TieredTableStatsMXBean {
   String SUMMARY = "_summary";
   String ORPHAN = "orphan";
   String SIZE = "size";
   String KEYS = "estimated_keys";
   String ROWS = "rows";
   String MAX_AGE = "max_data_age";
   String MAX_TS = "max_timestamp";
   String MIN_TS = "min_timestamp";
   String LEVEL = "level";
   String READ_15 = "reads_15_min";
   String READ_120 = "reads_120_min";
   String SSTABLES = "sstables";

   Map<String, Map<String, Map<String, String>>> tierInfo(String var1, String var2, boolean var3);

   Map<String, Map<String, Map<String, Map<String, Map<String, String>>>>> tierInfo(boolean var1);
}
