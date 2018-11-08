package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

public final class CachingParams {
   private static final String ALL = "ALL";
   private static final String NONE = "NONE";
   static final boolean DEFAULT_CACHE_KEYS = true;
   static final int DEFAULT_ROWS_PER_PARTITION_TO_CACHE = 0;
   public static final CachingParams CACHE_NOTHING = new CachingParams(false, 0);
   public static final CachingParams CACHE_KEYS = new CachingParams(true, 0);
   public static final CachingParams CACHE_EVERYTHING = new CachingParams(true, 2147483647);
   @VisibleForTesting
   public static CachingParams DEFAULT = new CachingParams(true, 0);
   final boolean cacheKeys;
   final int rowsPerPartitionToCache;

   public CachingParams(boolean cacheKeys, int rowsPerPartitionToCache) {
      this.cacheKeys = cacheKeys;
      this.rowsPerPartitionToCache = rowsPerPartitionToCache;
   }

   public boolean cacheKeys() {
      return this.cacheKeys;
   }

   public boolean cacheRows() {
      return this.rowsPerPartitionToCache > 0;
   }

   public boolean cacheAllRows() {
      return this.rowsPerPartitionToCache == 2147483647;
   }

   public int rowsPerPartitionToCache() {
      return this.rowsPerPartitionToCache;
   }

   public static CachingParams fromMap(Map<String, String> map) {
      Map<String, String> copy = new HashMap(map);
      String keys = (String)copy.remove(CachingParams.Option.KEYS.toString());
      boolean cacheKeys = keys != null && keysFromString(keys);
      String rows = (String)copy.remove(CachingParams.Option.ROWS_PER_PARTITION.toString());
      int rowsPerPartitionToCache = rows == null?0:rowsPerPartitionFromString(rows);
      if(!copy.isEmpty()) {
         throw new ConfigurationException(String.format("Invalid caching sub-options %s: only '%s' and '%s' are allowed", new Object[]{copy.keySet(), CachingParams.Option.KEYS, CachingParams.Option.ROWS_PER_PARTITION}));
      } else {
         return new CachingParams(cacheKeys, rowsPerPartitionToCache);
      }
   }

   public Map<String, String> asMap() {
      return ImmutableMap.of(CachingParams.Option.KEYS.toString(), this.keysAsString(), CachingParams.Option.ROWS_PER_PARTITION.toString(), this.rowsPerPartitionAsString());
   }

   private static boolean keysFromString(String value) {
      if(value.equalsIgnoreCase("ALL")) {
         return true;
      } else if(value.equalsIgnoreCase("NONE")) {
         return false;
      } else {
         throw new ConfigurationException(String.format("Invalid value '%s' for caching sub-option '%s': only '%s' and '%s' are allowed", new Object[]{value, CachingParams.Option.KEYS, "ALL", "NONE"}));
      }
   }

   String keysAsString() {
      return this.cacheKeys?"ALL":"NONE";
   }

   private static int rowsPerPartitionFromString(String value) {
      if(value.equalsIgnoreCase("ALL")) {
         return 2147483647;
      } else if(value.equalsIgnoreCase("NONE")) {
         return 0;
      } else if(StringUtils.isNumeric(value)) {
         return Integer.parseInt(value);
      } else {
         throw new ConfigurationException(String.format("Invalid value '%s' for caching sub-option '%s': only '%s', '%s', and integer values are allowed", new Object[]{value, CachingParams.Option.ROWS_PER_PARTITION, "ALL", "NONE"}));
      }
   }

   String rowsPerPartitionAsString() {
      return this.rowsPerPartitionToCache == 0?"NONE":(this.rowsPerPartitionToCache == 2147483647?"ALL":Integer.toString(this.rowsPerPartitionToCache));
   }

   public String toString() {
      return String.format("{'%s' : '%s', '%s' : '%s'}", new Object[]{CachingParams.Option.KEYS, this.keysAsString(), CachingParams.Option.ROWS_PER_PARTITION, this.rowsPerPartitionAsString()});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof CachingParams)) {
         return false;
      } else {
         CachingParams c = (CachingParams)o;
         return this.cacheKeys == c.cacheKeys && this.rowsPerPartitionToCache == c.rowsPerPartitionToCache;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Boolean.valueOf(this.cacheKeys), Integer.valueOf(this.rowsPerPartitionToCache)});
   }

   public static enum Option {
      KEYS,
      ROWS_PER_PARTITION;

      private Option() {
      }

      public String toString() {
         return this.name().toLowerCase();
      }
   }
}
