package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.BloomCalculations;

public final class TableParams {
   public static final TableParams DEFAULT = builder().build();
   public static final String DEFAULT_COMMENT = "";
   public static final double DEFAULT_READ_REPAIR_CHANCE = 0.0D;
   public static final double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.1D;
   public static final int DEFAULT_GC_GRACE_SECONDS = 864000;
   public static final int DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
   public static final int DEFAULT_MEMTABLE_FLUSH_PERIOD_IN_MS = 0;
   public static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
   public static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;
   public static final double DEFAULT_CRC_CHECK_CHANCE = 1.0D;
   public final String comment;
   public final double readRepairChance;
   public final double dcLocalReadRepairChance;
   public final double bloomFilterFpChance;
   public final double crcCheckChance;
   public final int gcGraceSeconds;
   public final int defaultTimeToLive;
   public final int memtableFlushPeriodInMs;
   public final int minIndexInterval;
   public final int maxIndexInterval;
   public final SpeculativeRetryParam speculativeRetry;
   public final CachingParams caching;
   public final CompactionParams compaction;
   public final CompressionParams compression;
   public final ImmutableMap<String, ByteBuffer> extensions;
   public final boolean cdc;
   public final NodeSyncParams nodeSync;

   private TableParams(TableParams.Builder builder) {
      this.comment = builder.comment;
      this.readRepairChance = builder.readRepairChance;
      this.dcLocalReadRepairChance = builder.dcLocalReadRepairChance;
      this.bloomFilterFpChance = builder.bloomFilterFpChance == null?builder.compaction.defaultBloomFilterFbChance():builder.bloomFilterFpChance.doubleValue();
      this.crcCheckChance = builder.crcCheckChance.doubleValue();
      this.gcGraceSeconds = builder.gcGraceSeconds;
      this.defaultTimeToLive = builder.defaultTimeToLive;
      this.memtableFlushPeriodInMs = builder.memtableFlushPeriodInMs;
      this.minIndexInterval = builder.minIndexInterval;
      this.maxIndexInterval = builder.maxIndexInterval;
      this.speculativeRetry = builder.speculativeRetry;
      this.caching = builder.caching;
      this.compaction = builder.compaction;
      this.compression = builder.compression;
      this.extensions = builder.extensions;
      this.cdc = builder.cdc;
      this.nodeSync = builder.nodeSync;
   }

   public static TableParams.Builder builder() {
      return new TableParams.Builder();
   }

   public static TableParams.Builder builder(TableParams params) {
      return (new TableParams.Builder()).bloomFilterFpChance(params.bloomFilterFpChance).caching(params.caching).comment(params.comment).compaction(params.compaction).compression(params.compression).dcLocalReadRepairChance(params.dcLocalReadRepairChance).crcCheckChance(params.crcCheckChance).defaultTimeToLive(params.defaultTimeToLive).gcGraceSeconds(params.gcGraceSeconds).maxIndexInterval(params.maxIndexInterval).memtableFlushPeriodInMs(params.memtableFlushPeriodInMs).minIndexInterval(params.minIndexInterval).readRepairChance(params.readRepairChance).speculativeRetry(params.speculativeRetry).extensions(params.extensions).cdc(params.cdc).nodeSync(params.nodeSync);
   }

   public TableParams.Builder unbuild() {
      return builder(this);
   }

   public void validate() {
      this.compaction.validate();
      this.compression.validate();
      double minBloomFilterFpChanceValue = BloomCalculations.minSupportedBloomFilterFpChance();
      if(this.bloomFilterFpChance <= minBloomFilterFpChanceValue || this.bloomFilterFpChance > 1.0D) {
         fail("%s must be larger than %s and less than or equal to 1.0 (got %s)", new Object[]{TableParams.Option.BLOOM_FILTER_FP_CHANCE, Double.valueOf(minBloomFilterFpChanceValue), Double.valueOf(this.bloomFilterFpChance)});
      }

      if(this.dcLocalReadRepairChance < 0.0D || this.dcLocalReadRepairChance > 1.0D) {
         fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)", new Object[]{TableParams.Option.DCLOCAL_READ_REPAIR_CHANCE, Double.valueOf(this.dcLocalReadRepairChance)});
      }

      if(this.readRepairChance < 0.0D || this.readRepairChance > 1.0D) {
         fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)", new Object[]{TableParams.Option.READ_REPAIR_CHANCE, Double.valueOf(this.readRepairChance)});
      }

      if(this.crcCheckChance < 0.0D || this.crcCheckChance > 1.0D) {
         fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)", new Object[]{TableParams.Option.CRC_CHECK_CHANCE, Double.valueOf(this.crcCheckChance)});
      }

      if(this.defaultTimeToLive < 0) {
         fail("%s must be greater than or equal to 0 (got %s)", new Object[]{TableParams.Option.DEFAULT_TIME_TO_LIVE, Integer.valueOf(this.defaultTimeToLive)});
      }

      if(this.defaultTimeToLive > 630720000) {
         fail("%s must be less than or equal to %d (got %s)", new Object[]{TableParams.Option.DEFAULT_TIME_TO_LIVE, Integer.valueOf(630720000), Integer.valueOf(this.defaultTimeToLive)});
      }

      if(this.gcGraceSeconds < 0) {
         fail("%s must be greater than or equal to 0 (got %s)", new Object[]{TableParams.Option.GC_GRACE_SECONDS, Integer.valueOf(this.gcGraceSeconds)});
      }

      if(this.minIndexInterval < 1) {
         fail("%s must be greater than or equal to 1 (got %s)", new Object[]{TableParams.Option.MIN_INDEX_INTERVAL, Integer.valueOf(this.minIndexInterval)});
      }

      if(this.maxIndexInterval < this.minIndexInterval) {
         fail("%s must be greater than or equal to %s (%s) (got %s)", new Object[]{TableParams.Option.MAX_INDEX_INTERVAL, TableParams.Option.MIN_INDEX_INTERVAL, Integer.valueOf(this.minIndexInterval), Integer.valueOf(this.maxIndexInterval)});
      }

      if(this.memtableFlushPeriodInMs < 0) {
         fail("%s must be greater than or equal to 0 (got %s)", new Object[]{TableParams.Option.MEMTABLE_FLUSH_PERIOD_IN_MS, Integer.valueOf(this.memtableFlushPeriodInMs)});
      }

   }

   private static void fail(String format, Object... args) {
      throw new ConfigurationException(String.format(format, args));
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TableParams)) {
         return false;
      } else {
         TableParams p = (TableParams)o;
         return this.equalsIgnoringNodeSync(p) && this.nodeSync.equals(p.nodeSync);
      }
   }

   public boolean equalsIgnoringNodeSync(TableParams p) {
      return this.comment.equals(p.comment) && this.readRepairChance == p.readRepairChance && this.dcLocalReadRepairChance == p.dcLocalReadRepairChance && this.bloomFilterFpChance == p.bloomFilterFpChance && this.crcCheckChance == p.crcCheckChance && this.gcGraceSeconds == p.gcGraceSeconds && this.defaultTimeToLive == p.defaultTimeToLive && this.memtableFlushPeriodInMs == p.memtableFlushPeriodInMs && this.minIndexInterval == p.minIndexInterval && this.maxIndexInterval == p.maxIndexInterval && this.speculativeRetry.equals(p.speculativeRetry) && this.caching.equals(p.caching) && this.compaction.equals(p.compaction) && this.compression.equals(p.compression) && this.extensions.equals(p.extensions) && this.cdc == p.cdc;
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.comment, Double.valueOf(this.readRepairChance), Double.valueOf(this.dcLocalReadRepairChance), Double.valueOf(this.bloomFilterFpChance), Double.valueOf(this.crcCheckChance), Integer.valueOf(this.gcGraceSeconds), Integer.valueOf(this.defaultTimeToLive), Integer.valueOf(this.memtableFlushPeriodInMs), Integer.valueOf(this.minIndexInterval), Integer.valueOf(this.maxIndexInterval), this.speculativeRetry, this.caching, this.compaction, this.compression, this.extensions, Boolean.valueOf(this.cdc), this.nodeSync});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add(TableParams.Option.COMMENT.toString(), this.comment).add(TableParams.Option.READ_REPAIR_CHANCE.toString(), this.readRepairChance).add(TableParams.Option.DCLOCAL_READ_REPAIR_CHANCE.toString(), this.dcLocalReadRepairChance).add(TableParams.Option.BLOOM_FILTER_FP_CHANCE.toString(), this.bloomFilterFpChance).add(TableParams.Option.CRC_CHECK_CHANCE.toString(), this.crcCheckChance).add(TableParams.Option.GC_GRACE_SECONDS.toString(), this.gcGraceSeconds).add(TableParams.Option.DEFAULT_TIME_TO_LIVE.toString(), this.defaultTimeToLive).add(TableParams.Option.MEMTABLE_FLUSH_PERIOD_IN_MS.toString(), this.memtableFlushPeriodInMs).add(TableParams.Option.MIN_INDEX_INTERVAL.toString(), this.minIndexInterval).add(TableParams.Option.MAX_INDEX_INTERVAL.toString(), this.maxIndexInterval).add(TableParams.Option.SPECULATIVE_RETRY.toString(), this.speculativeRetry).add(TableParams.Option.CACHING.toString(), this.caching).add(TableParams.Option.COMPACTION.toString(), this.compaction).add(TableParams.Option.COMPRESSION.toString(), this.compression).add(TableParams.Option.EXTENSIONS.toString(), this.extensions).add(TableParams.Option.CDC.toString(), this.cdc).add(TableParams.Option.NODESYNC.toString(), this.nodeSync).toString();
   }

   public static final class Builder {
      private String comment;
      private double readRepairChance;
      private double dcLocalReadRepairChance;
      private Double bloomFilterFpChance;
      public Double crcCheckChance;
      private int gcGraceSeconds;
      private int defaultTimeToLive;
      private int memtableFlushPeriodInMs;
      private int minIndexInterval;
      private int maxIndexInterval;
      private SpeculativeRetryParam speculativeRetry;
      private CachingParams caching;
      private CompactionParams compaction;
      private CompressionParams compression;
      private ImmutableMap<String, ByteBuffer> extensions;
      private boolean cdc;
      private NodeSyncParams nodeSync;

      private Builder() {
         this.comment = "";
         this.readRepairChance = 0.0D;
         this.dcLocalReadRepairChance = 0.1D;
         this.crcCheckChance = Double.valueOf(1.0D);
         this.gcGraceSeconds = 864000;
         this.defaultTimeToLive = 0;
         this.memtableFlushPeriodInMs = 0;
         this.minIndexInterval = 128;
         this.maxIndexInterval = 2048;
         this.speculativeRetry = SpeculativeRetryParam.DEFAULT;
         this.caching = CachingParams.DEFAULT;
         this.compaction = CompactionParams.DEFAULT;
         this.compression = CompressionParams.DEFAULT;
         this.extensions = ImmutableMap.of();
         this.nodeSync = NodeSyncParams.DEFAULT;
      }

      public TableParams build() {
         return new TableParams(this);
      }

      public TableParams.Builder comment(String val) {
         this.comment = val;
         return this;
      }

      public TableParams.Builder readRepairChance(double val) {
         this.readRepairChance = val;
         return this;
      }

      public TableParams.Builder dcLocalReadRepairChance(double val) {
         this.dcLocalReadRepairChance = val;
         return this;
      }

      public TableParams.Builder bloomFilterFpChance(double val) {
         this.bloomFilterFpChance = Double.valueOf(val);
         return this;
      }

      public TableParams.Builder crcCheckChance(double val) {
         this.crcCheckChance = Double.valueOf(val);
         return this;
      }

      public TableParams.Builder gcGraceSeconds(int val) {
         this.gcGraceSeconds = val;
         return this;
      }

      public TableParams.Builder defaultTimeToLive(int val) {
         this.defaultTimeToLive = val;
         return this;
      }

      public TableParams.Builder memtableFlushPeriodInMs(int val) {
         this.memtableFlushPeriodInMs = val;
         return this;
      }

      public TableParams.Builder minIndexInterval(int val) {
         this.minIndexInterval = val;
         return this;
      }

      public TableParams.Builder maxIndexInterval(int val) {
         this.maxIndexInterval = val;
         return this;
      }

      public TableParams.Builder speculativeRetry(SpeculativeRetryParam val) {
         this.speculativeRetry = val;
         return this;
      }

      public TableParams.Builder caching(CachingParams val) {
         this.caching = val;
         return this;
      }

      public TableParams.Builder compaction(CompactionParams val) {
         this.compaction = val;
         return this;
      }

      public TableParams.Builder compression(CompressionParams val) {
         this.compression = val;
         return this;
      }

      public TableParams.Builder cdc(boolean val) {
         this.cdc = val;
         return this;
      }

      public TableParams.Builder nodeSync(NodeSyncParams val) {
         this.nodeSync = val;
         return this;
      }

      public TableParams.Builder extensions(Map<String, ByteBuffer> val) {
         this.extensions = ImmutableMap.copyOf(val);
         return this;
      }
   }

   public static enum Option {
      BLOOM_FILTER_FP_CHANCE,
      CACHING,
      COMMENT,
      COMPACTION,
      COMPRESSION,
      DCLOCAL_READ_REPAIR_CHANCE,
      DEFAULT_TIME_TO_LIVE,
      EXTENSIONS,
      GC_GRACE_SECONDS,
      MAX_INDEX_INTERVAL,
      MEMTABLE_FLUSH_PERIOD_IN_MS,
      MIN_INDEX_INTERVAL,
      READ_REPAIR_CHANCE,
      SPECULATIVE_RETRY,
      CRC_CHECK_CHANCE,
      CDC,
      NODESYNC;

      private Option() {
      }

      public String toString() {
         return this.name().toLowerCase();
      }
   }
}
