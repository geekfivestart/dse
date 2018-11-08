package org.apache.cassandra.cql3.statements;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientWarn;

public final class TableAttributes extends PropertyDefinitions {
   private static final String KW_ID = "id";
   private static final Set<String> validKeywords;
   private static final Set<String> obsoleteKeywords;
   private static boolean loggedReadRepairChanceDeprecationWarnings;

   public TableAttributes() {
   }

   public void validate() {
      this.validate(validKeywords, obsoleteKeywords);
      this.build(TableParams.builder()).validate();
   }

   public TableParams asNewTableParams() {
      return this.build(TableParams.builder());
   }

   public TableParams asAlteredTableParams(TableParams previous) {
      if(this.getId() != null) {
         throw new ConfigurationException("Cannot alter table id.");
      } else {
         return this.build(previous.unbuild());
      }
   }

   public TableId getId() throws ConfigurationException {
      String id = this.getSimple("id");

      try {
         return id != null?TableId.fromString(id):null;
      } catch (IllegalArgumentException var3) {
         throw new ConfigurationException("Invalid table id", var3);
      }
   }

   private TableParams build(TableParams.Builder builder) {
      if(this.hasOption(TableParams.Option.BLOOM_FILTER_FP_CHANCE)) {
         builder.bloomFilterFpChance(this.getDouble(TableParams.Option.BLOOM_FILTER_FP_CHANCE));
      }

      if(this.hasOption(TableParams.Option.CACHING)) {
         builder.caching(CachingParams.fromMap(this.getMap(TableParams.Option.CACHING)));
      }

      if(this.hasOption(TableParams.Option.CDC)) {
         builder.cdc(this.getBoolean(TableParams.Option.CDC.toString(), Boolean.valueOf(false)).booleanValue());
      }

      if(this.hasOption(TableParams.Option.COMMENT)) {
         builder.comment(this.getString(TableParams.Option.COMMENT));
      }

      if(this.hasOption(TableParams.Option.COMPACTION)) {
         builder.compaction(CompactionParams.fromMap(this.getMap(TableParams.Option.COMPACTION)));
      }

      if(this.hasOption(TableParams.Option.COMPRESSION)) {
         Map<String, String> compressionOpts = this.getMap(TableParams.Option.COMPRESSION);
         if(compressionOpts.containsKey(TableParams.Option.CRC_CHECK_CHANCE.toString().toLowerCase())) {
            Double crcCheckChance = this.getDeprecatedCrcCheckChance(compressionOpts);
            builder.crcCheckChance(crcCheckChance.doubleValue());
         }

         builder.compression(CompressionParams.fromMap(this.getMap(TableParams.Option.COMPRESSION)));
      }

      if(this.hasOption(TableParams.Option.CRC_CHECK_CHANCE)) {
         builder.crcCheckChance(this.getDouble(TableParams.Option.CRC_CHECK_CHANCE));
      }

      double chance;
      if(this.hasOption(TableParams.Option.DCLOCAL_READ_REPAIR_CHANCE)) {
         chance = this.getDouble(TableParams.Option.DCLOCAL_READ_REPAIR_CHANCE);
         if(chance != 0.0D) {
            ClientWarn.instance.warn("dclocal_read_repair_chance table option has been deprecated and will be removed in version 4.0");
            this.maybeLogReadRepairChanceDeprecationWarning();
         }

         builder.dcLocalReadRepairChance(chance);
      }

      if(this.hasOption(TableParams.Option.DEFAULT_TIME_TO_LIVE)) {
         builder.defaultTimeToLive(this.getInt(TableParams.Option.DEFAULT_TIME_TO_LIVE));
      }

      if(this.hasOption(TableParams.Option.GC_GRACE_SECONDS)) {
         builder.gcGraceSeconds(this.getInt(TableParams.Option.GC_GRACE_SECONDS));
      }

      if(this.hasOption(TableParams.Option.MAX_INDEX_INTERVAL)) {
         builder.maxIndexInterval(this.getInt(TableParams.Option.MAX_INDEX_INTERVAL));
      }

      if(this.hasOption(TableParams.Option.MEMTABLE_FLUSH_PERIOD_IN_MS)) {
         builder.memtableFlushPeriodInMs(this.getInt(TableParams.Option.MEMTABLE_FLUSH_PERIOD_IN_MS));
      }

      if(this.hasOption(TableParams.Option.MIN_INDEX_INTERVAL)) {
         builder.minIndexInterval(this.getInt(TableParams.Option.MIN_INDEX_INTERVAL));
      }

      if(this.hasOption(TableParams.Option.NODESYNC)) {
         builder.nodeSync(NodeSyncParams.fromUserProvidedParameters(this.getMap(TableParams.Option.NODESYNC)));
      }

      if(this.hasOption(TableParams.Option.READ_REPAIR_CHANCE)) {
         chance = this.getDouble(TableParams.Option.READ_REPAIR_CHANCE);
         if(chance != 0.0D) {
            ClientWarn.instance.warn("read_repair_chance table option has been deprecated and will be removed in version 4.0");
            this.maybeLogReadRepairChanceDeprecationWarning();
         }

         builder.readRepairChance(chance);
      }

      if(this.hasOption(TableParams.Option.SPECULATIVE_RETRY)) {
         builder.speculativeRetry(SpeculativeRetryParam.fromString(this.getString(TableParams.Option.SPECULATIVE_RETRY)));
      }

      return builder.build();
   }

   private void maybeLogReadRepairChanceDeprecationWarning() {
      if(!loggedReadRepairChanceDeprecationWarnings) {
         logger.warn("dclocal_read_repair_chance and read_repair_chance table options have been deprecated and will be removed in version 4.0");
         loggedReadRepairChanceDeprecationWarnings = true;
      }

   }

   private Double getDeprecatedCrcCheckChance(Map<String, String> compressionOpts) {
      String value = (String)compressionOpts.get(TableParams.Option.CRC_CHECK_CHANCE.toString().toLowerCase());

      try {
         return Double.valueOf(value);
      } catch (NumberFormatException var4) {
         throw new SyntaxException(String.format("Invalid double value %s for crc_check_chance.'", new Object[]{value}));
      }
   }

   private double getDouble(TableParams.Option option) {
      String value = this.getString(option);

      try {
         return Double.parseDouble(value);
      } catch (NumberFormatException var4) {
         throw new SyntaxException(String.format("Invalid double value %s for '%s'", new Object[]{value, option}));
      }
   }

   private int getInt(TableParams.Option option) {
      String value = this.getString(option);

      try {
         return Integer.parseInt(value);
      } catch (NumberFormatException var4) {
         throw new SyntaxException(String.format("Invalid integer value %s for '%s'", new Object[]{value, option}));
      }
   }

   private String getString(TableParams.Option option) {
      String value = this.getSimple(option.toString());
      if(value == null) {
         throw new IllegalStateException(String.format("Option '%s' is absent", new Object[]{option}));
      } else {
         return value;
      }
   }

   private Map<String, String> getMap(TableParams.Option option) {
      Map<String, String> value = this.getMap(option.toString());
      if(value == null) {
         throw new IllegalStateException(String.format("Option '%s' is absent", new Object[]{option}));
      } else {
         return value;
      }
   }

   boolean hasOption(TableParams.Option option) {
      return this.hasProperty(option.toString()).booleanValue();
   }

   static {
      Builder<String> validBuilder = ImmutableSet.builder();
      TableParams.Option[] var1 = TableParams.Option.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         TableParams.Option option = var1[var3];
         validBuilder.add(option.toString());
      }

      validBuilder.add("id");
      validKeywords = validBuilder.build();
      obsoleteKeywords = ImmutableSet.of();
   }
}
