package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;

public class RateSimulator {
   private final RateSimulator.Info info;
   private final RateSimulator.Parameters parameters;
   private Consumer<String> eventLogger;
   private boolean ignoreReplicationFactor;

   public RateSimulator(RateSimulator.Info info, RateSimulator.Parameters parameters) {
      this.info = info;
      this.parameters = parameters;
      this.eventLogger = (s) -> {
      };
   }

   public RateSimulator withLogger(Consumer<String> eventLogger) {
      this.eventLogger = eventLogger;
      return this;
   }

   public RateSimulator ignoreReplicationFactor() {
      this.ignoreReplicationFactor = true;
      return this;
   }

   public RateValue computeRate() {
      this.logParameters();
      long minRate = 0L;
      long cumulativeSize = 0L;
      Iterator var5 = this.info.tables().iterator();

      while(var5.hasNext()) {
         RateSimulator.TableInfo table = (RateSimulator.TableInfo)var5.next();
         this.log("%s:", new Object[]{table.tableName()});
         if(table.dataSize.equals(SizeValue.ZERO)) {
            this.log("  - No data so nothing to validate.", new Object[0]);
         } else {
            long target = table.deadlineTarget.in(TimeUnit.SECONDS);
            long adjustedTarget = this.parameters.adjustedDeadline(target);
            String adjustedTargetFrom = target == adjustedTarget?"":String.format(", adjusted from %s for safety", new Object[]{table.deadlineTarget});
            this.log("  - Deadline target=%s%s.", new Object[]{Units.toString(adjustedTarget, TimeUnit.SECONDS), adjustedTargetFrom});
            long tableSize = table.dataSize.in(SizeUnit.BYTES);
            long adjustedSize = this.parameters.adjustedSize(tableSize);
            long size = this.ignoreReplicationFactor?adjustedSize:adjustedSize / (long)table.replicationFactor;
            String adjustedSizeFrom = tableSize == adjustedSize?"":String.format(" (adjusted from %s for future growth)", new Object[]{table.dataSize});
            if(this.ignoreReplicationFactor) {
               this.log("  - Size=%s%s.", new Object[]{Units.toString(adjustedSize, SizeUnit.BYTES), adjustedSizeFrom});
            } else {
               this.log("  - Size=%s to validate (%s total%s but RF=%d).", new Object[]{Units.toString(size, SizeUnit.BYTES), Units.toString(adjustedSize, SizeUnit.BYTES), adjustedSizeFrom, Integer.valueOf(table.replicationFactor)});
            }

            cumulativeSize += size;
            long rate = rate(cumulativeSize, adjustedTarget);
            this.log("  - Added to previous tables, %s to validate in %s => %s", new Object[]{Units.toString(cumulativeSize, SizeUnit.BYTES), Units.toString(adjustedTarget, TimeUnit.SECONDS), Units.toString(rate, RateUnit.B_S)});
            if(rate > minRate) {
               minRate = rate;
               this.log("  => New minimum rate: %s", new Object[]{Units.toString(rate, RateUnit.B_S)});
            } else {
               this.log("  => Unchanged minimum rate: %s", new Object[]{Units.toString(minRate, RateUnit.B_S)});
            }
         }
      }

      this.logSeparation();
      long adjustedRate = this.parameters.adjustedRate(minRate);
      RateValue rate = RateValue.of(adjustedRate, RateUnit.B_S);
      this.log("Computed rate: %s%s.", new Object[]{rate, minRate == adjustedRate?"":String.format(", adjusted from %s for safety", new Object[]{Units.toString(minRate, RateUnit.B_S)})});
      return rate;
   }

   private void logParameters() {
      this.log("Using parameters:", new Object[0]);
      this.log(" - Size growing factor:    %.2f", new Object[]{Float.valueOf(this.parameters.sizeGrowingFactor)});
      this.log(" - Deadline safety factor: %.2f", new Object[]{Float.valueOf(this.parameters.deadlineSafetyFactor)});
      this.log(" - Rate safety factor:     %.2f", new Object[]{Float.valueOf(this.parameters.rateSafetyFactor)});
      this.logSeparation();
   }

   private void logSeparation() {
      this.eventLogger.accept("");
   }

   private void log(String msg, Object... values) {
      this.eventLogger.accept(String.format(msg, values));
   }

   private static long rate(long sizeInBytes, long timeInSeconds) {
      return (long)Math.ceil((double)sizeInBytes / (double)timeInSeconds);
   }

   public static class TableInfo {
      public final String keyspace;
      public final String table;
      public final int replicationFactor;
      public final boolean isNodeSyncEnabled;
      public final SizeValue dataSize;
      public final TimeValue deadlineTarget;

      @VisibleForTesting
      TableInfo(String keyspace, String table, int replicationFactor, boolean isNodeSyncEnabled, SizeValue dataSize, TimeValue deadlineTarget) {
         this.keyspace = keyspace;
         this.table = table;
         this.replicationFactor = replicationFactor;
         this.isNodeSyncEnabled = isNodeSyncEnabled;
         this.dataSize = dataSize;
         this.deadlineTarget = deadlineTarget;
      }

      public String tableName() {
         return String.format("%s.%s", new Object[]{ColumnIdentifier.maybeQuote(this.keyspace), ColumnIdentifier.maybeQuote(this.table)});
      }

      public TimeValue getDeadlineTarget(){
         return deadlineTarget;
      }

      public String getKeyspace(){return keyspace;}

      public String getTable(){return table;}

      static RateSimulator.TableInfo fromStore(ColumnFamilyStore store) {
         TableMetadata table = store.metadata();
         return new RateSimulator.TableInfo(table.keyspace, table.name, store.keyspace.getReplicationStrategy().getReplicationFactor(), NodeSyncHelpers.isNodeSyncEnabled(store), SizeValue.of(NodeSyncHelpers.estimatedSizeOf(store), SizeUnit.BYTES), TimeValue.of(table.params.nodeSync.deadlineTarget(table, TimeUnit.SECONDS), TimeUnit.SECONDS));
      }

      Map<String, String> toStringMap() {
         com.google.common.collect.ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
         return builder.put(RateSimulator.TableInfo.Property.KEYSPACE.toString(), this.keyspace).put(RateSimulator.TableInfo.Property.TABLE.toString(), this.table).put(RateSimulator.TableInfo.Property.REPLICATION_FACTOR.toString(), Integer.toString(this.replicationFactor)).put(RateSimulator.TableInfo.Property.NODESYNC_ENABLED.toString(), Boolean.toString(this.isNodeSyncEnabled)).put(RateSimulator.TableInfo.Property.DATA_SIZE.toString(), Long.toString(this.dataSize.in(SizeUnit.BYTES))).put(RateSimulator.TableInfo.Property.DEADLINE_TARGET.toString(), Long.toString(this.deadlineTarget.in(TimeUnit.SECONDS))).build();
      }

      static RateSimulator.TableInfo fromStringMap(Map<String, String> m) {
         return new RateSimulator.TableInfo(get(RateSimulator.TableInfo.Property.KEYSPACE, m), get(RateSimulator.TableInfo.Property.TABLE, m), Integer.parseInt(get(RateSimulator.TableInfo.Property.REPLICATION_FACTOR, m)), Boolean.parseBoolean(get(RateSimulator.TableInfo.Property.NODESYNC_ENABLED, m)), SizeValue.of(Long.parseLong(get(RateSimulator.TableInfo.Property.DATA_SIZE, m)), SizeUnit.BYTES), TimeValue.of(Long.parseLong(get(RateSimulator.TableInfo.Property.DEADLINE_TARGET, m)), TimeUnit.SECONDS));
      }

      public RateSimulator.TableInfo withNewDeadline(TimeValue newDeadlineTarget) {
         return new RateSimulator.TableInfo(this.keyspace, this.table, this.replicationFactor, this.isNodeSyncEnabled, this.dataSize, newDeadlineTarget);
      }

      public RateSimulator.TableInfo withNodeSyncEnabled() {
         return new RateSimulator.TableInfo(this.keyspace, this.table, this.replicationFactor, true, this.dataSize, this.deadlineTarget);
      }

      public RateSimulator.TableInfo withoutNodeSyncEnabled() {
         return new RateSimulator.TableInfo(this.keyspace, this.table, this.replicationFactor, false, this.dataSize, this.deadlineTarget);
      }

      private static String get(RateSimulator.TableInfo.Property p, Map<String, String> m) {
         String v = (String)m.get(p.toString());
         if(v == null) {
            throw new IllegalArgumentException(String.format("Missing mandatory property '%s' in TableInfo map: %s", new Object[]{p, m}));
         } else {
            return v;
         }
      }

      public String toString() {
         return String.format("%s[rf=%d, nodesync=%b, size=%s, deadline=%s", new Object[]{this.tableName(), Integer.valueOf(this.replicationFactor), Boolean.valueOf(this.isNodeSyncEnabled), this.dataSize, this.deadlineTarget});
      }

      public boolean equals(Object other) {
         if(!(other instanceof RateSimulator.TableInfo)) {
            return false;
         } else {
            RateSimulator.TableInfo that = (RateSimulator.TableInfo)other;
            return this.keyspace.equals(that.keyspace) && this.table.equals(that.table) && this.replicationFactor == that.replicationFactor && this.isNodeSyncEnabled == that.isNodeSyncEnabled && this.dataSize.equals(that.dataSize) && this.deadlineTarget.equals(that.deadlineTarget);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.keyspace, this.table, Integer.valueOf(this.replicationFactor), Boolean.valueOf(this.isNodeSyncEnabled), this.dataSize, this.deadlineTarget});
      }

      @VisibleForTesting
      static enum Property {
         KEYSPACE,
         TABLE,
         REPLICATION_FACTOR,
         NODESYNC_ENABLED,
         DATA_SIZE,
         DEADLINE_TARGET;

         private Property() {
         }

         public static RateSimulator.TableInfo.Property fromString(String str) {
            return valueOf(str.toUpperCase());
         }

         public String toString() {
            return super.toString().toLowerCase();
         }
      }
   }

   public static class Info {
      private static final Comparator<RateSimulator.TableInfo> COMPARATOR = Comparator.comparing(TableInfo::getDeadlineTarget).thenComparing(TableInfo::getKeyspace).thenComparing(TableInfo::getTable);
      private final SortedSet<RateSimulator.TableInfo> tables;

      private Info(SortedSet<RateSimulator.TableInfo> tables) {
         this.tables = tables;
      }

      private static SortedSet<RateSimulator.TableInfo> newBackingSet() {
         return new TreeSet(COMPARATOR);
      }

      @VisibleForTesting
      static RateSimulator.Info from(RateSimulator.TableInfo... infos) {
         SortedSet<RateSimulator.TableInfo> tables = newBackingSet();
         tables.addAll(Arrays.asList(infos));
         return new RateSimulator.Info(tables);
      }

      public static RateSimulator.Info compute(boolean includeAllTables) {
         return new RateSimulator.Info((SortedSet)(includeAllTables?allStores():NodeSyncHelpers.nodeSyncEnabledStores()).map(RateSimulator.TableInfo::fromStore).collect(Collectors.toCollection(RateSimulator.Info::newBackingSet)));
      }

      private static Stream<ColumnFamilyStore> allStores() {
         return StorageService.instance.getNonSystemKeyspaces().stream().map(Keyspace::open).flatMap((k) -> {
            return k.getColumnFamilyStores().stream();
         });
      }

      public RateSimulator.Info transform(UnaryOperator<RateSimulator.TableInfo> transformation) {
         return new RateSimulator.Info((SortedSet)this.tables.stream().map(transformation).collect(Collectors.toCollection(RateSimulator.Info::newBackingSet)));
      }

      public List<Map<String, String>> toJMX() {
         return (List)this.tables.stream().map(RateSimulator.TableInfo::toStringMap).collect(Collectors.toList());
      }

      public static RateSimulator.Info fromJMX(List<Map<String, String>> l) {
         return new RateSimulator.Info((SortedSet)l.stream().map(RateSimulator.TableInfo::fromStringMap).collect(Collectors.toCollection(RateSimulator.Info::newBackingSet)));
      }

      public boolean isEmpty() {
         return this.tables.isEmpty();
      }

      public Iterable<RateSimulator.TableInfo> tables() {
         return Iterables.filter(this.tables, (t) -> {
            return t.isNodeSyncEnabled;
         });
      }

      public String toString() {
         return this.tables.toString();
      }

      public boolean equals(Object other) {
         if(!(other instanceof RateSimulator.Info)) {
            return false;
         } else {
            RateSimulator.Info that = (RateSimulator.Info)other;
            return this.tables.equals(that.tables);
         }
      }

      public int hashCode() {
         return this.tables.hashCode();
      }
   }

   public static class Parameters {
      public static final RateSimulator.Parameters THEORETICAL_MINIMUM = new RateSimulator.Parameters(0.0F, 0.0F, 0.0F);
      public static final RateSimulator.Parameters MINIMUM_RECOMMENDED = new RateSimulator.Parameters(0.2F, 0.25F, 0.0F);
      public static final RateSimulator.Parameters RECOMMENDED = new RateSimulator.Parameters(1.0F, 0.25F, 0.1F);
      public final float sizeGrowingFactor;
      public final float deadlineSafetyFactor;
      public final float rateSafetyFactor;

      private Parameters(float sizeGrowingFactor, float deadlineSafetyFactor, float rateSafetyFactor) {
         assert sizeGrowingFactor >= 0.0F;

         assert deadlineSafetyFactor >= 0.0F && deadlineSafetyFactor < 1.0F;

         assert rateSafetyFactor >= 0.0F;

         this.sizeGrowingFactor = sizeGrowingFactor;
         this.deadlineSafetyFactor = deadlineSafetyFactor;
         this.rateSafetyFactor = rateSafetyFactor;
      }

      public static RateSimulator.Parameters.Builder builder() {
         return new RateSimulator.Parameters.Builder();
      }

      long adjustedSize(long size) {
         return size + (long)Math.ceil((double)((float)size * this.sizeGrowingFactor));
      }

      long adjustedDeadline(long deadline) {
         return deadline - (long)Math.ceil((double)((float)deadline * this.deadlineSafetyFactor));
      }

      long adjustedRate(long rate) {
         return rate + (long)Math.ceil((double)((float)rate * this.rateSafetyFactor));
      }

      public static class Builder {
         private float sizeGrowingFactor;
         private float deadlineSafetyFactor;
         private float rateSafetyFactor;

         private Builder() {
         }

         public RateSimulator.Parameters.Builder sizeGrowingFactor(float factor) {
            if(factor < 0.0F) {
               throw new IllegalArgumentException("The size growing factor must be positive (>= 0)");
            } else {
               this.sizeGrowingFactor = factor;
               return this;
            }
         }

         public RateSimulator.Parameters.Builder deadlineSafetyFactor(float factor) {
            this.deadlineSafetyFactor = factor;
            return this;
         }

         public RateSimulator.Parameters.Builder rateSafetyFactor(float factor) {
            this.rateSafetyFactor = factor;
            return this;
         }

         public RateSimulator.Parameters build() {
            return new RateSimulator.Parameters(this.sizeGrowingFactor, this.deadlineSafetyFactor, this.rateSafetyFactor);
         }
      }
   }
}
