package org.apache.cassandra.schema;

import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.CQLSyntaxHelper;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NodeSyncParams {
   private static final Logger logger = LoggerFactory.getLogger(NodeSyncParams.class);
   private static final NoSpamLogger nospamLogger;
   private static final boolean DEFAULT_ENABLED = false;
   private static final int MIN_DEFAULT_DEADLINE_TARGET_SEC;
   public static final NodeSyncParams DEFAULT;
   @Nullable
   private final Boolean isEnabled;
   @Nullable
   private final Integer deadlineTargetSec;
   private final Map<String, String> unknownParameters;

   @VisibleForTesting
   public NodeSyncParams(Boolean isEnabled, Integer deadlineTargetSec, Map<String, String> unknownParameters) {
      this.isEnabled = isEnabled;
      this.deadlineTargetSec = deadlineTargetSec;
      this.unknownParameters = unknownParameters;
   }

   public static NodeSyncParams fromUserProvidedParameters(Map<String, String> map) {
      NodeSyncParams params = fromMapInternal(map, (exc, opt) -> {
         throw exc instanceof InvalidRequestException?new InvalidRequestException(String.format("Invalid value for nodesync option '%s': %s", new Object[]{opt, exc.getMessage()})):exc;
      });
      if(!params.unknownParameters.isEmpty()) {
         throw new InvalidRequestException(String.format("Unknown options provided for nodesync: %s", new Object[]{map.keySet()}));
      } else if(params.deadlineTargetSec != null && TimeUnit.SECONDS.toMillis((long)params.deadlineTargetSec.intValue()) <= NodeSyncService.MIN_VALIDATION_INTERVAL_MS) {
         boolean minValidationIsHigh = NodeSyncService.MIN_VALIDATION_INTERVAL_MS > TimeUnit.HOURS.toMillis(10L);
         throw new InvalidRequestException(String.format("nodesync '%s' setting has been set to %s which is lower than the %s value (%s): this mean the deadline cannot be achieved, at least on this node. %s", new Object[]{NodeSyncParams.Option.DEADLINE_TARGET_SEC, Units.toString((long)params.deadlineTargetSec.intValue(), TimeUnit.SECONDS), "dse.nodesync.min_validation_interval_ms", Units.toString(NodeSyncService.MIN_VALIDATION_INTERVAL_MS, TimeUnit.MILLISECONDS), minValidationIsHigh?"The custom value set for dse.nodesync.min_validation_interval_ms seems unwisely high":"This seems like an unwisely low value for " + NodeSyncParams.Option.DEADLINE_TARGET_SEC}));
      } else {
         return params;
      }
   }

   public static NodeSyncParams fromMap(String ksName, String tableName, Map<String, String> map) {
      return fromMapInternal(map, (exc, opt) -> {
         nospamLogger.error("Unexpected error parsing NodeSync '{}' option for {}.{} from {}: {}", new Object[]{opt, ksName, tableName, map, exc});
      });
   }

   private static NodeSyncParams fromMapInternal(Map<String, String> map, BiConsumer<RuntimeException, NodeSyncParams.Option> errHandler) {
      if(map == null) {
         return DEFAULT;
      } else {
         Map<String, String> params = new HashMap(map);
         Boolean isEnabled = (Boolean)getOpt(params, NodeSyncParams.Option.ENABLED, NodeSyncParams::parseEnabled, errHandler);
         Integer deadlineTargetSec = (Integer)getOpt(params, NodeSyncParams.Option.DEADLINE_TARGET_SEC, NodeSyncParams::parseDeadline, errHandler);
         return new NodeSyncParams(isEnabled, deadlineTargetSec, Collections.unmodifiableMap(params));
      }
   }

   private static Boolean parseEnabled(String value) {
      if(value.equalsIgnoreCase("true")) {
         return Boolean.valueOf(true);
      } else if(value.equalsIgnoreCase("false")) {
         return Boolean.valueOf(false);
      } else {
         throw new InvalidRequestException("expected 'true' or 'false' but got " + CQLSyntaxHelper.toCQLString(value));
      }
   }

   private static Integer parseDeadline(String value) {
      try {
         int deadline = Integer.parseInt(value);
         if(deadline <= 0) {
            throw new InvalidRequestException("expected a strictly positive integer but got " + deadline);
         } else {
            return Integer.valueOf(deadline);
         }
      } catch (NumberFormatException var2) {
         throw new InvalidRequestException("expect a strictly positive integer but got " + CQLSyntaxHelper.toCQLString(value));
      }
   }

   private static <T> T getOpt(Map<String, String> options, NodeSyncParams.Option option, Function<String, T> optHandler, BiConsumer<RuntimeException, NodeSyncParams.Option> errHandler) {
      try {
         String val = (String)options.remove(option.toString());
         return val == null?null:optHandler.apply(val);
      } catch (RuntimeException var5) {
         errHandler.accept(var5, option);
         return null;
      }
   }

   public Map<String, String> asMap() {
      Map<String, String> map = new HashMap(this.unknownParameters);
      if(this.isEnabled != null) {
         map.put(NodeSyncParams.Option.ENABLED.toString(), this.isEnabled.toString());
      }

      if(this.deadlineTargetSec != null) {
         map.put(NodeSyncParams.Option.DEADLINE_TARGET_SEC.toString(), Integer.toString(this.deadlineTargetSec.intValue()));
      }

      return map;
   }

   private static <T> T withViewDefaultHandling(TableMetadata table, NodeSyncParams.Option option, T nonViewDefault, BiFunction<NodeSyncParams, TableMetadata, T> getter) {
      if(!table.isView()) {
         return nonViewDefault;
      } else {
         TableMetadataRef baseTable = View.findBaseTable(table.keyspace, table.name);
         if(baseTable == null) {
            nospamLogger.warn("Cannot find base table for view {} while checking NodeSync '{}' setting: this shouldn't happen and should be reported but defaulting to {} in the meantime", new Object[]{table, option, nonViewDefault});
            return nonViewDefault;
         } else {
            TableMetadata base = baseTable.get();
            return getter.apply(base.params.nodeSync, base);
         }
      }
   }

   public boolean isEnabled(TableMetadata table) {
      return this.isEnabled != null?this.isEnabled.booleanValue():((Boolean)withViewDefaultHandling(table, NodeSyncParams.Option.ENABLED, Boolean.valueOf(false), NodeSyncParams::isEnabled)).booleanValue();
   }

   public long deadlineTarget(TableMetadata table, TimeUnit unit) {
      if(this.deadlineTargetSec != null) {
         return unit.convert((long)this.deadlineTargetSec.intValue(), TimeUnit.SECONDS);
      } else {
         long defaultValue = unit.convert((long)Math.max(MIN_DEFAULT_DEADLINE_TARGET_SEC, table.params.gcGraceSeconds), TimeUnit.SECONDS);
         return ((Long)withViewDefaultHandling(table, NodeSyncParams.Option.DEADLINE_TARGET_SEC, Long.valueOf(defaultValue), (p, t) -> {
            return Long.valueOf(p.deadlineTarget(t, unit));
         })).longValue();
      }
   }

   public String toString() {
      return CQLSyntaxHelper.toCQLMap(this.asMap());
   }

   public boolean equals(Object o) {
      if(!(o instanceof NodeSyncParams)) {
         return false;
      } else {
         NodeSyncParams that = (NodeSyncParams)o;
         return Objects.equals(this.isEnabled, that.isEnabled) && Objects.equals(this.deadlineTargetSec, that.deadlineTargetSec) && Objects.equals(this.unknownParameters, that.unknownParameters);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.isEnabled, this.deadlineTargetSec, this.unknownParameters});
   }

   static {
      nospamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
      MIN_DEFAULT_DEADLINE_TARGET_SEC = (int)TimeUnit.DAYS.toSeconds(4L);
      DEFAULT = new NodeSyncParams((Boolean)null, (Integer)null, Collections.emptyMap());
   }

   public static enum Option {
      ENABLED,
      DEADLINE_TARGET_SEC;

      private Option() {
      }

      public String toString() {
         return this.name().toLowerCase();
      }
   }
}
