package com.datastax.bdp.db.nodesync;

import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class TracingOptions {
   public static final Versioned<NodeSyncVerbs.NodeSyncVersion, Serializer<TracingOptions>> serializers = NodeSyncVerbs.NodeSyncVersion.versioned(TracingOptions.OptionSerializer::new);
   static final String ID = "id";
   static final String LEVEL = "level";
   static final String TIMEOUT_SEC = "timeout_sec";
   static final String TABLES = "tables";
   final UUID id;
   final TracingLevel level;
   final long timeoutSec;
   @Nullable
   final Set<TableId> tables;

   TracingOptions(UUID id, TracingLevel level, long timeoutSec, Set<TableId> tables) {
      this.id = id;
      this.level = level;
      this.timeoutSec = timeoutSec;
      this.tables = tables;
   }

   public static TracingOptions fromMap(Map<String, String> optionMap) {
      Map<String, String> map = new HashMap(optionMap);
      String idStr = (String)map.remove("id");
      UUID id = idStr == null?UUIDGen.getTimeUUID():UUID.fromString(idStr);
      String levelStr = (String)map.remove("level");
      TracingLevel level = levelStr == null?TracingLevel.LOW:TracingLevel.parse(levelStr);
      String timeoutStr = (String)map.remove("timeout_sec");
      long timeout = -1L;
      if(timeoutStr != null) {
         try {
            timeout = Long.parseLong(timeoutStr);
         } catch (NumberFormatException var15) {
            throw new IllegalArgumentException(String.format("Invalid value '%s' for the 'timeout_sec' option, not a number", new Object[]{timeoutStr}));
         }

         if(timeout <= 0L) {
            throw new IllegalArgumentException(String.format("Invalid value '%s' for the 'timeout_sec' option, must be strictly positive", new Object[]{timeoutStr}));
         }
      }

      String tablesStr = (String)map.remove("tables");
      Set<TableId> tables = null;
      if(tablesStr != null) {
         tables = new HashSet();
         Iterator var11 = Splitter.on(',').trimResults().omitEmptyStrings().split(tablesStr).iterator();

         while(var11.hasNext()) {
            String s = (String)var11.next();
            List<String> kt = Splitter.on('.').splitToList(s);
            if(kt.size() != 2) {
               throw new IllegalArgumentException(String.format("Invalid fully-qualified table name '%s' in 'tables' option", new Object[]{s}));
            }

            TableMetadata table = Schema.instance.getTableMetadata((String)kt.get(0), (String)kt.get(1));
            if(table == null) {
               throw new IllegalArgumentException(String.format("Unknown table '%s.%s' in 'tables' option", new Object[]{kt.get(0), kt.get(1)}));
            }

            tables.add(table.id);
         }
      }

      if(!map.isEmpty()) {
         throw new IllegalArgumentException("Invalid options provided: " + map.keySet());
      } else {
         return new TracingOptions(id, level, timeout, tables);
      }
   }

   public static class OptionSerializer extends VersionDependent<NodeSyncVerbs.NodeSyncVersion> implements Serializer<TracingOptions> {
      public OptionSerializer(NodeSyncVerbs.NodeSyncVersion version) {
         super(version);
      }

      public void serialize(TracingOptions options, DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(options.id, out);
         out.writeBoolean(options.level == TracingLevel.HIGH);
         out.writeLong(options.timeoutSec);
         Set<TableId> tables = options.tables;
         out.writeInt(tables == null?-1:tables.size());
         if(tables != null) {
            Iterator var4 = options.tables.iterator();

            while(var4.hasNext()) {
               TableId table = (TableId)var4.next();
               table.serialize(out);
            }
         }

      }

      public TracingOptions deserialize(DataInputPlus in) throws IOException {
         UUID id = UUIDSerializer.serializer.deserialize(in);
         TracingLevel level = in.readBoolean()?TracingLevel.HIGH:TracingLevel.LOW;
         long timeoutSec = in.readLong();
         Set<TableId> tables = null;
         int tablesSize = in.readInt();
         if(tablesSize >= 0) {
            tables = new HashSet(tablesSize);

            for(int i = 0; i < tablesSize; ++i) {
               tables.add(TableId.deserialize(in));
            }
         }

         return new TracingOptions(id, level, timeoutSec, tables);
      }

      public long serializedSize(TracingOptions options) {
         long size = UUIDSerializer.serializer.serializedSize(options.id);
         size += 13L;
         TableId table;
         if(options.tables != null) {
            for(Iterator var4 = options.tables.iterator(); var4.hasNext(); size += (long)table.serializedSize()) {
               table = (TableId)var4.next();
            }
         }

         return size;
      }
   }
}
