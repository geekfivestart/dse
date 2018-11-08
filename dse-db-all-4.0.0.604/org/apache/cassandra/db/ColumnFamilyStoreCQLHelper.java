package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnFamilyStoreCQLHelper {
   public ColumnFamilyStoreCQLHelper() {
   }

   public static List<String> dumpReCreateStatements(TableMetadata metadata) {
      List<String> l = new ArrayList();
      l.addAll(getUserTypesAsCQL(metadata));
      l.add(getTableMetadataAsCQL(metadata, true));
      l.addAll(getDroppedColumnsAsCQL(metadata));
      l.addAll(getIndexesAsCQL(metadata));
      return l;
   }

   private static List<ColumnMetadata> getClusteringColumns(TableMetadata metadata) {
      List<ColumnMetadata> cds = new ArrayList(metadata.clusteringColumns().size());
      if(!metadata.isStaticCompactTable()) {
         UnmodifiableIterator var2 = metadata.clusteringColumns().iterator();

         while(var2.hasNext()) {
            ColumnMetadata cd = (ColumnMetadata)var2.next();
            cds.add(cd);
         }
      }

      return cds;
   }

   private static List<ColumnMetadata> getPartitionColumns(TableMetadata metadata) {
      List<ColumnMetadata> cds = new ArrayList(metadata.regularAndStaticColumns().size());
      Iterator var2 = metadata.staticColumns().iterator();

      ColumnMetadata cd;
      while(var2.hasNext()) {
         cd = (ColumnMetadata)var2.next();
         cds.add(cd);
      }

      if(metadata.isDense()) {
         var2 = metadata.regularColumns().iterator();

         while(var2.hasNext()) {
            cd = (ColumnMetadata)var2.next();
            if(!cd.type.equals(EmptyType.instance)) {
               cds.add(cd);
            }
         }
      } else if(!metadata.isStaticCompactTable()) {
         var2 = metadata.regularColumns().iterator();

         while(var2.hasNext()) {
            cd = (ColumnMetadata)var2.next();
            cds.add(cd);
         }
      }

      return cds;
   }

   @VisibleForTesting
   public static String getTableMetadataAsCQL(TableMetadata metadata, boolean includeDroppedColumns) {
      StringBuilder sb = new StringBuilder();
      if(!isCqlCompatible(metadata)) {
         sb.append(String.format("/*\nWarning: Table %s omitted because it has constructs not compatible with CQL (was created via legacy API).\n", new Object[]{metadata.toString()}));
         sb.append("\nApproximate structure, for reference:");
         sb.append("\n(this should not be used to reproduce this schema)\n\n");
      }

      List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);
      if(metadata.isView()) {
         sb.append(getViewMetadataAsCQL(metadata, includeDroppedColumns));
      } else {
         sb.append(getBaseTableMetadataAsCQL(metadata, includeDroppedColumns));
      }

      sb.append("WITH ");
      sb.append("ID = ").append(metadata.id).append("\n\tAND ");
      if(metadata.isCompactTable()) {
         sb.append("COMPACT STORAGE\n\tAND ");
      }

      if(clusteringColumns.size() > 0) {
         sb.append("CLUSTERING ORDER BY (");
         Consumer<StringBuilder> cOrderCommaAppender = commaAppender(" ");
         Iterator var5 = clusteringColumns.iterator();

         while(var5.hasNext()) {
            ColumnMetadata cd = (ColumnMetadata)var5.next();
            cOrderCommaAppender.accept(sb);
            sb.append(cd.name.toCQLString()).append(' ').append(cd.clusteringOrder().toString());
         }

         sb.append(")\n\tAND ");
      }

      sb.append(toCQL(metadata.params));
      sb.append(";");
      if(!isCqlCompatible(metadata)) {
         sb.append("\n*/");
      }

      return sb.toString();
   }

   private static String getViewMetadataAsCQL(TableMetadata metadata, boolean includeDroppedColumns) {
      assert metadata.isView();

      KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(metadata.keyspace);

      assert keyspaceMetadata != null;

      ViewMetadata viewMetadata = (ViewMetadata)keyspaceMetadata.views.get(metadata.name).orElse((Object)null);

      assert viewMetadata != null;

      List<ColumnMetadata> partitionKeyColumns = metadata.partitionKeyColumns();
      List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);
      StringBuilder sb = new StringBuilder();
      sb.append("CREATE MATERIALIZED VIEW IF NOT EXISTS ").append(metadata.toString()).append(" AS SELECT ");
      if(viewMetadata.includeAllColumns) {
         sb.append("*");
      } else {
         boolean isFirst = true;
         List<ColumnMetadata> columns = new ArrayList(metadata.columns());
         columns.sort(Comparator.comparing((c) -> {
            return c.name.toCQLString();
         }));

         for(Iterator var9 = columns.iterator(); var9.hasNext(); isFirst = false) {
            ColumnMetadata column = (ColumnMetadata)var9.next();
            if(!isFirst) {
               sb.append(", ");
            }

            sb.append(column.name.toCQLString());
         }
      }

      sb.append(" FROM ").append(ColumnIdentifier.maybeQuote(viewMetadata.keyspace)).append(".").append(ColumnIdentifier.maybeQuote(viewMetadata.baseTableName)).append("\n\t");
      sb.append("WHERE ").append(viewMetadata.whereClause).append("\n\t");
      if(clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1) {
         sb.append("PRIMARY KEY (");
         if(partitionKeyColumns.size() <= 1) {
            sb.append(((ColumnMetadata)partitionKeyColumns.get(0)).name.toCQLString());
         } else {
            sb.append("(");
            Consumer<StringBuilder> pkCommaAppender = commaAppender(" ");
            Iterator var13 = partitionKeyColumns.iterator();

            while(var13.hasNext()) {
               ColumnMetadata cfd = (ColumnMetadata)var13.next();
               pkCommaAppender.accept(sb);
               sb.append(cfd.name.toCQLString());
            }

            sb.append(")");
         }

         UnmodifiableIterator var12 = metadata.clusteringColumns().iterator();

         while(var12.hasNext()) {
            ColumnMetadata cfd = (ColumnMetadata)var12.next();
            sb.append(", ").append(cfd.name.toCQLString());
         }

         sb.append(')');
      }

      sb.append("\n\t");
      return sb.toString();
   }

   @VisibleForTesting
   public static String getBaseTableMetadataAsCQL(TableMetadata metadata, boolean includeDroppedColumns) {
      StringBuilder sb = new StringBuilder();
      sb.append("CREATE TABLE IF NOT EXISTS ");
      sb.append(metadata.toString()).append(" (");
      List<ColumnMetadata> partitionKeyColumns = metadata.partitionKeyColumns();
      List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);
      List<ColumnMetadata> partitionColumns = getPartitionColumns(metadata);
      Consumer<StringBuilder> cdCommaAppender = commaAppender("\n\t");
      sb.append("\n\t");
      Iterator var7 = partitionKeyColumns.iterator();

      ColumnMetadata cfd;
      while(var7.hasNext()) {
         cfd = (ColumnMetadata)var7.next();
         cdCommaAppender.accept(sb);
         sb.append(toCQL(cfd));
         if(partitionKeyColumns.size() == 1 && clusteringColumns.size() == 0) {
            sb.append(" PRIMARY KEY");
         }
      }

      var7 = clusteringColumns.iterator();

      while(var7.hasNext()) {
         cfd = (ColumnMetadata)var7.next();
         cdCommaAppender.accept(sb);
         sb.append(toCQL(cfd));
      }

      var7 = partitionColumns.iterator();

      while(var7.hasNext()) {
         cfd = (ColumnMetadata)var7.next();
         cdCommaAppender.accept(sb);
         sb.append(toCQL(cfd, metadata.isStaticCompactTable()));
      }

      UnmodifiableIterator var10;
      if(includeDroppedColumns) {
         var10 = metadata.droppedColumns.entrySet().iterator();

         while(var10.hasNext()) {
            Entry<ByteBuffer, DroppedColumn> entry = (Entry)var10.next();
            if(metadata.getColumn((ByteBuffer)entry.getKey()) == null) {
               DroppedColumn droppedColumn = (DroppedColumn)entry.getValue();
               cdCommaAppender.accept(sb);
               sb.append(droppedColumn.column.name.toCQLString());
               sb.append(' ');
               sb.append(droppedColumn.column.type.asCQL3Type().toString());
            }
         }
      }

      if(clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1) {
         sb.append(",\n\tPRIMARY KEY (");
         if(partitionKeyColumns.size() <= 1) {
            sb.append(((ColumnMetadata)partitionKeyColumns.get(0)).name.toCQLString());
         } else {
            sb.append("(");
            Consumer<StringBuilder> pkCommaAppender = commaAppender(" ");
            Iterator var13 = partitionKeyColumns.iterator();

            while(var13.hasNext()) {
               ColumnMetadata cfd = (ColumnMetadata)var13.next();
               pkCommaAppender.accept(sb);
               sb.append(cfd.name.toCQLString());
            }

            sb.append(")");
         }

         var10 = metadata.clusteringColumns().iterator();

         while(var10.hasNext()) {
            cfd = (ColumnMetadata)var10.next();
            sb.append(", ").append(cfd.name.toCQLString());
         }

         sb.append(')');
      }

      sb.append(")\n\t");
      return sb.toString();
   }

   @VisibleForTesting
   public static List<String> getUserTypesAsCQL(TableMetadata metadata) {
      List<AbstractType> types = new ArrayList();
      Set<AbstractType> typeSet = new HashSet();
      Iterator var3 = Iterables.concat(metadata.partitionKeyColumns(), metadata.clusteringColumns(), metadata.regularAndStaticColumns()).iterator();

      AbstractType type;
      while(var3.hasNext()) {
         ColumnMetadata cd = (ColumnMetadata)var3.next();
         type = cd.type;
         if(type.isUDT()) {
            resolveUserType((UserType)type, typeSet, types);
         }
      }

      List<String> typeStrings = new ArrayList(types.size());
      Iterator var7 = types.iterator();

      while(var7.hasNext()) {
         type = (AbstractType)var7.next();
         typeStrings.add(toCQL((UserType)type));
      }

      return typeStrings;
   }

   @VisibleForTesting
   public static List<String> getDroppedColumnsAsCQL(TableMetadata metadata) {
      List<String> droppedColumns = new ArrayList();
      UnmodifiableIterator var2 = metadata.droppedColumns.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<ByteBuffer, DroppedColumn> entry = (Entry)var2.next();
         DroppedColumn column = (DroppedColumn)entry.getValue();
         droppedColumns.add(toCQLDrop(metadata, column));
         if(metadata.getColumn((ByteBuffer)entry.getKey()) != null) {
            droppedColumns.add(toCQLAdd(metadata, metadata.getColumn((ByteBuffer)entry.getKey())));
         }
      }

      return droppedColumns;
   }

   @VisibleForTesting
   public static List<String> getIndexesAsCQL(TableMetadata metadata) {
      List<String> indexes = new ArrayList(metadata.indexes.size());
      Iterator var2 = metadata.indexes.iterator();

      while(var2.hasNext()) {
         IndexMetadata indexMetadata = (IndexMetadata)var2.next();
         indexes.add(toCQL(metadata, indexMetadata));
      }

      return indexes;
   }

   private static String toCQL(TableMetadata baseTable, IndexMetadata indexMetadata) {
      if(indexMetadata.isCustom()) {
         Map<String, String> options = new HashMap();
         indexMetadata.options.forEach((k, v) -> {
            if(!k.equals("target") && !k.equals("class_name")) {
               options.put(k, v);
            }

         });
         return String.format("CREATE CUSTOM INDEX %s ON %s (%s) USING '%s'%s;", new Object[]{indexMetadata.toCQLString(), baseTable.toString(), indexMetadata.options.get("target"), indexMetadata.options.get("class_name"), options.isEmpty()?"":" WITH OPTIONS = " + toCQL((Map)options)});
      } else {
         return String.format("CREATE INDEX %s ON %s (%s);", new Object[]{indexMetadata.toCQLString(), baseTable.toString(), indexMetadata.options.get("target")});
      }
   }

   private static String toCQL(UserType userType) {
      StringBuilder sb = new StringBuilder();
      sb.append("CREATE TYPE ").append(userType.toCQLString()).append(" (");
      Consumer<StringBuilder> commaAppender = commaAppender(" ");

      for(int i = 0; i < userType.size(); ++i) {
         commaAppender.accept(sb);
         sb.append(String.format("%s %s", new Object[]{userType.fieldNameAsString(i), userType.fieldType(i).asCQL3Type()}));
      }

      sb.append(");");
      return sb.toString();
   }

   private static String toCQL(TableParams tableParams) {
      StringBuilder builder = new StringBuilder();
      builder.append("bloom_filter_fp_chance = ").append(tableParams.bloomFilterFpChance);
      builder.append("\n\tAND caching = ").append(toCQL(tableParams.caching.asMap()));
      builder.append("\n\tAND cdc = ").append(tableParams.cdc);
      builder.append("\n\tAND comment = ").append(singleQuote(tableParams.comment));
      builder.append("\n\tAND compaction = ").append(toCQL(tableParams.compaction.asMap()));
      builder.append("\n\tAND compression = ").append(toCQL(tableParams.compression.asMap()));
      builder.append("\n\tAND crc_check_chance = ").append(tableParams.crcCheckChance);
      builder.append("\n\tAND dclocal_read_repair_chance = ").append(tableParams.dcLocalReadRepairChance);
      builder.append("\n\tAND default_time_to_live = ").append(tableParams.defaultTimeToLive);
      builder.append("\n\tAND extensions = { ");
      UnmodifiableIterator var2 = tableParams.extensions.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, ByteBuffer> entry = (Entry)var2.next();
         builder.append(singleQuote((String)entry.getKey()));
         builder.append(": ");
         builder.append("0x").append(ByteBufferUtil.bytesToHex((ByteBuffer)entry.getValue()));
      }

      builder.append(" }");
      builder.append("\n\tAND gc_grace_seconds = ").append(tableParams.gcGraceSeconds);
      builder.append("\n\tAND max_index_interval = ").append(tableParams.maxIndexInterval);
      builder.append("\n\tAND memtable_flush_period_in_ms = ").append(tableParams.memtableFlushPeriodInMs);
      builder.append("\n\tAND min_index_interval = ").append(tableParams.minIndexInterval);
      Map<String, String> nodeSyncParams = tableParams.nodeSync.asMap();
      if(!nodeSyncParams.isEmpty()) {
         builder.append("\n\tAND nodesync = ").append(toCQL(nodeSyncParams));
      }

      builder.append("\n\tAND read_repair_chance = ").append(tableParams.readRepairChance);
      builder.append("\n\tAND speculative_retry = '").append(tableParams.speculativeRetry).append("'");
      return builder.toString();
   }

   private static String toCQL(Map<?, ?> map) {
      if(map.isEmpty()) {
         return "{}";
      } else {
         StringBuilder builder = new StringBuilder("{ ");
         boolean isFirst = true;
         Iterator var3 = map.entrySet().iterator();

         while(var3.hasNext()) {
            Entry entry = (Entry)var3.next();
            if(isFirst) {
               isFirst = false;
            } else {
               builder.append(", ");
            }

            builder.append(singleQuote(entry.getKey().toString()));
            builder.append(": ");
            builder.append(singleQuote(entry.getValue().toString()));
         }

         builder.append(" }");
         return builder.toString();
      }
   }

   private static String toCQL(ColumnMetadata cd) {
      return toCQL(cd, false);
   }

   private static String toCQL(ColumnMetadata cd, boolean isStaticCompactTable) {
      return String.format("%s %s%s", new Object[]{cd.name.toCQLString(), cd.type.asCQL3Type().toString(), cd.isStatic() && !isStaticCompactTable?" static":""});
   }

   private static String toCQLAdd(TableMetadata table, ColumnMetadata cd) {
      return String.format("ALTER TABLE %s ADD %s %s%s;", new Object[]{table.toString(), cd.name.toCQLString(), cd.type.asCQL3Type().toString(), cd.isStatic()?" static":""});
   }

   private static String toCQLDrop(TableMetadata table, DroppedColumn droppedColumn) {
      return String.format("ALTER TABLE %s DROP %s USING TIMESTAMP %s;", new Object[]{table.toString(), droppedColumn.column.name.toCQLString(), Long.valueOf(droppedColumn.droppedTime)});
   }

   private static void resolveUserType(UserType type, Set<AbstractType> typeSet, List<AbstractType> types) {
      Iterator var3 = type.fieldTypes().iterator();

      while(var3.hasNext()) {
         AbstractType subType = (AbstractType)var3.next();
         if(!typeSet.contains(subType) && subType.isUDT()) {
            resolveUserType((UserType)subType, typeSet, types);
         }
      }

      if(!typeSet.contains(type)) {
         typeSet.add(type);
         types.add(type);
      }

   }

   private static String singleQuote(String s) {
      return String.format("'%s'", new Object[]{s.replaceAll("'", "''")});
   }

   private static Consumer<StringBuilder> commaAppender(final String afterComma) {
      final AtomicBoolean isFirst = new AtomicBoolean(true);
      return new Consumer<StringBuilder>() {
         public void accept(StringBuilder stringBuilder) {
            if(!isFirst.getAndSet(false)) {
               stringBuilder.append(',').append(afterComma);
            }

         }
      };
   }

   public static boolean isCqlCompatible(TableMetadata metaData) {
      return metaData.isSuper()?false:!metaData.isCompactTable() || metaData.regularColumns().size() <= 1 || metaData.clusteringColumns().size() < 1;
   }
}
