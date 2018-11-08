package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CompactTables {
   public static final ByteBuffer SUPER_COLUMN_MAP_COLUMN;

   private CompactTables() {
   }

   public static ColumnMetadata getCompactValueColumn(RegularAndStaticColumns columns, boolean isSuper) {
      if(isSuper) {
         Iterator var2 = columns.regulars.iterator();

         ColumnMetadata column;
         do {
            if(!var2.hasNext()) {
               throw new AssertionError("Invalid super column table definition, no 'dynamic' map column");
            }

            column = (ColumnMetadata)var2.next();
         } while(!column.name.bytes.equals(SUPER_COLUMN_MAP_COLUMN));

         return column;
      } else {
         assert columns.regulars.simpleColumnCount() == 1 && columns.regulars.complexColumnCount() == 0;

         return columns.regulars.getSimple(0);
      }
   }

   public static boolean hasEmptyCompactValue(TableMetadata metadata) {
      return metadata.compactValueColumn.type instanceof EmptyType;
   }

   public static boolean isSuperColumnMapColumn(ColumnMetadata column) {
      return column.kind == ColumnMetadata.Kind.REGULAR && column.name.bytes.equals(SUPER_COLUMN_MAP_COLUMN);
   }

   public static CompactTables.DefaultNames defaultNameGenerator(Set<String> usedNames) {
      return new CompactTables.DefaultNames(new HashSet(usedNames));
   }

   static {
      SUPER_COLUMN_MAP_COLUMN = ByteBufferUtil.EMPTY_BYTE_BUFFER;
   }

   public static class DefaultNames {
      private static final String DEFAULT_CLUSTERING_NAME = "column";
      private static final String DEFAULT_COMPACT_VALUE_NAME = "value";
      private final Set<String> usedNames;
      private int clusteringIndex;
      private int compactIndex;

      private DefaultNames(Set<String> usedNames) {
         this.clusteringIndex = 1;
         this.compactIndex = 0;
         this.usedNames = usedNames;
      }

      public String defaultClusteringName() {
         String candidate;
         do {
            candidate = "column" + this.clusteringIndex;
            ++this.clusteringIndex;
         } while(!this.usedNames.add(candidate));

         return candidate;
      }

      public String defaultCompactValueName() {
         String candidate;
         do {
            candidate = this.compactIndex == 0?"value":"value" + this.compactIndex;
            ++this.compactIndex;
         } while(!this.usedNames.add(candidate));

         return candidate;
      }
   }
}
