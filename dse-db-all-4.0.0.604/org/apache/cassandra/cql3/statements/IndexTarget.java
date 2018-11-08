package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class IndexTarget {
   public static final String TARGET_OPTION_NAME = "target";
   public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";
   public final ColumnIdentifier column;
   public final IndexTarget.Type type;

   public IndexTarget(ColumnIdentifier column, IndexTarget.Type type) {
      this.column = column;
      this.type = type;
   }

   public String asCqlString() {
      return this.type == IndexTarget.Type.SIMPLE?this.column.toCQLString():String.format("%s(%s)", new Object[]{this.type.toString(), this.column.toCQLString()});
   }

   public static enum Type {
      VALUES,
      KEYS,
      KEYS_AND_VALUES,
      FULL,
      SIMPLE;

      private Type() {
      }

      public String toString() {
         switch(null.$SwitchMap$org$apache$cassandra$cql3$statements$IndexTarget$Type[this.ordinal()]) {
         case 1:
            return "keys";
         case 2:
            return "entries";
         case 3:
            return "full";
         case 4:
            return "values";
         case 5:
            return "";
         default:
            return "";
         }
      }

      public static IndexTarget.Type fromString(String s) {
         if("".equals(s)) {
            return SIMPLE;
         } else if("values".equals(s)) {
            return VALUES;
         } else if("keys".equals(s)) {
            return KEYS;
         } else if("entries".equals(s)) {
            return KEYS_AND_VALUES;
         } else if("full".equals(s)) {
            return FULL;
         } else {
            throw new AssertionError("Unrecognized index target type " + s);
         }
      }
   }

   public static class Raw {
      private final ColumnMetadata.Raw column;
      private final IndexTarget.Type type;

      private Raw(ColumnMetadata.Raw column, IndexTarget.Type type) {
         this.column = column;
         this.type = type;
      }

      public static IndexTarget.Raw simpleIndexOn(ColumnMetadata.Raw c) {
         return new IndexTarget.Raw(c, IndexTarget.Type.SIMPLE);
      }

      public static IndexTarget.Raw valuesOf(ColumnMetadata.Raw c) {
         return new IndexTarget.Raw(c, IndexTarget.Type.VALUES);
      }

      public static IndexTarget.Raw keysOf(ColumnMetadata.Raw c) {
         return new IndexTarget.Raw(c, IndexTarget.Type.KEYS);
      }

      public static IndexTarget.Raw keysAndValuesOf(ColumnMetadata.Raw c) {
         return new IndexTarget.Raw(c, IndexTarget.Type.KEYS_AND_VALUES);
      }

      public static IndexTarget.Raw fullCollection(ColumnMetadata.Raw c) {
         return new IndexTarget.Raw(c, IndexTarget.Type.FULL);
      }

      public IndexTarget prepare(TableMetadata table) {
         ColumnMetadata columnDef = this.column.prepare(table);
         IndexTarget.Type actualType = this.type == IndexTarget.Type.SIMPLE && columnDef.type.isCollection()?IndexTarget.Type.VALUES:this.type;
         return new IndexTarget(columnDef.name, actualType);
      }
   }
}
