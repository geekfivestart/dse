package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.schema.ColumnMetadata;

public class AlterTableStatementColumn {
   private final CQL3Type.Raw dataType;
   private final ColumnMetadata.Raw colName;
   private final Boolean isStatic;

   public AlterTableStatementColumn(ColumnMetadata.Raw colName, CQL3Type.Raw dataType, boolean isStatic) {
      assert colName != null;

      this.dataType = dataType;
      this.colName = colName;
      this.isStatic = Boolean.valueOf(isStatic);
   }

   public AlterTableStatementColumn(ColumnMetadata.Raw colName, CQL3Type.Raw dataType) {
      this(colName, dataType, false);
   }

   public AlterTableStatementColumn(ColumnMetadata.Raw colName) {
      this(colName, (CQL3Type.Raw)null, false);
   }

   public CQL3Type.Raw getColumnType() {
      return this.dataType;
   }

   public ColumnMetadata.Raw getColumnName() {
      return this.colName;
   }

   public Boolean getStaticType() {
      return this.isStatic;
   }
}
