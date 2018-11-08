package org.apache.cassandra.schema;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class ViewMetadata {
   public final String keyspace;
   public final String name;
   public final TableId baseTableId;
   public final String baseTableName;
   public final boolean includeAllColumns;
   public final TableMetadata metadata;
   public final SelectStatement.RawStatement select;
   public final String whereClause;

   public ViewMetadata(String keyspace, String name, TableId baseTableId, String baseTableName, boolean includeAllColumns, SelectStatement.RawStatement select, String whereClause, TableMetadata metadata) {
      this.keyspace = keyspace;
      this.name = name;
      this.baseTableId = baseTableId;
      this.baseTableName = baseTableName;
      this.includeAllColumns = includeAllColumns;
      this.select = select;
      this.whereClause = whereClause;
      this.metadata = metadata;
   }

   public boolean includes(ColumnIdentifier column) {
      return this.metadata.getColumn(column) != null;
   }

   public ViewMetadata copy(TableMetadata newMetadata) {
      return new ViewMetadata(this.keyspace, this.name, this.baseTableId, this.baseTableName, this.includeAllColumns, this.select, this.whereClause, newMetadata);
   }

   public TableMetadata baseTableMetadata() {
      return Schema.instance.getTableMetadata(this.baseTableId);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ViewMetadata)) {
         return false;
      } else {
         ViewMetadata other = (ViewMetadata)o;
         return Objects.equals(this.keyspace, other.keyspace) && Objects.equals(this.name, other.name) && Objects.equals(this.baseTableId, other.baseTableId) && Objects.equals(Boolean.valueOf(this.includeAllColumns), Boolean.valueOf(other.includeAllColumns)) && Objects.equals(this.whereClause, other.whereClause) && Objects.equals(this.metadata, other.metadata);
      }
   }

   public int hashCode() {
      return (new HashCodeBuilder(29, 1597)).append(this.keyspace).append(this.name).append(this.baseTableId).append(this.includeAllColumns).append(this.whereClause).append(this.metadata).toHashCode();
   }

   public String toString() {
      return (new ToStringBuilder(this)).append("keyspace", this.keyspace).append("name", this.name).append("baseTableId", this.baseTableId).append("baseTableName", this.baseTableName).append("includeAllColumns", this.includeAllColumns).append("whereClause", this.whereClause).append("metadata", this.metadata).toString();
   }

   public ViewMetadata renamePrimaryKeyColumn(ColumnIdentifier from, ColumnIdentifier to) {
      List<Relation> relations = whereClauseToRelations(this.whereClause);
      ColumnMetadata.Raw fromRaw = ColumnMetadata.Raw.forQuoted(from.toString());
      ColumnMetadata.Raw toRaw = ColumnMetadata.Raw.forQuoted(to.toString());
      List<Relation> newRelations = (List)relations.stream().map((r) -> {
         return r.renameIdentifier(fromRaw, toRaw);
      }).collect(Collectors.toList());
      String rawSelect = View.buildSelectStatement(this.baseTableName, this.metadata.columns(), this.whereClause);
      return new ViewMetadata(this.keyspace, this.name, this.baseTableId, this.baseTableName, this.includeAllColumns, (SelectStatement.RawStatement)QueryProcessor.parseStatement(rawSelect), View.relationsToWhereClause(newRelations), this.metadata.unbuild().renamePrimaryKeyColumn(from, to).build());
   }

   public ViewMetadata withAddedRegularColumn(ColumnMetadata column) {
      return new ViewMetadata(this.keyspace, this.name, this.baseTableId, this.baseTableName, this.includeAllColumns, this.select, this.whereClause, this.metadata.unbuild().addColumn(column).build());
   }

   public ViewMetadata withAlteredColumnType(ColumnIdentifier name, AbstractType<?> type) {
      return new ViewMetadata(this.keyspace, this.name, this.baseTableId, this.baseTableName, this.includeAllColumns, this.select, this.whereClause, this.metadata.unbuild().alterColumnType(name, type).build());
   }

   private static List<Relation> whereClauseToRelations(String whereClause) {
      try {
         return ((WhereClause.Builder)CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, whereClause)).build().relations;
      } catch (SyntaxException | RecognitionException var2) {
         throw new RuntimeException("Unexpected error parsing materialized view's where clause while handling column rename: ", var2);
      }
   }
}
