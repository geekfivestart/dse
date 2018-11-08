package org.apache.cassandra.cql3.restrictions;

import java.util.function.Supplier;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class CustomIndexExpression implements ExternalRestriction {
   private final ColumnIdentifier valueColId = new ColumnIdentifier("custom index expression", false);
   public final IndexName targetIndex;
   public final Term.Raw valueRaw;
   private Term value;

   public CustomIndexExpression(IndexName targetIndex, Term.Raw value) {
      this.targetIndex = targetIndex;
      this.valueRaw = value;
   }

   public void prepareValue(TableMetadata table, AbstractType<?> expressionType, VariableSpecifications boundNames) {
      ColumnSpecification spec = new ColumnSpecification(table.keyspace, table.keyspace, this.valueColId, expressionType);
      this.value = this.valueRaw.prepare(table.keyspace, spec);
      this.value.collectMarkerSpecification(boundNames);
   }

   public void addToRowFilter(RowFilter filter, TableMetadata table, QueryOptions options) {
      filter.addCustomIndexExpression(table, (IndexMetadata)table.indexes.get(this.targetIndex.getIdx()).orElseThrow(() -> {
         return IndexRestrictions.indexNotFound(this.targetIndex, table);
      }), this.value.bindAndGet(options));
   }
}
