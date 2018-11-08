package org.apache.cassandra.cql3.selection;

import java.util.List;
import java.util.function.Predicate;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

final class AliasedSelectable implements Selectable {
   private final Selectable selectable;
   private final ColumnIdentifier alias;

   public AliasedSelectable(Selectable selectable, ColumnIdentifier alias) {
      this.selectable = selectable;
      this.alias = alias;
   }

   public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
      return this.selectable.testAssignment(keyspace, receiver);
   }

   public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
      final Selector.Factory delegate = this.selectable.newSelectorFactory(table, expectedType, defs, boundNames);
      final ColumnSpecification columnSpec = delegate.getColumnSpecification(table).withAlias(this.alias);
      return new ForwardingFactory() {
         protected Selector.Factory delegate() {
            return delegate;
         }

         public ColumnSpecification getColumnSpecification(TableMetadata table) {
            return columnSpec;
         }
      };
   }

   public AbstractType<?> getExactTypeIfKnown(String keyspace) {
      return this.selectable.getExactTypeIfKnown(keyspace);
   }

   public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
      return this.selectable.selectColumns(predicate);
   }
}
