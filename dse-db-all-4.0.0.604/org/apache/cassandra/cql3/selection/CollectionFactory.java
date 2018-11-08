package org.apache.cassandra.cql3.selection;

import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

abstract class CollectionFactory extends Selector.Factory {
   private final AbstractType<?> type;
   private final SelectorFactories factories;

   public CollectionFactory(AbstractType<?> type, SelectorFactories factories) {
      this.type = type;
      this.factories = factories;
   }

   protected final AbstractType<?> getReturnType() {
      return this.type;
   }

   public final void addFunctionsTo(List<Function> functions) {
      this.factories.addFunctionsTo(functions);
   }

   public final boolean isAggregateSelectorFactory() {
      return this.factories.doesAggregation();
   }

   public final boolean isWritetimeSelectorFactory() {
      return this.factories.containsWritetimeSelectorFactory();
   }

   public final boolean isTTLSelectorFactory() {
      return this.factories.containsTTLSelectorFactory();
   }

   boolean areAllFetchedColumnsKnown() {
      return this.factories.areAllFetchedColumnsKnown();
   }

   void addFetchedColumns(ColumnFilter.Builder builder) {
      this.factories.addFetchedColumns(builder);
   }

   protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
      SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
      Iterator var4 = this.factories.iterator();

      while(var4.hasNext()) {
         Selector.Factory factory = (Selector.Factory)var4.next();
         factory.addColumnMapping(tmpMapping, resultsColumn);
      }

      if(tmpMapping.getMappings().get(resultsColumn).isEmpty()) {
         mapping.addMapping(resultsColumn, (ColumnMetadata)null);
      } else {
         mapping.addMapping(resultsColumn, (Iterable)tmpMapping.getMappings().values());
      }

   }
}
