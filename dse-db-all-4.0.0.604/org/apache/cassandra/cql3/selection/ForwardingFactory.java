package org.apache.cassandra.cql3.selection;

import java.util.List;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

abstract class ForwardingFactory extends Selector.Factory {
   ForwardingFactory() {
   }

   protected abstract Selector.Factory delegate();

   public Selector newInstance(QueryOptions options) throws InvalidRequestException {
      return this.delegate().newInstance(options);
   }

   protected String getColumnName() {
      return this.delegate().getColumnName();
   }

   protected AbstractType<?> getReturnType() {
      return this.delegate().getReturnType();
   }

   protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
      this.delegate().addColumnMapping(mapping, resultsColumn);
   }

   public void addFunctionsTo(List<Function> functions) {
      this.delegate().addFunctionsTo(functions);
   }

   public boolean isAggregateSelectorFactory() {
      return this.delegate().isAggregateSelectorFactory();
   }

   public boolean isWritetimeSelectorFactory() {
      return this.delegate().isWritetimeSelectorFactory();
   }

   public boolean isTTLSelectorFactory() {
      return this.delegate().isTTLSelectorFactory();
   }

   public boolean isSimpleSelectorFactory() {
      return this.delegate().isSimpleSelectorFactory();
   }

   public boolean isSimpleSelectorFactoryFor(int index) {
      return this.delegate().isSimpleSelectorFactoryFor(index);
   }

   boolean areAllFetchedColumnsKnown() {
      return this.delegate().areAllFetchedColumnsKnown();
   }

   void addFetchedColumns(ColumnFilter.Builder builder) {
      this.delegate().addFetchedColumns(builder);
   }
}
