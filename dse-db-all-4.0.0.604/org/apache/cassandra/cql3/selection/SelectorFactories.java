package org.apache.cassandra.cql3.selection;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

final class SelectorFactories implements Iterable<Selector.Factory> {
   private final List<Selector.Factory> factories;
   private boolean containsWritetimeFactory;
   private boolean containsTTLFactory;
   private int numberOfAggregateFactories;

   public static SelectorFactories createFactoriesAndCollectColumnDefinitions(List<Selectable> selectables, List<AbstractType<?>> expectedTypes, TableMetadata table, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException {
      return new SelectorFactories(selectables, expectedTypes, table, defs, boundNames);
   }

   private SelectorFactories(List<Selectable> selectables, List<AbstractType<?>> expectedTypes, TableMetadata table, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException {
      this.factories = new ArrayList(selectables.size());

      for(int i = 0; i < selectables.size(); ++i) {
         Selectable selectable = (Selectable)selectables.get(i);
         AbstractType<?> expectedType = expectedTypes == null?null:(AbstractType)expectedTypes.get(i);
         Selector.Factory factory = selectable.newSelectorFactory(table, expectedType, defs, boundNames);
         this.containsWritetimeFactory |= factory.isWritetimeSelectorFactory();
         this.containsTTLFactory |= factory.isTTLSelectorFactory();
         if(factory.isAggregateSelectorFactory()) {
            ++this.numberOfAggregateFactories;
         }

         this.factories.add(factory);
      }

   }

   public void addFunctionsTo(List<Function> functions) {
      this.factories.forEach((p) -> {
         p.addFunctionsTo(functions);
      });
   }

   public Selector.Factory get(int i) {
      return (Selector.Factory)this.factories.get(i);
   }

   public int indexOfSimpleSelectorFactory(int columnIndex) {
      int i = 0;

      for(int m = this.factories.size(); i < m; ++i) {
         if(((Selector.Factory)this.factories.get(i)).isSimpleSelectorFactoryFor(columnIndex)) {
            return i;
         }
      }

      return -1;
   }

   public void addSelectorForOrdering(ColumnMetadata def, int index) {
      this.factories.add(SimpleSelector.newFactory(def, index));
   }

   public boolean doesAggregation() {
      return this.numberOfAggregateFactories > 0;
   }

   public boolean containsWritetimeSelectorFactory() {
      return this.containsWritetimeFactory;
   }

   public boolean containsTTLSelectorFactory() {
      return this.containsTTLFactory;
   }

   public List<Selector> newInstances(QueryOptions options) throws InvalidRequestException {
      List<Selector> selectors = new ArrayList(this.factories.size());
      Iterator var3 = this.factories.iterator();

      while(var3.hasNext()) {
         Selector.Factory factory = (Selector.Factory)var3.next();
         selectors.add(factory.newInstance(options));
      }

      return selectors;
   }

   public Iterator<Selector.Factory> iterator() {
      return this.factories.iterator();
   }

   public List<String> getColumnNames() {
      return Lists.transform(this.factories, new com.google.common.base.Function<Selector.Factory, String>() {
         public String apply(Selector.Factory factory) {
            return factory.getColumnName();
         }
      });
   }

   public List<AbstractType<?>> getReturnTypes() {
      return Lists.transform(this.factories, new com.google.common.base.Function<Selector.Factory, AbstractType<?>>() {
         public AbstractType<?> apply(Selector.Factory factory) {
            return factory.getReturnType();
         }
      });
   }

   boolean areAllFetchedColumnsKnown() {
      Iterator var1 = this.factories.iterator();

      Selector.Factory factory;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         factory = (Selector.Factory)var1.next();
      } while(factory.areAllFetchedColumnsKnown());

      return false;
   }

   void addFetchedColumns(ColumnFilter.Builder builder) {
      Iterator var2 = this.factories.iterator();

      while(var2.hasNext()) {
         Selector.Factory factory = (Selector.Factory)var2.next();
         factory.addFetchedColumns(builder);
      }

   }

   public int size() {
      return this.factories.size();
   }
}
