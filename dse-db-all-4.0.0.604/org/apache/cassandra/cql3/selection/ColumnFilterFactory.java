package org.apache.cassandra.cql3.selection;

import java.util.List;
import java.util.Set;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

abstract class ColumnFilterFactory {
   ColumnFilterFactory() {
   }

   abstract ColumnFilter newInstance(List<Selector> var1);

   public static ColumnFilterFactory wildcard(TableMetadata table) {
      return new ColumnFilterFactory.PrecomputedColumnFilter(ColumnFilter.all(table));
   }

   public static ColumnFilterFactory fromColumns(TableMetadata table, List<ColumnMetadata> selectedColumns, Set<ColumnMetadata> orderingColumns, Set<ColumnMetadata> nonPKRestrictedColumns) {
      ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(table);
      builder.addAll(selectedColumns);
      builder.addAll(orderingColumns);
      builder.addAll(nonPKRestrictedColumns);
      return new ColumnFilterFactory.PrecomputedColumnFilter(builder.build());
   }

   public static ColumnFilterFactory fromSelectorFactories(TableMetadata table, SelectorFactories factories, Set<ColumnMetadata> orderingColumns, Set<ColumnMetadata> nonPKRestrictedColumns) {
      if(factories.areAllFetchedColumnsKnown()) {
         ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(table);
         factories.addFetchedColumns(builder);
         builder.addAll(orderingColumns);
         builder.addAll(nonPKRestrictedColumns);
         return new ColumnFilterFactory.PrecomputedColumnFilter(builder.build());
      } else {
         return new ColumnFilterFactory.OnRequestColumnFilterFactory(table, nonPKRestrictedColumns);
      }
   }

   private static class OnRequestColumnFilterFactory extends ColumnFilterFactory {
      private final TableMetadata table;
      private final Set<ColumnMetadata> nonPKRestrictedColumns;

      public OnRequestColumnFilterFactory(TableMetadata table, Set<ColumnMetadata> nonPKRestrictedColumns) {
         this.table = table;
         this.nonPKRestrictedColumns = nonPKRestrictedColumns;
      }

      public ColumnFilter newInstance(List<Selector> selectors) {
         ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(this.table);
         int i = 0;

         for(int m = selectors.size(); i < m; ++i) {
            ((Selector)selectors.get(i)).addFetchedColumns(builder);
         }

         builder.addAll(this.nonPKRestrictedColumns);
         return builder.build();
      }
   }

   private static class PrecomputedColumnFilter extends ColumnFilterFactory {
      private final ColumnFilter columnFilter;

      public PrecomputedColumnFilter(ColumnFilter columnFilter) {
         this.columnFilter = columnFilter;
      }

      public ColumnFilter newInstance(List<Selector> selectors) {
         return this.columnFilter;
      }
   }
}
