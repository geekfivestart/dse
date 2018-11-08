package org.apache.cassandra.cql3.selection;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class Selection {
   private static final Predicate<ColumnMetadata> STATIC_COLUMN_FILTER = (column) -> {
      return column.isStatic();
   };
   private final TableMetadata table;
   final List<ColumnMetadata> columns;
   private final SelectionColumnMapping columnMapping;
   final ResultSet.ResultMetadata metadata;
   protected final ColumnFilterFactory columnFilterFactory;
   protected final boolean isJson;
   protected final List<ColumnMetadata> orderingColumns;

   protected Selection(TableMetadata table, List<ColumnMetadata> selectedColumns, Set<ColumnMetadata> orderingColumns, SelectionColumnMapping columnMapping, ColumnFilterFactory columnFilterFactory, boolean isJson) {
      this.table = table;
      this.columns = selectedColumns;
      this.columnMapping = columnMapping;
      this.metadata = new ResultSet.ResultMetadata(columnMapping.getColumnSpecifications());
      this.columnFilterFactory = columnFilterFactory;
      this.isJson = isJson;
      this.columns.addAll(orderingColumns);
      this.metadata.addNonSerializedColumns(orderingColumns);
      this.orderingColumns = (List)(orderingColumns.isEmpty()?Collections.emptyList():new ArrayList(orderingColumns));
   }

   public boolean isWildcard() {
      return false;
   }

   public final List<ColumnSpecification> getSelectedColumns() {
      return new ArrayList(this.metadata.requestNames());
   }

   public boolean containsStaticColumns() {
      return !this.table.isStaticCompactTable() && this.table.hasStaticColumns()?(this.isWildcard()?true:!Iterables.isEmpty(Iterables.filter(this.columns, STATIC_COLUMN_FILTER))):false;
   }

   public Integer getOrderingIndex(ColumnMetadata c) {
      return !this.isJson?Integer.valueOf(this.getResultSetIndex(c)):Integer.valueOf(this.orderingColumns.indexOf(c) + 1);
   }

   public ResultSet.ResultMetadata getResultMetadata() {
      if(!this.isJson) {
         return this.metadata.copy();
      } else {
         ColumnSpecification firstColumn = (ColumnSpecification)this.metadata.names.get(0);
         ColumnSpecification jsonSpec = new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, Json.JSON_COLUMN_ID, UTF8Type.instance);
         ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Lists.newArrayList(new ColumnSpecification[]{jsonSpec}));
         resultMetadata.addNonSerializedColumns(this.orderingColumns);
         return resultMetadata;
      }
   }

   public static Selection wildcard(TableMetadata table, boolean isJson) {
      List<ColumnMetadata> all = new ArrayList(table.columns().size());
      Iterators.addAll(all, table.allColumnsInSelectOrder());
      return new Selection.SimpleSelection(table, all, Collections.emptySet(), true, isJson);
   }

   public static Selection wildcardWithGroupBy(TableMetadata table, VariableSpecifications boundNames, boolean isJson) {
      return fromSelectors(table, Lists.newArrayList(table.allColumnsInSelectOrder()), boundNames, Collections.emptySet(), Collections.emptySet(), true, isJson);
   }

   public static Selection forColumns(TableMetadata table, List<ColumnMetadata> columns) {
      return new Selection.SimpleSelection(table, columns, Collections.emptySet(), false, false);
   }

   public void addFunctionsTo(List<Function> functions) {
   }

   private static boolean processesSelection(List<Selectable> selectables) {
      Iterator var1 = selectables.iterator();

      Selectable selectable;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         selectable = (Selectable)var1.next();
      } while(!selectable.processesSelection());

      return true;
   }

   public static Selection fromSelectors(TableMetadata table, List<Selectable> selectables, VariableSpecifications boundNames, Set<ColumnMetadata> orderingColumns, Set<ColumnMetadata> nonPKRestrictedColumns, boolean hasGroupBy, boolean isJson) {
      List<ColumnMetadata> selectedColumns = new ArrayList();
      SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(selectables, (List)null, table, selectedColumns, boundNames);
      SelectionColumnMapping mapping = collectColumnMappings(table, factories);
      Set<ColumnMetadata> filteredOrderingColumns = filterOrderingColumns(orderingColumns, selectedColumns, factories, isJson);
      return (Selection)(!processesSelection(selectables) && selectables.size() == selectedColumns.size() && !hasGroupBy?new Selection.SimpleSelection(table, selectedColumns, filteredOrderingColumns, nonPKRestrictedColumns, mapping, isJson):new Selection.SelectionWithProcessing(table, selectedColumns, filteredOrderingColumns, nonPKRestrictedColumns, mapping, factories, isJson));
   }

   private static Set<ColumnMetadata> filterOrderingColumns(Set<ColumnMetadata> orderingColumns, List<ColumnMetadata> selectedColumns, SelectorFactories factories, boolean isJson) {
      if(isJson) {
         return orderingColumns;
      } else {
         Set<ColumnMetadata> filteredOrderingColumns = new LinkedHashSet(orderingColumns.size());
         Iterator var5 = orderingColumns.iterator();

         while(true) {
            ColumnMetadata orderingColumn;
            int index;
            do {
               if(!var5.hasNext()) {
                  return filteredOrderingColumns;
               }

               orderingColumn = (ColumnMetadata)var5.next();
               index = selectedColumns.indexOf(orderingColumn);
            } while(index >= 0 && factories.indexOfSimpleSelectorFactory(index) >= 0);

            filteredOrderingColumns.add(orderingColumn);
         }
      }
   }

   public int getResultSetIndex(ColumnMetadata c) {
      return this.getColumnIndex(c);
   }

   protected final int getColumnIndex(ColumnMetadata c) {
      return this.columns.indexOf(c);
   }

   private static SelectionColumnMapping collectColumnMappings(TableMetadata table, SelectorFactories factories) {
      SelectionColumnMapping selectionColumns = SelectionColumnMapping.newMapping();
      Iterator var3 = factories.iterator();

      while(var3.hasNext()) {
         Selector.Factory factory = (Selector.Factory)var3.next();
         ColumnSpecification colSpec = factory.getColumnSpecification(table);
         factory.addColumnMapping(selectionColumns, colSpec);
      }

      return selectionColumns;
   }

   public abstract Selection.Selectors newSelectors(QueryOptions var1);

   public List<ColumnMetadata> getColumns() {
      return this.columns;
   }

   public SelectionColumns getColumnMapping() {
      return this.columnMapping;
   }

   public abstract boolean isAggregate();

   public String toString() {
      return MoreObjects.toStringHelper(this).add("columns", this.columns).add("columnMapping", this.columnMapping).add("metadata", this.metadata).toString();
   }

   public static List<ByteBuffer> rowToJson(List<ByteBuffer> row, ProtocolVersion protocolVersion, ResultSet.ResultMetadata metadata, List<ColumnMetadata> orderingColumns) {
      ByteBuffer[] jsonRow = new ByteBuffer[orderingColumns.size() + 1];
      StringBuilder sb = new StringBuilder("{");

      for(int i = 0; i < metadata.names.size(); ++i) {
         ColumnSpecification spec = (ColumnSpecification)metadata.names.get(i);
         ByteBuffer buffer = (ByteBuffer)row.get(i);
         int index = orderingColumns.indexOf(spec);
         if(index >= 0) {
            jsonRow[index + 1] = buffer;
         }

         if(i < metadata.getColumnCount()) {
            if(i > 0) {
               sb.append(", ");
            }

            String columnName = spec.name.toString();
            if(!columnName.equals(columnName.toLowerCase(Locale.US))) {
               columnName = "\"" + columnName + "\"";
            }

            sb.append('"');
            sb.append(Json.quoteAsJsonString(columnName));
            sb.append("\": ");
            if(buffer == null) {
               sb.append("null");
            } else if(!buffer.hasRemaining()) {
               sb.append("\"\"");
            } else {
               sb.append(spec.type.toJSONString(buffer, protocolVersion));
            }
         }
      }

      sb.append("}");
      jsonRow[0] = UTF8Type.instance.getSerializer().serialize(sb.toString());
      return Arrays.asList(jsonRow);
   }

   private static class SelectionWithProcessing extends Selection {
      private final SelectorFactories factories;
      private final boolean collectTimestamps;
      private final boolean collectTTLs;

      public SelectionWithProcessing(TableMetadata table, List<ColumnMetadata> columns, Set<ColumnMetadata> orderingColumns, Set<ColumnMetadata> nonPKRestrictedColumns, SelectionColumnMapping metadata, SelectorFactories factories, boolean isJson) {
         super(table, columns, orderingColumns, metadata, ColumnFilterFactory.fromSelectorFactories(table, factories, orderingColumns, nonPKRestrictedColumns), isJson);
         this.factories = factories;
         this.collectTimestamps = factories.containsWritetimeSelectorFactory();
         this.collectTTLs = factories.containsTTLSelectorFactory();
         Iterator var8 = orderingColumns.iterator();

         while(var8.hasNext()) {
            ColumnMetadata orderingColumn = (ColumnMetadata)var8.next();
            factories.addSelectorForOrdering(orderingColumn, this.getColumnIndex(orderingColumn));
         }

      }

      public void addFunctionsTo(List<Function> functions) {
         this.factories.addFunctionsTo(functions);
      }

      public int getResultSetIndex(ColumnMetadata c) {
         return this.factories.indexOfSimpleSelectorFactory(super.getResultSetIndex(c));
      }

      public boolean isAggregate() {
         return this.factories.doesAggregation();
      }

      public Selection.Selectors newSelectors(final QueryOptions options) throws InvalidRequestException {
         return new Selection.Selectors() {
            private final List<Selector> selectors;

            {
               this.selectors = SelectionWithProcessing.this.factories.newInstances(options);
            }

            public Selector.InputRow newInputRow() {
               return new Selector.InputRow(SelectionWithProcessing.this.getColumns().size(), SelectionWithProcessing.this.collectTimestamps, SelectionWithProcessing.this.collectTTLs);
            }

            public void reset() {
               Iterator var1 = this.selectors.iterator();

               while(var1.hasNext()) {
                  Selector selector = (Selector)var1.next();
                  selector.reset();
               }

            }

            public boolean hasProcessing() {
               return true;
            }

            public List<ByteBuffer> getOutputRow() {
               List<ByteBuffer> outputRow = new ArrayList(this.selectors.size());
               Iterator var2 = this.selectors.iterator();

               while(var2.hasNext()) {
                  Selector selector = (Selector)var2.next();
                  outputRow.add(selector.getOutput(options.getProtocolVersion()));
               }

               return (List)(SelectionWithProcessing.this.isJson?Selection.rowToJson(outputRow, options.getProtocolVersion(), SelectionWithProcessing.this.metadata, SelectionWithProcessing.this.orderingColumns):outputRow);
            }

            public void addInputRow(Selector.InputRow input) {
               Iterator var2 = this.selectors.iterator();

               while(var2.hasNext()) {
                  Selector selector = (Selector)var2.next();
                  selector.addInput(options.getProtocolVersion(), input);
               }

            }

            public ColumnFilter getColumnFilter() {
               return SelectionWithProcessing.this.columnFilterFactory.newInstance(this.selectors);
            }
         };
      }
   }

   private static class SimpleSelection extends Selection {
      private final boolean isWildcard;

      public SimpleSelection(TableMetadata table, List<ColumnMetadata> selectedColumns, Set<ColumnMetadata> orderingColumns, boolean isWildcard, boolean isJson) {
         this(table, selectedColumns, orderingColumns, SelectionColumnMapping.simpleMapping(selectedColumns), isWildcard?ColumnFilterFactory.wildcard(table):ColumnFilterFactory.fromColumns(table, selectedColumns, orderingColumns, Collections.emptySet()), isWildcard, isJson);
      }

      public SimpleSelection(TableMetadata table, List<ColumnMetadata> selectedColumns, Set<ColumnMetadata> orderingColumns, Set<ColumnMetadata> nonPKRestrictedColumns, SelectionColumnMapping mapping, boolean isJson) {
         this(table, selectedColumns, orderingColumns, mapping, ColumnFilterFactory.fromColumns(table, selectedColumns, orderingColumns, nonPKRestrictedColumns), false, isJson);
      }

      private SimpleSelection(TableMetadata table, List<ColumnMetadata> selectedColumns, Set<ColumnMetadata> orderingColumns, SelectionColumnMapping mapping, ColumnFilterFactory columnFilterFactory, boolean isWildcard, boolean isJson) {
         super(table, selectedColumns, orderingColumns, mapping, columnFilterFactory, isJson);
         this.isWildcard = isWildcard;
      }

      public boolean isWildcard() {
         return this.isWildcard;
      }

      public boolean isAggregate() {
         return false;
      }

      public Selection.Selectors newSelectors(final QueryOptions options) {
         return new Selection.Selectors() {
            private List<ByteBuffer> current;

            public Selector.InputRow newInputRow() {
               return new Selector.InputRow(SimpleSelection.this.getColumns().size(), false, false);
            }

            public void reset() {
               this.current = null;
            }

            public List<ByteBuffer> getOutputRow() {
               return SimpleSelection.this.isJson?Selection.rowToJson(this.current, options.getProtocolVersion(), SimpleSelection.this.metadata, SimpleSelection.this.orderingColumns):this.current;
            }

            public void addInputRow(Selector.InputRow input) {
               this.current = input.getValues();
            }

            public boolean hasProcessing() {
               return false;
            }

            public ColumnFilter getColumnFilter() {
               return SimpleSelection.this.columnFilterFactory.newInstance((List)null);
            }
         };
      }
   }

   public interface Selectors {
      Selector.InputRow newInputRow();

      boolean hasProcessing();

      ColumnFilter getColumnFilter();

      void addInputRow(Selector.InputRow var1);

      List<ByteBuffer> getOutputRow();

      void reset();
   }
}
