package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;

public abstract class MultiColumnRestriction implements SingleRestriction {
   protected final List<ColumnMetadata> columnDefs;

   public MultiColumnRestriction(List<ColumnMetadata> columnDefs) {
      this.columnDefs = columnDefs;
   }

   public boolean isMultiColumn() {
      return true;
   }

   public ColumnMetadata getFirstColumn() {
      return (ColumnMetadata)this.columnDefs.get(0);
   }

   public ColumnMetadata getLastColumn() {
      return (ColumnMetadata)this.columnDefs.get(this.columnDefs.size() - 1);
   }

   public List<ColumnMetadata> getColumnDefs() {
      return this.columnDefs;
   }

   public final SingleRestriction mergeWith(SingleRestriction otherRestriction) {
      return !otherRestriction.isMultiColumn() && ((SingleColumnRestriction)otherRestriction).canBeConvertedToMultiColumnRestriction()?this.doMergeWith(((SingleColumnRestriction)otherRestriction).toMultiColumnRestriction()):this.doMergeWith(otherRestriction);
   }

   protected abstract SingleRestriction doMergeWith(SingleRestriction var1);

   protected final String getColumnsInCommons(Restriction otherRestriction) {
      Set<ColumnMetadata> commons = new HashSet(this.getColumnDefs());
      commons.retainAll(otherRestriction.getColumnDefs());
      StringBuilder builder = new StringBuilder();

      ColumnMetadata columnMetadata;
      for(Iterator var4 = commons.iterator(); var4.hasNext(); builder.append(columnMetadata.name)) {
         columnMetadata = (ColumnMetadata)var4.next();
         if(builder.length() != 0) {
            builder.append(" ,");
         }
      }

      return builder.toString();
   }

   public final boolean hasSupportingIndex(SecondaryIndexManager indexManager) {
      Iterator var2 = indexManager.listIndexes().iterator();

      Index index;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         index = (Index)var2.next();
      } while(!this.isSupportedBy(index));

      return true;
   }

   protected abstract boolean isSupportedBy(Index var1);

   public static class NotNullRestriction extends MultiColumnRestriction {
      public NotNullRestriction(List<ColumnMetadata> columnDefs) {
         super(columnDefs);

         assert columnDefs.size() == 1;

      }

      public void addFunctionsTo(List<Function> functions) {
      }

      public boolean isNotNull() {
         return true;
      }

      public String toString() {
         return "IS NOT NULL";
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by a relation if it includes an IS NOT NULL clause", new Object[]{this.getColumnsInCommons(otherRestriction)});
      }

      protected boolean isSupportedBy(Index index) {
         Iterator var2 = this.columnDefs.iterator();

         ColumnMetadata column;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            column = (ColumnMetadata)var2.next();
         } while(!index.supportsExpression(column, Operator.IS_NOT));

         return true;
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
      }

      public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexMananger, QueryOptions options) {
         throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
      }
   }

   public static class SliceRestriction extends MultiColumnRestriction {
      private final TermSlice slice;

      public SliceRestriction(List<ColumnMetadata> columnDefs, Bound bound, boolean inclusive, Term term) {
         this(columnDefs, TermSlice.newInstance(bound, inclusive, term));
      }

      SliceRestriction(List<ColumnMetadata> columnDefs, TermSlice slice) {
         super(columnDefs);
         this.slice = slice;
      }

      public boolean isSlice() {
         return true;
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException();
      }

      public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options) {
         boolean reversed = this.getFirstColumn().isReversedType();
         EnumMap<Bound, List<ByteBuffer>> componentBounds = new EnumMap(Bound.class);
         componentBounds.put(Bound.START, this.componentBounds(Bound.START, options));
         componentBounds.put(Bound.END, this.componentBounds(Bound.END, options));
         List<List<ByteBuffer>> toAdd = new ArrayList();
         List<ByteBuffer> values = new ArrayList();
         int i = 0;

         for(int m = this.columnDefs.size(); i < m; ++i) {
            ColumnMetadata column = (ColumnMetadata)this.columnDefs.get(i);
            Bound b = bound.reverseIfNeeded(column);
            if(reversed != column.isReversedType()) {
               reversed = column.isReversedType();
               toAdd.add(values);
               if(!this.hasComponent(b, i, componentBounds)) {
                  continue;
               }

               if(this.hasComponent(b.reverse(), i, componentBounds)) {
                  toAdd.add(values);
               }

               values = new ArrayList();
               List<ByteBuffer> vals = (List)componentBounds.get(b);
               int n = Math.min(i, vals.size());

               for(int j = 0; j < n; ++j) {
                  ByteBuffer v = (ByteBuffer)RequestValidations.checkNotNull(vals.get(j), "Invalid null value in condition for column %s", ((ColumnMetadata)this.columnDefs.get(j)).name);
                  values.add(v);
               }
            }

            if(this.hasComponent(b, i, componentBounds)) {
               ByteBuffer v = (ByteBuffer)RequestValidations.checkNotNull(((List)componentBounds.get(b)).get(i), "Invalid null value in condition for column %s", ((ColumnMetadata)this.columnDefs.get(i)).name);
               values.add(v);
            }
         }

         toAdd.add(values);
         if(bound.isEnd()) {
            Collections.reverse(toAdd);
         }

         return builder.addAllElementsToAll(toAdd);
      }

      protected boolean isSupportedBy(Index index) {
         Iterator var2 = this.columnDefs.iterator();

         ColumnMetadata def;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            def = (ColumnMetadata)var2.next();
         } while(!this.slice.isSupportedBy(def, index));

         return true;
      }

      public boolean hasBound(Bound bound) {
         return this.slice.hasBound(bound);
      }

      public void addFunctionsTo(List<Function> functions) {
         this.slice.addFunctionsTo(functions);
      }

      public boolean isInclusive(Bound bound) {
         return this.slice.isInclusive(bound);
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         RequestValidations.checkTrue(otherRestriction.isSlice(), "Column \"%s\" cannot be restricted by both an equality and an inequality relation", this.getColumnsInCommons(otherRestriction));
         if(!this.getFirstColumn().equals(otherRestriction.getFirstColumn())) {
            ColumnMetadata column = this.getFirstColumn().position() > otherRestriction.getFirstColumn().position()?this.getFirstColumn():otherRestriction.getFirstColumn();
            throw RequestValidations.invalidRequest("Column \"%s\" cannot be restricted by two inequalities not starting with the same column", new Object[]{column.name});
         } else {
            RequestValidations.checkFalse(this.hasBound(Bound.START) && otherRestriction.hasBound(Bound.START), "More than one restriction was found for the start bound on %s", this.getColumnsInCommons(otherRestriction));
            RequestValidations.checkFalse(this.hasBound(Bound.END) && otherRestriction.hasBound(Bound.END), "More than one restriction was found for the end bound on %s", this.getColumnsInCommons(otherRestriction));
            MultiColumnRestriction.SliceRestriction otherSlice = (MultiColumnRestriction.SliceRestriction)otherRestriction;
            List<ColumnMetadata> newColumnDefs = this.columnDefs.size() >= otherSlice.columnDefs.size()?this.columnDefs:otherSlice.columnDefs;
            return new MultiColumnRestriction.SliceRestriction(newColumnDefs, this.slice.merge(otherSlice.slice));
         }
      }

      public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         throw RequestValidations.invalidRequest("Multi-column slice restrictions cannot be used for filtering.");
      }

      public String toString() {
         return "SLICE" + this.slice;
      }

      private List<ByteBuffer> componentBounds(Bound b, QueryOptions options) {
         if(!this.slice.hasBound(b)) {
            return Collections.emptyList();
         } else {
            Term.Terminal terminal = this.slice.bound(b).bind(options);
            return terminal instanceof Tuples.Value?((Tuples.Value)terminal).getElements():Collections.singletonList(terminal.get(options.getProtocolVersion()));
         }
      }

      private boolean hasComponent(Bound b, int index, EnumMap<Bound, List<ByteBuffer>> componentBounds) {
         return ((List)componentBounds.get(b)).size() > index;
      }
   }

   public static class InRestrictionWithMarker extends MultiColumnRestriction.INRestriction {
      protected final AbstractMarker marker;

      public InRestrictionWithMarker(List<ColumnMetadata> columnDefs, AbstractMarker marker) {
         super(columnDefs);
         this.marker = marker;
      }

      public void addFunctionsTo(List<Function> functions) {
      }

      public String toString() {
         return "IN ?";
      }

      protected List<List<ByteBuffer>> splitValues(QueryOptions options) {
         Tuples.InMarker inMarker = (Tuples.InMarker)this.marker;
         Tuples.InValue inValue = inMarker.bind(options);
         RequestValidations.checkNotNull(inValue, "Invalid null value for IN restriction");
         return inValue.getSplitValues();
      }
   }

   public static class InRestrictionWithValues extends MultiColumnRestriction.INRestriction {
      protected final List<Term> values;

      public InRestrictionWithValues(List<ColumnMetadata> columnDefs, List<Term> values) {
         super(columnDefs);
         this.values = values;
      }

      public void addFunctionsTo(List<Function> functions) {
         Terms.addFunctions(this.values, functions);
      }

      public String toString() {
         return String.format("IN(%s)", new Object[]{this.values});
      }

      protected List<List<ByteBuffer>> splitValues(QueryOptions options) {
         List<List<ByteBuffer>> buffers = new ArrayList(this.values.size());
         Iterator var3 = this.values.iterator();

         while(var3.hasNext()) {
            Term value = (Term)var3.next();
            Term.MultiItemTerminal term = (Term.MultiItemTerminal)value.bind(options);
            buffers.add(term.getElements());
         }

         return buffers;
      }
   }

   public abstract static class INRestriction extends MultiColumnRestriction {
      public INRestriction(List<ColumnMetadata> columnDefs) {
         super(columnDefs);
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         List<List<ByteBuffer>> splitInValues = this.splitValues(options);
         builder.addAllElementsToAll(splitInValues);
         if(builder.containsNull()) {
            throw RequestValidations.invalidRequest("Invalid null value in condition for columns: %s", new Object[]{ColumnMetadata.toIdentifiers(this.columnDefs)});
         } else {
            return builder;
         }
      }

      public boolean isIN() {
         return true;
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes a IN", new Object[]{this.getColumnsInCommons(otherRestriction)});
      }

      protected boolean isSupportedBy(Index index) {
         Iterator var2 = this.columnDefs.iterator();

         ColumnMetadata column;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            column = (ColumnMetadata)var2.next();
         } while(!index.supportsExpression(column, Operator.IN));

         return true;
      }

      public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         throw RequestValidations.invalidRequest("IN restrictions are not supported on indexed columns");
      }

      protected abstract List<List<ByteBuffer>> splitValues(QueryOptions var1);
   }

   public static class EQRestriction extends MultiColumnRestriction {
      protected final Term value;

      public EQRestriction(List<ColumnMetadata> columnDefs, Term value) {
         super(columnDefs);
         this.value = value;
      }

      public boolean isEQ() {
         return true;
      }

      public void addFunctionsTo(List<Function> functions) {
         this.value.addFunctionsTo(functions);
      }

      public String toString() {
         return String.format("EQ(%s)", new Object[]{this.value});
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", new Object[]{this.getColumnsInCommons(otherRestriction)});
      }

      protected boolean isSupportedBy(Index index) {
         Iterator var2 = this.columnDefs.iterator();

         ColumnMetadata column;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            column = (ColumnMetadata)var2.next();
         } while(!index.supportsExpression(column, Operator.EQ));

         return true;
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         Tuples.Value t = (Tuples.Value)this.value.bind(options);
         List<ByteBuffer> values = t.getElements();
         int i = 0;

         for(int m = values.size(); i < m; ++i) {
            builder.addElementToAll((ByteBuffer)values.get(i));
            RequestValidations.checkFalse(builder.containsNull(), "Invalid null value for column %s", ((ColumnMetadata)this.columnDefs.get(i)).name);
         }

         return builder;
      }

      public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexMananger, QueryOptions options) {
         Tuples.Value t = (Tuples.Value)this.value.bind(options);
         List<ByteBuffer> values = t.getElements();
         int i = 0;

         for(int m = this.columnDefs.size(); i < m; ++i) {
            ColumnMetadata columnDef = (ColumnMetadata)this.columnDefs.get(i);
            filter.add(columnDef, Operator.EQ, (ByteBuffer)values.get(i));
         }

      }
   }
}
