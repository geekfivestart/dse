package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public abstract class SingleColumnRestriction implements SingleRestriction {
   protected final ColumnMetadata columnDef;

   public SingleColumnRestriction(ColumnMetadata columnDef) {
      this.columnDef = columnDef;
   }

   public List<ColumnMetadata> getColumnDefs() {
      return Collections.singletonList(this.columnDef);
   }

   public ColumnMetadata getFirstColumn() {
      return this.columnDef;
   }

   public ColumnMetadata getLastColumn() {
      return this.columnDef;
   }

   public boolean hasSupportingIndex(SecondaryIndexManager indexManager) {
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

   public final SingleRestriction mergeWith(SingleRestriction otherRestriction) {
      return otherRestriction.isMultiColumn() && this.canBeConvertedToMultiColumnRestriction()?this.toMultiColumnRestriction().mergeWith(otherRestriction):this.doMergeWith(otherRestriction);
   }

   protected abstract SingleRestriction doMergeWith(SingleRestriction var1);

   abstract MultiColumnRestriction toMultiColumnRestriction();

   boolean canBeConvertedToMultiColumnRestriction() {
      return true;
   }

   protected abstract boolean isSupportedBy(Index var1);

   public static class LikeRestriction extends SingleColumnRestriction {
      private static final ByteBuffer LIKE_WILDCARD = ByteBufferUtil.bytes("%");
      protected final Operator operator;
      protected final Term value;

      public LikeRestriction(ColumnMetadata columnDef, Operator operator, Term value) {
         super(columnDef);
         this.operator = operator;
         this.value = value;
      }

      public void addFunctionsTo(List<Function> functions) {
         this.value.addFunctionsTo(functions);
      }

      public boolean isEQ() {
         return false;
      }

      public boolean isLIKE() {
         return true;
      }

      public boolean canBeConvertedToMultiColumnRestriction() {
         return false;
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         throw new UnsupportedOperationException();
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         Pair<Operator, ByteBuffer> operation = makeSpecific(this.value.bindAndGet(options));
         RowFilter.SimpleExpression expression = filter.add(this.columnDef, (Operator)operation.left, (ByteBuffer)operation.right);
         indexManager.getBestIndexFor((RowFilter.Expression)expression).orElseThrow(() -> {
            return RequestValidations.invalidRequest("%s is only supported on properly indexed columns", new Object[]{expression});
         });
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException();
      }

      public String toString() {
         return this.operator.toString();
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes a %s", new Object[]{this.columnDef.name, this.operator});
      }

      protected boolean isSupportedBy(Index index) {
         return index.supportsExpression(this.columnDef, this.operator);
      }

      protected static Pair<Operator, ByteBuffer> makeSpecific(ByteBuffer value) {
         int beginIndex = value.position();
         int endIndex = value.limit() - 1;
         Operator operator;
         if(ByteBufferUtil.endsWith(value, LIKE_WILDCARD)) {
            if(ByteBufferUtil.startsWith(value, LIKE_WILDCARD)) {
               operator = Operator.LIKE_CONTAINS;
               beginIndex = 1;
            } else {
               operator = Operator.LIKE_PREFIX;
            }
         } else if(ByteBufferUtil.startsWith(value, LIKE_WILDCARD)) {
            operator = Operator.LIKE_SUFFIX;
            ++beginIndex;
            ++endIndex;
         } else {
            operator = Operator.LIKE_MATCHES;
            ++endIndex;
         }

         if(endIndex != 0 && beginIndex != endIndex) {
            ByteBuffer newValue = value.duplicate();
            newValue.position(beginIndex);
            newValue.limit(endIndex);
            return Pair.create(operator, newValue);
         } else {
            throw RequestValidations.invalidRequest("LIKE value can't be empty.");
         }
      }
   }

   public static class IsNotNullRestriction extends SingleColumnRestriction {
      public IsNotNullRestriction(ColumnMetadata columnDef) {
         super(columnDef);
      }

      public void addFunctionsTo(List<Function> functions) {
      }

      public boolean isNotNull() {
         return true;
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         return new MultiColumnRestriction.NotNullRestriction(Collections.singletonList(this.columnDef));
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
      }

      public String toString() {
         return "IS NOT NULL";
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by a relation if it includes an IS NOT NULL", new Object[]{this.columnDef.name});
      }

      protected boolean isSupportedBy(Index index) {
         return index.supportsExpression(this.columnDef, Operator.IS_NOT);
      }
   }

   public static class ContainsRestriction extends SingleColumnRestriction {
      private List<Term> values = new ArrayList();
      private List<Term> keys = new ArrayList();
      private List<Term> entryKeys = new ArrayList();
      private List<Term> entryValues = new ArrayList();

      public ContainsRestriction(ColumnMetadata columnDef, Term t, boolean isKey) {
         super(columnDef);
         if(isKey) {
            this.keys.add(t);
         } else {
            this.values.add(t);
         }

      }

      public ContainsRestriction(ColumnMetadata columnDef, Term mapKey, Term mapValue) {
         super(columnDef);
         this.entryKeys.add(mapKey);
         this.entryValues.add(mapValue);
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         throw new UnsupportedOperationException();
      }

      boolean canBeConvertedToMultiColumnRestriction() {
         return false;
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException();
      }

      public boolean isContains() {
         return true;
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         RequestValidations.checkTrue(otherRestriction.isContains(), "Collection column %s can only be restricted by CONTAINS, CONTAINS KEY, or map-entry equality", this.columnDef.name);
         SingleColumnRestriction.ContainsRestriction newContains = new SingleColumnRestriction.ContainsRestriction(this.columnDef);
         copyKeysAndValues(this, newContains);
         copyKeysAndValues((SingleColumnRestriction.ContainsRestriction)otherRestriction, newContains);
         return newContains;
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         Iterator var4 = bindAndGet(this.values, options).iterator();

         ByteBuffer key;
         while(var4.hasNext()) {
            key = (ByteBuffer)var4.next();
            filter.add(this.columnDef, Operator.CONTAINS, key);
         }

         var4 = bindAndGet(this.keys, options).iterator();

         while(var4.hasNext()) {
            key = (ByteBuffer)var4.next();
            filter.add(this.columnDef, Operator.CONTAINS_KEY, key);
         }

         List<ByteBuffer> eks = bindAndGet(this.entryKeys, options);
         List<ByteBuffer> evs = bindAndGet(this.entryValues, options);

         assert eks.size() == evs.size();

         for(int i = 0; i < eks.size(); ++i) {
            filter.addMapEquality(this.columnDef, (ByteBuffer)eks.get(i), Operator.EQ, (ByteBuffer)evs.get(i));
         }

      }

      protected boolean isSupportedBy(Index index) {
         boolean supported = false;
         if(this.numberOfValues() > 0) {
            supported |= index.supportsExpression(this.columnDef, Operator.CONTAINS);
         }

         if(this.numberOfKeys() > 0) {
            supported |= index.supportsExpression(this.columnDef, Operator.CONTAINS_KEY);
         }

         if(this.numberOfEntries() > 0) {
            supported |= index.supportsExpression(this.columnDef, Operator.EQ);
         }

         return supported;
      }

      public int numberOfValues() {
         return this.values.size();
      }

      public int numberOfKeys() {
         return this.keys.size();
      }

      public int numberOfEntries() {
         return this.entryKeys.size();
      }

      public void addFunctionsTo(List<Function> functions) {
         Terms.addFunctions(this.values, functions);
         Terms.addFunctions(this.keys, functions);
         Terms.addFunctions(this.entryKeys, functions);
         Terms.addFunctions(this.entryValues, functions);
      }

      public String toString() {
         return String.format("CONTAINS(values=%s, keys=%s, entryKeys=%s, entryValues=%s)", new Object[]{this.values, this.keys, this.entryKeys, this.entryValues});
      }

      public boolean hasBound(Bound b) {
         throw new UnsupportedOperationException();
      }

      public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options) {
         throw new UnsupportedOperationException();
      }

      public boolean isInclusive(Bound b) {
         throw new UnsupportedOperationException();
      }

      private static List<ByteBuffer> bindAndGet(List<Term> terms, QueryOptions options) {
         List<ByteBuffer> buffers = new ArrayList(terms.size());
         Iterator var3 = terms.iterator();

         while(var3.hasNext()) {
            Term value = (Term)var3.next();
            buffers.add(value.bindAndGet(options));
         }

         return buffers;
      }

      protected static void copyKeysAndValues(SingleColumnRestriction.ContainsRestriction from, SingleColumnRestriction.ContainsRestriction to) {
         to.values.addAll(from.values);
         to.keys.addAll(from.keys);
         to.entryKeys.addAll(from.entryKeys);
         to.entryValues.addAll(from.entryValues);
      }

      protected ContainsRestriction(ColumnMetadata columnDef) {
         super(columnDef);
      }
   }

   public static class SliceRestriction extends SingleColumnRestriction {
      protected final TermSlice slice;

      public SliceRestriction(ColumnMetadata columnDef, Bound bound, boolean inclusive, Term term) {
         super(columnDef);
         this.slice = TermSlice.newInstance(bound, inclusive, term);
      }

      public void addFunctionsTo(List<Function> functions) {
         this.slice.addFunctionsTo(functions);
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         return new MultiColumnRestriction.SliceRestriction(Collections.singletonList(this.columnDef), this.slice);
      }

      public boolean isSlice() {
         return true;
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         throw new UnsupportedOperationException();
      }

      public boolean hasBound(Bound b) {
         return this.slice.hasBound(b);
      }

      public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options) {
         Bound b = bound.reverseIfNeeded(this.getFirstColumn());
         if(!this.hasBound(b)) {
            return builder;
         } else {
            ByteBuffer value = this.slice.bound(b).bindAndGet(options);
            RequestValidations.checkBindValueSet(value, "Invalid unset value for column %s", this.columnDef.name);
            return builder.addElementToAll(value);
         }
      }

      public boolean isInclusive(Bound b) {
         return this.slice.isInclusive(b);
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         RequestValidations.checkTrue(otherRestriction.isSlice(), "Column \"%s\" cannot be restricted by both an equality and an inequality relation", this.columnDef.name);
         SingleColumnRestriction.SliceRestriction otherSlice = (SingleColumnRestriction.SliceRestriction)otherRestriction;
         RequestValidations.checkFalse(this.hasBound(Bound.START) && otherSlice.hasBound(Bound.START), "More than one restriction was found for the start bound on %s", this.columnDef.name);
         RequestValidations.checkFalse(this.hasBound(Bound.END) && otherSlice.hasBound(Bound.END), "More than one restriction was found for the end bound on %s", this.columnDef.name);
         return new SingleColumnRestriction.SliceRestriction(this.columnDef, this.slice.merge(otherSlice.slice));
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         Bound[] var4 = Bound.values();
         int var5 = var4.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            Bound b = var4[var6];
            if(this.hasBound(b)) {
               filter.add(this.columnDef, this.slice.getIndexOperator(b), this.slice.bound(b).bindAndGet(options));
            }
         }

      }

      protected boolean isSupportedBy(Index index) {
         return this.slice.isSupportedBy(this.columnDef, index);
      }

      public String toString() {
         return String.format("SLICE%s", new Object[]{this.slice});
      }

      protected SliceRestriction(ColumnMetadata columnDef, TermSlice slice) {
         super(columnDef);
         this.slice = slice;
      }
   }

   public static class InRestrictionWithMarker extends SingleColumnRestriction.INRestriction {
      protected final AbstractMarker marker;

      public InRestrictionWithMarker(ColumnMetadata columnDef, AbstractMarker marker) {
         super(columnDef);
         this.marker = marker;
      }

      public void addFunctionsTo(List<Function> functions) {
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         return new MultiColumnRestriction.InRestrictionWithMarker(Collections.singletonList(this.columnDef), this.marker);
      }

      protected List<ByteBuffer> getValues(QueryOptions options) {
         Term.Terminal term = this.marker.bind(options);
         RequestValidations.checkNotNull(term, "Invalid null value for column %s", this.columnDef.name);
         RequestValidations.checkFalse(term == Constants.UNSET_VALUE, "Invalid unset value for column %s", this.columnDef.name);
         Term.MultiItemTerminal lval = (Term.MultiItemTerminal)term;
         return lval.getElements();
      }

      public String toString() {
         return "IN ?";
      }
   }

   public static class InRestrictionWithValues extends SingleColumnRestriction.INRestriction {
      protected final List<Term> values;

      public InRestrictionWithValues(ColumnMetadata columnDef, List<Term> values) {
         super(columnDef);
         this.values = values;
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         return new MultiColumnRestriction.InRestrictionWithValues(Collections.singletonList(this.columnDef), this.values);
      }

      public void addFunctionsTo(List<Function> functions) {
         Terms.addFunctions(this.values, functions);
      }

      protected List<ByteBuffer> getValues(QueryOptions options) {
         List<ByteBuffer> buffers = new ArrayList(this.values.size());
         Iterator var3 = this.values.iterator();

         while(var3.hasNext()) {
            Term value = (Term)var3.next();
            buffers.add(value.bindAndGet(options));
         }

         return buffers;
      }

      public String toString() {
         return String.format("IN(%s)", new Object[]{this.values});
      }
   }

   public abstract static class INRestriction extends SingleColumnRestriction {
      public INRestriction(ColumnMetadata columnDef) {
         super(columnDef);
      }

      public final boolean isIN() {
         return true;
      }

      public final SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes a IN", new Object[]{this.columnDef.name});
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         builder.addEachElementToAll(this.getValues(options));
         RequestValidations.checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", this.columnDef.name);
         RequestValidations.checkFalse(builder.containsUnset(), "Invalid unset value for column %s", this.columnDef.name);
         return builder;
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         throw RequestValidations.invalidRequest("IN restrictions are not supported on indexed columns");
      }

      protected boolean isSupportedBy(Index index) {
         return index.supportsExpression(this.columnDef, Operator.IN);
      }

      protected abstract List<ByteBuffer> getValues(QueryOptions var1);
   }

   public static class EQRestriction extends SingleColumnRestriction {
      protected final Term value;

      public EQRestriction(ColumnMetadata columnDef, Term value) {
         super(columnDef);
         this.value = value;
      }

      public void addFunctionsTo(List<Function> functions) {
         this.value.addFunctionsTo(functions);
      }

      public boolean isEQ() {
         return true;
      }

      MultiColumnRestriction toMultiColumnRestriction() {
         return new MultiColumnRestriction.EQRestriction(Collections.singletonList(this.columnDef), this.value);
      }

      public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
         filter.add(this.columnDef, Operator.EQ, this.value.bindAndGet(options));
      }

      public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
         builder.addElementToAll(this.value.bindAndGet(options));
         RequestValidations.checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", this.columnDef.name);
         RequestValidations.checkFalse(builder.containsUnset(), "Invalid unset value for column %s", this.columnDef.name);
         return builder;
      }

      public String toString() {
         return String.format("EQ(%s)", new Object[]{this.value});
      }

      public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", new Object[]{this.columnDef.name});
      }

      protected boolean isSupportedBy(Index index) {
         return index.supportsExpression(this.columnDef, Operator.EQ);
      }
   }
}
