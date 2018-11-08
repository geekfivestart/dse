package org.apache.cassandra.cql3.restrictions;

import com.google.common.base.Joiner;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class TokenRestriction implements PartitionKeyRestrictions {
   protected final List<ColumnMetadata> columnDefs;
   protected final TableMetadata metadata;

   public TokenRestriction(TableMetadata metadata, List<ColumnMetadata> columnDefs) {
      this.columnDefs = columnDefs;
      this.metadata = metadata;
   }

   public boolean hasIN() {
      return false;
   }

   public boolean hasOnlyEqualityRestrictions() {
      return false;
   }

   public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
      return Collections.singleton(this);
   }

   public final boolean isOnToken() {
      return true;
   }

   public boolean needFiltering(TableMetadata table) {
      return false;
   }

   public boolean hasSlice() {
      return false;
   }

   public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table) {
      return false;
   }

   public List<ColumnMetadata> getColumnDefs() {
      return this.columnDefs;
   }

   public ColumnMetadata getFirstColumn() {
      return (ColumnMetadata)this.columnDefs.get(0);
   }

   public ColumnMetadata getLastColumn() {
      return (ColumnMetadata)this.columnDefs.get(this.columnDefs.size() - 1);
   }

   public boolean hasSupportingIndex(SecondaryIndexManager secondaryIndexManager) {
      return false;
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
      throw new UnsupportedOperationException("Index expression cannot be created for token restriction");
   }

   public final boolean isEmpty() {
      return this.getColumnDefs().isEmpty();
   }

   public final int size() {
      return this.getColumnDefs().size();
   }

   protected final String getColumnNamesAsString() {
      return Joiner.on(", ").join(ColumnMetadata.toIdentifiers(this.columnDefs));
   }

   public final PartitionKeyRestrictions mergeWith(Restriction otherRestriction) throws InvalidRequestException {
      return (PartitionKeyRestrictions)(!otherRestriction.isOnToken()?new TokenFilter(this.toPartitionKeyRestrictions(otherRestriction), this):this.doMergeWith((TokenRestriction)otherRestriction));
   }

   protected abstract PartitionKeyRestrictions doMergeWith(TokenRestriction var1) throws InvalidRequestException;

   private PartitionKeyRestrictions toPartitionKeyRestrictions(Restriction restriction) throws InvalidRequestException {
      return restriction instanceof PartitionKeyRestrictions?(PartitionKeyRestrictions)restriction:(new PartitionKeySingleRestrictionSet(this.metadata.partitionKeyAsClusteringComparator())).mergeWith(restriction);
   }

   public static class SliceRestriction extends TokenRestriction {
      private final TermSlice slice;

      public SliceRestriction(TableMetadata table, List<ColumnMetadata> columnDefs, Bound bound, boolean inclusive, Term term) {
         super(table, columnDefs);
         this.slice = TermSlice.newInstance(bound, inclusive, term);
      }

      public boolean hasContains() {
         return false;
      }

      public boolean hasSlice() {
         return true;
      }

      public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException {
         throw new UnsupportedOperationException();
      }

      public boolean hasBound(Bound b) {
         return this.slice.hasBound(b);
      }

      public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException {
         return Collections.singletonList(this.slice.bound(b).bindAndGet(options));
      }

      public void addFunctionsTo(List<Function> functions) {
         this.slice.addFunctionsTo(functions);
      }

      public boolean isInclusive(Bound b) {
         return this.slice.isInclusive(b);
      }

      protected PartitionKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException {
         if(!(otherRestriction instanceof TokenRestriction.SliceRestriction)) {
            throw RequestValidations.invalidRequest("Columns \"%s\" cannot be restricted by both an equality and an inequality relation", new Object[]{this.getColumnNamesAsString()});
         } else {
            TokenRestriction.SliceRestriction otherSlice = (TokenRestriction.SliceRestriction)otherRestriction;
            if(this.hasBound(Bound.START) && otherSlice.hasBound(Bound.START)) {
               throw RequestValidations.invalidRequest("More than one restriction was found for the start bound on %s", new Object[]{this.getColumnNamesAsString()});
            } else if(this.hasBound(Bound.END) && otherSlice.hasBound(Bound.END)) {
               throw RequestValidations.invalidRequest("More than one restriction was found for the end bound on %s", new Object[]{this.getColumnNamesAsString()});
            } else {
               return new TokenRestriction.SliceRestriction(this.metadata, this.columnDefs, this.slice.merge(otherSlice.slice));
            }
         }
      }

      public String toString() {
         return String.format("SLICE%s", new Object[]{this.slice});
      }

      private SliceRestriction(TableMetadata table, List<ColumnMetadata> columnDefs, TermSlice slice) {
         super(table, columnDefs);
         this.slice = slice;
      }
   }

   public static final class EQRestriction extends TokenRestriction {
      private final Term value;

      public EQRestriction(TableMetadata table, List<ColumnMetadata> columnDefs, Term value) {
         super(table, columnDefs);
         this.value = value;
      }

      public void addFunctionsTo(List<Function> functions) {
         this.value.addFunctionsTo(functions);
      }

      protected PartitionKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException {
         throw RequestValidations.invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", new Object[]{Joiner.on(", ").join(ColumnMetadata.toIdentifiers(this.columnDefs))});
      }

      public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException {
         return this.values(options);
      }

      public boolean hasBound(Bound b) {
         return true;
      }

      public boolean isInclusive(Bound b) {
         return true;
      }

      public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException {
         return Collections.singletonList(this.value.bindAndGet(options));
      }

      public boolean hasContains() {
         return false;
      }
   }
}
