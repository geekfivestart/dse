package org.apache.cassandra.cql3.restrictions;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.ImmutableRangeSet.Builder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

final class TokenFilter implements PartitionKeyRestrictions {
   private final PartitionKeyRestrictions restrictions;
   private final TokenRestriction tokenRestriction;
   private final IPartitioner partitioner;

   public boolean hasIN() {
      return this.isOnToken()?false:this.restrictions.hasIN();
   }

   public boolean hasContains() {
      return this.isOnToken()?false:this.restrictions.hasContains();
   }

   public boolean hasOnlyEqualityRestrictions() {
      return this.isOnToken()?false:this.restrictions.hasOnlyEqualityRestrictions();
   }

   public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
      Set<Restriction> set = new HashSet();
      set.addAll(this.restrictions.getRestrictions(columnDef));
      set.addAll(this.tokenRestriction.getRestrictions(columnDef));
      return set;
   }

   public boolean isOnToken() {
      return this.needFiltering(this.tokenRestriction.metadata) || this.restrictions.size() < this.tokenRestriction.size();
   }

   public TokenFilter(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction) {
      this.restrictions = restrictions;
      this.tokenRestriction = tokenRestriction;
      this.partitioner = tokenRestriction.metadata.partitioner;
   }

   public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException {
      return this.filter(this.restrictions.values(options), options);
   }

   public PartitionKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException {
      return restriction.isOnToken()?new TokenFilter(this.restrictions, (TokenRestriction)this.tokenRestriction.mergeWith(restriction)):new TokenFilter(this.restrictions.mergeWith(restriction), this.tokenRestriction);
   }

   public boolean isInclusive(Bound bound) {
      return this.isOnToken()?this.tokenRestriction.isInclusive(bound):this.restrictions.isInclusive(bound);
   }

   public boolean hasBound(Bound bound) {
      return this.isOnToken()?this.tokenRestriction.hasBound(bound):this.restrictions.hasBound(bound);
   }

   public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException {
      return this.isOnToken()?this.tokenRestriction.bounds(bound, options):this.restrictions.bounds(bound, options);
   }

   private List<ByteBuffer> filter(List<ByteBuffer> values, QueryOptions options) throws InvalidRequestException {
      RangeSet<Token> rangeSet = this.tokenRestriction.hasSlice()?this.toRangeSet(this.tokenRestriction, options):this.toRangeSet(this.tokenRestriction.values(options));
      return this.filterWithRangeSet(rangeSet, values);
   }

   private List<ByteBuffer> filterWithRangeSet(RangeSet<Token> tokens, List<ByteBuffer> values) {
      List<ByteBuffer> remaining = new ArrayList();
      Iterator var4 = values.iterator();

      while(var4.hasNext()) {
         ByteBuffer value = (ByteBuffer)var4.next();
         Token token = this.partitioner.getToken(value);
         if(tokens.contains(token)) {
            remaining.add(value);
         }
      }

      return remaining;
   }

   private RangeSet<Token> toRangeSet(List<ByteBuffer> buffers) {
      Builder<Token> builder = ImmutableRangeSet.builder();
      Iterator var3 = buffers.iterator();

      while(var3.hasNext()) {
         ByteBuffer buffer = (ByteBuffer)var3.next();
         builder.add(Range.singleton(this.deserializeToken(buffer)));
      }

      return builder.build();
   }

   private RangeSet<Token> toRangeSet(TokenRestriction slice, QueryOptions options) throws InvalidRequestException {
      Token start;
      if(slice.hasBound(Bound.START)) {
         start = this.deserializeToken((ByteBuffer)slice.bounds(Bound.START, options).get(0));
         BoundType startBoundType = toBoundType(slice.isInclusive(Bound.START));
         if(!slice.hasBound(Bound.END)) {
            return ImmutableRangeSet.of(Range.downTo(start, startBoundType));
         } else {
            BoundType endBoundType = toBoundType(slice.isInclusive(Bound.END));
            Token end = this.deserializeToken((ByteBuffer)slice.bounds(Bound.END, options).get(0));
            return start.equals(end) && (BoundType.OPEN == startBoundType || BoundType.OPEN == endBoundType)?ImmutableRangeSet.of():(start.compareTo(end) <= 0?ImmutableRangeSet.of(Range.range(start, startBoundType, end, endBoundType)):ImmutableRangeSet.builder().add(Range.upTo(end, endBoundType)).add(Range.downTo(start, startBoundType)).build());
         }
      } else {
         start = this.deserializeToken((ByteBuffer)slice.bounds(Bound.END, options).get(0));
         return ImmutableRangeSet.of(Range.upTo(start, toBoundType(slice.isInclusive(Bound.END))));
      }
   }

   private Token deserializeToken(ByteBuffer buffer) {
      return this.partitioner.getTokenFactory().fromByteArray(buffer);
   }

   private static BoundType toBoundType(boolean inclusive) {
      return inclusive?BoundType.CLOSED:BoundType.OPEN;
   }

   public ColumnMetadata getFirstColumn() {
      return this.restrictions.getFirstColumn();
   }

   public ColumnMetadata getLastColumn() {
      return this.restrictions.getLastColumn();
   }

   public List<ColumnMetadata> getColumnDefs() {
      return this.restrictions.getColumnDefs();
   }

   public void addFunctionsTo(List<Function> functions) {
      this.restrictions.addFunctionsTo(functions);
   }

   public boolean hasSupportingIndex(SecondaryIndexManager indexManager) {
      return this.restrictions.hasSupportingIndex(indexManager);
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
      this.restrictions.addRowFilterTo(filter, indexManager, options);
   }

   public boolean isEmpty() {
      return this.restrictions.isEmpty();
   }

   public int size() {
      return this.restrictions.size();
   }

   public boolean needFiltering(TableMetadata table) {
      return this.restrictions.needFiltering(table);
   }

   public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table) {
      return this.restrictions.hasUnrestrictedPartitionKeyComponents(table);
   }

   public boolean hasSlice() {
      return this.restrictions.hasSlice();
   }
}
