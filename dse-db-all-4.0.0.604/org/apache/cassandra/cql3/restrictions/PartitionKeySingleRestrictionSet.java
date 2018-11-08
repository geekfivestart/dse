package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.TableMetadata;

final class PartitionKeySingleRestrictionSet extends RestrictionSetWrapper implements PartitionKeyRestrictions {
   protected final ClusteringComparator comparator;

   public PartitionKeySingleRestrictionSet(ClusteringComparator comparator) {
      super(new RestrictionSet());
      this.comparator = comparator;
   }

   private PartitionKeySingleRestrictionSet(PartitionKeySingleRestrictionSet restrictionSet, SingleRestriction restriction) {
      super(restrictionSet.restrictions.addRestriction(restriction));
      this.comparator = restrictionSet.comparator;
   }

   public PartitionKeyRestrictions mergeWith(Restriction restriction) {
      return (PartitionKeyRestrictions)(restriction.isOnToken()?(this.isEmpty()?(PartitionKeyRestrictions)restriction:new TokenFilter(this, (TokenRestriction)restriction)):new PartitionKeySingleRestrictionSet(this, (SingleRestriction)restriction));
   }

   public List<ByteBuffer> values(QueryOptions options) {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      Iterator var3 = this.restrictions.iterator();

      while(var3.hasNext()) {
         SingleRestriction r = (SingleRestriction)var3.next();
         r.appendTo(builder, options);
         if(builder.hasMissingElements()) {
            break;
         }
      }

      return builder.buildSerializedPartitionKeys();
   }

   public List<ByteBuffer> bounds(Bound bound, QueryOptions options) {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      Iterator var4 = this.restrictions.iterator();

      do {
         if(!var4.hasNext()) {
            return builder.buildSerializedPartitionKeys();
         }

         SingleRestriction r = (SingleRestriction)var4.next();
         r.appendBoundTo(builder, bound, options);
      } while(!builder.hasMissingElements());

      return Collections.emptyList();
   }

   public boolean hasBound(Bound b) {
      return this.isEmpty()?false:this.restrictions.lastRestriction().hasBound(b);
   }

   public boolean isInclusive(Bound b) {
      return this.isEmpty()?false:this.restrictions.lastRestriction().isInclusive(b);
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
      Iterator var4 = this.restrictions.iterator();

      while(var4.hasNext()) {
         SingleRestriction restriction = (SingleRestriction)var4.next();
         restriction.addRowFilterTo(filter, indexManager, options);
      }

   }

   public boolean needFiltering(TableMetadata table) {
      return this.isEmpty()?false:this.hasUnrestrictedPartitionKeyComponents(table) || this.hasSlice() || this.hasContains();
   }

   public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table) {
      return this.size() < table.partitionKeyColumns().size();
   }

   public boolean hasSlice() {
      Iterator var1 = this.restrictions.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         restriction = (SingleRestriction)var1.next();
      } while(!restriction.isSlice());

      return true;
   }
}
