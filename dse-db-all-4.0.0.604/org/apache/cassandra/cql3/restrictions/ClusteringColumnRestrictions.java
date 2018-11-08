package org.apache.cassandra.cql3.restrictions;

import java.util.Iterator;
import java.util.NavigableSet;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;

final class ClusteringColumnRestrictions extends RestrictionSetWrapper {
   protected final ClusteringComparator comparator;
   private final boolean allowFiltering;

   public ClusteringColumnRestrictions(TableMetadata table) {
      this(table, false);
   }

   public ClusteringColumnRestrictions(TableMetadata table, boolean allowFiltering) {
      this(table.comparator, new RestrictionSet(), allowFiltering);
   }

   private ClusteringColumnRestrictions(ClusteringComparator comparator, RestrictionSet restrictionSet, boolean allowFiltering) {
      super(restrictionSet);
      this.comparator = comparator;
      this.allowFiltering = allowFiltering;
   }

   public ClusteringColumnRestrictions mergeWith(Restriction restriction) throws InvalidRequestException {
      SingleRestriction newRestriction = (SingleRestriction)restriction;
      RestrictionSet newRestrictionSet = this.restrictions.addRestriction(newRestriction);
      if(!this.isEmpty() && !this.allowFiltering) {
         SingleRestriction lastRestriction = this.restrictions.lastRestriction();
         ColumnMetadata lastRestrictionStart = lastRestriction.getFirstColumn();
         ColumnMetadata newRestrictionStart = restriction.getFirstColumn();
         RequestValidations.checkFalse(lastRestriction.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(), "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)", newRestrictionStart.name, lastRestrictionStart.name);
         if(newRestrictionStart.position() < lastRestrictionStart.position() && newRestriction.isSlice()) {
            throw RequestValidations.invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)", new Object[]{this.restrictions.nextColumn(newRestrictionStart).name, newRestrictionStart.name});
         }
      }

      return new ClusteringColumnRestrictions(this.comparator, newRestrictionSet, this.allowFiltering);
   }

   private boolean hasMultiColumnSlice() {
      Iterator var1 = this.restrictions.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         restriction = (SingleRestriction)var1.next();
      } while(!restriction.isMultiColumn() || !restriction.isSlice());

      return true;
   }

   public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      Iterator var3 = this.restrictions.iterator();

      while(var3.hasNext()) {
         SingleRestriction r = (SingleRestriction)var3.next();
         r.appendTo(builder, options);
         if(builder.hasMissingElements()) {
            break;
         }
      }

      return builder.build();
   }

   public NavigableSet<ClusteringBound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN() || this.hasMultiColumnSlice());
      int keyPosition = 0;

      SingleRestriction r;
      for(Iterator var5 = this.restrictions.iterator(); var5.hasNext(); keyPosition = r.getLastColumn().position() + 1) {
         r = (SingleRestriction)var5.next();
         if(this.handleInFilter(r, keyPosition)) {
            break;
         }

         if(r.isSlice()) {
            r.appendBoundTo(builder, bound, options);
            return builder.buildBoundForSlice(bound.isStart(), r.isInclusive(bound), r.isInclusive(bound.reverse()), r.getColumnDefs());
         }

         r.appendBoundTo(builder, bound, options);
         if(builder.hasMissingElements()) {
            return BTreeSet.empty(this.comparator);
         }
      }

      return builder.buildBound(bound.isStart(), true);
   }

   public final boolean hasContains() {
      Iterator var1 = this.restrictions.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         restriction = (SingleRestriction)var1.next();
      } while(!restriction.isContains());

      return true;
   }

   public final boolean hasSlice() {
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

   public final boolean needFiltering() {
      int position = 0;
      Iterator var2 = this.restrictions.iterator();

      while(var2.hasNext()) {
         SingleRestriction restriction = (SingleRestriction)var2.next();
         if(this.handleInFilter(restriction, position)) {
            return true;
         }

         if(!restriction.isSlice()) {
            position = restriction.getLastColumn().position() + 1;
         }
      }

      return this.hasContains();
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException {
      int position = 0;
      Iterator var5 = this.restrictions.iterator();

      while(true) {
         while(var5.hasNext()) {
            SingleRestriction restriction = (SingleRestriction)var5.next();
            if(!this.handleInFilter(restriction, position) && !restriction.hasSupportingIndex(indexManager)) {
               if(!restriction.isSlice()) {
                  position = restriction.getLastColumn().position() + 1;
               }
            } else {
               restriction.addRowFilterTo(filter, indexManager, options);
            }
         }

         return;
      }
   }

   private boolean handleInFilter(SingleRestriction restriction, int index) {
      return restriction.isContains() || restriction.isLIKE() || index != restriction.getFirstColumn().position();
   }
}
