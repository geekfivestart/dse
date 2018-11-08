package org.apache.cassandra.cql3.restrictions;

import java.util.List;
import java.util.Set;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;

class RestrictionSetWrapper implements Restrictions {
   protected final RestrictionSet restrictions;

   public RestrictionSetWrapper(RestrictionSet restrictions) {
      this.restrictions = restrictions;
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) {
      this.restrictions.addRowFilterTo(filter, indexManager, options);
   }

   public List<ColumnMetadata> getColumnDefs() {
      return this.restrictions.getColumnDefs();
   }

   public void addFunctionsTo(List<Function> functions) {
      this.restrictions.addFunctionsTo(functions);
   }

   public boolean isEmpty() {
      return this.restrictions.isEmpty();
   }

   public int size() {
      return this.restrictions.size();
   }

   public boolean hasSupportingIndex(SecondaryIndexManager indexManager) {
      return this.restrictions.hasSupportingIndex(indexManager);
   }

   public ColumnMetadata getFirstColumn() {
      return this.restrictions.getFirstColumn();
   }

   public ColumnMetadata getLastColumn() {
      return this.restrictions.getLastColumn();
   }

   public boolean hasIN() {
      return this.restrictions.hasIN();
   }

   public boolean hasContains() {
      return this.restrictions.hasContains();
   }

   public boolean hasSlice() {
      return this.restrictions.hasSlice();
   }

   public boolean hasOnlyEqualityRestrictions() {
      return this.restrictions.hasOnlyEqualityRestrictions();
   }

   public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
      return this.restrictions.getRestrictions(columnDef);
   }
}
