package org.apache.cassandra.cql3.restrictions;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;

public interface SingleRestriction extends Restriction {
   default boolean isSlice() {
      return false;
   }

   default boolean isEQ() {
      return false;
   }

   default boolean isLIKE() {
      return false;
   }

   default boolean isIN() {
      return false;
   }

   default boolean isContains() {
      return false;
   }

   default boolean isNotNull() {
      return false;
   }

   default boolean isMultiColumn() {
      return false;
   }

   default boolean hasBound(Bound b) {
      return true;
   }

   default boolean isInclusive(Bound b) {
      return true;
   }

   SingleRestriction mergeWith(SingleRestriction var1);

   MultiCBuilder appendTo(MultiCBuilder var1, QueryOptions var2);

   default MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options) {
      return this.appendTo(builder, options);
   }
}
