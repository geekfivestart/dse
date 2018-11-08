package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.schema.TableMetadata;

public interface Unfiltered extends Clusterable {
   Unfiltered.Kind kind();

   void digest(Hasher var1);

   void validateData(TableMetadata var1);

   boolean isEmpty();

   String toString(TableMetadata var1);

   String toString(TableMetadata var1, boolean var2);

   String toString(TableMetadata var1, boolean var2, boolean var3);

   default boolean isRow() {
      return this.kind() == Unfiltered.Kind.ROW;
   }

   default boolean isRangeTombstoneMarker() {
      return this.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER;
   }

   Unfiltered purge(DeletionPurger var1, int var2, boolean var3);

   public static enum Kind {
      ROW,
      RANGE_TOMBSTONE_MARKER;

      private Kind() {
      }
   }
}
