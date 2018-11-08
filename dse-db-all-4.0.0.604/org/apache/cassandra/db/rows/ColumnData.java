package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import java.util.Comparator;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.schema.ColumnMetadata;

public abstract class ColumnData {
   public static final Comparator<ColumnData> comparator = (cd1, cd2) -> {
      return cd1.column().compareTo(cd2.column());
   };
   protected final ColumnMetadata column;

   protected ColumnData(ColumnMetadata column) {
      this.column = column;
   }

   public final ColumnMetadata column() {
      return this.column;
   }

   public abstract int dataSize();

   public abstract long unsharedHeapSizeExcludingData();

   public abstract void validate();

   public abstract void digest(Hasher var1);

   public abstract ColumnData updateAllTimestamp(long var1);

   public abstract ColumnData markCounterLocalToBeCleared();

   public abstract ColumnData purge(DeletionPurger var1, int var2);

   public abstract long maxTimestamp();
}
