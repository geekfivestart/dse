package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public final class DroppedColumn {
   public final ColumnMetadata column;
   public final long droppedTime;

   public DroppedColumn(ColumnMetadata column, long droppedTime) {
      this.column = column;
      this.droppedTime = droppedTime;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof DroppedColumn)) {
         return false;
      } else {
         DroppedColumn dc = (DroppedColumn)o;
         return this.column.equals(dc.column) && this.droppedTime == dc.droppedTime;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column, Long.valueOf(this.droppedTime)});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("column", this.column).add("droppedTime", this.droppedTime).toString();
   }
}
