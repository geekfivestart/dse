package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.schema.ColumnMetadata;

public enum Bound {
   START(0),
   END(1);

   public final int idx;

   private Bound(int idx) {
      this.idx = idx;
   }

   public Bound reverseIfNeeded(ColumnMetadata columnMetadata) {
      return columnMetadata.isReversedType()?this.reverse():this;
   }

   public Bound reverse() {
      return this.isStart()?END:START;
   }

   public boolean isStart() {
      return this == START;
   }

   public boolean isEnd() {
      return this == END;
   }
}
