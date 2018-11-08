package org.apache.cassandra.index.sasi.sa;

import java.nio.ByteBuffer;

public class IndexedTerm {
   private final ByteBuffer term;
   private final boolean isPartial;

   public IndexedTerm(ByteBuffer term, boolean isPartial) {
      this.term = term;
      this.isPartial = isPartial;
   }

   public ByteBuffer getBytes() {
      return this.term;
   }

   public boolean isPartial() {
      return this.isPartial;
   }
}
