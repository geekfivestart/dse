package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.db.marshal.AbstractType;

public class NoOpAnalyzer extends AbstractAnalyzer {
   private ByteBuffer input;
   private boolean hasNext = false;

   public NoOpAnalyzer() {
   }

   public void init(Map<String, String> options, AbstractType validator) {
   }

   public boolean hasNext() {
      if(this.hasNext) {
         this.next = this.input;
         this.hasNext = false;
         return true;
      } else {
         return false;
      }
   }

   public void reset(ByteBuffer input) {
      this.next = null;
      this.input = input;
      this.hasNext = true;
   }
}
