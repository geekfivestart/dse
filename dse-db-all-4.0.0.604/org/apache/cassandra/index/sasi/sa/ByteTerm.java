package org.apache.cassandra.index.sasi.sa;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public class ByteTerm extends Term<ByteBuffer> {
   public ByteTerm(int position, ByteBuffer value, TokenTreeBuilder tokens) {
      super(position, value, tokens);
   }

   public ByteBuffer getTerm() {
      return ((ByteBuffer)this.value).duplicate();
   }

   public ByteBuffer getSuffix(int start) {
      return (ByteBuffer)((ByteBuffer)this.value).duplicate().position(((ByteBuffer)this.value).position() + start);
   }

   public int compareTo(AbstractType<?> comparator, Term other) {
      return comparator.compare((ByteBuffer)this.value, (ByteBuffer)other.value);
   }

   public int length() {
      return ((ByteBuffer)this.value).remaining();
   }
}
