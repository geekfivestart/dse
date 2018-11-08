package org.apache.cassandra.index.sasi.sa;

import com.google.common.base.Charsets;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public class CharTerm extends Term<CharBuffer> {
   public CharTerm(int position, CharBuffer value, TokenTreeBuilder tokens) {
      super(position, value, tokens);
   }

   public ByteBuffer getTerm() {
      return Charsets.UTF_8.encode(((CharBuffer)this.value).duplicate());
   }

   public ByteBuffer getSuffix(int start) {
      return Charsets.UTF_8.encode(((CharBuffer)this.value).subSequence(((CharBuffer)this.value).position() + start, ((CharBuffer)this.value).remaining()));
   }

   public int compareTo(AbstractType<?> comparator, Term other) {
      return ((CharBuffer)this.value).compareTo((CharBuffer)other.value);
   }

   public int length() {
      return ((CharBuffer)this.value).length();
   }
}
