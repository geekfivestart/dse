package org.apache.cassandra.index.sasi.sa;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public abstract class Term<T extends Buffer> {
   protected final int position;
   protected final T value;
   protected TokenTreeBuilder tokens;

   public Term(int position, T value, TokenTreeBuilder tokens) {
      this.position = position;
      this.value = value;
      this.tokens = tokens;
   }

   public int getPosition() {
      return this.position;
   }

   public abstract ByteBuffer getTerm();

   public abstract ByteBuffer getSuffix(int var1);

   public TokenTreeBuilder getTokens() {
      return this.tokens;
   }

   public abstract int compareTo(AbstractType<?> var1, Term var2);

   public abstract int length();
}
