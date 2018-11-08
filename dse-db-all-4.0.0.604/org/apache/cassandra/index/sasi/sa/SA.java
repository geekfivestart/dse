package org.apache.cassandra.index.sasi.sa;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public abstract class SA<T extends Buffer> {
   protected final AbstractType<?> comparator;
   protected final OnDiskIndexBuilder.Mode mode;
   protected final List<Term<T>> terms = new ArrayList();
   protected int charCount = 0;

   public SA(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode) {
      this.comparator = comparator;
      this.mode = mode;
   }

   public OnDiskIndexBuilder.Mode getMode() {
      return this.mode;
   }

   public void add(ByteBuffer termValue, TokenTreeBuilder tokens) {
      Term<T> term = this.getTerm(termValue, tokens);
      this.terms.add(term);
      this.charCount += term.length();
   }

   public abstract TermIterator finish();

   protected abstract Term<T> getTerm(ByteBuffer var1, TokenTreeBuilder var2);
}
