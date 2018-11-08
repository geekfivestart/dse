package org.apache.cassandra.index.sasi.sa;

import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.utils.Pair;

public abstract class TermIterator extends AbstractIterator<Pair<IndexedTerm, TokenTreeBuilder>> {
   public TermIterator() {
   }

   public abstract ByteBuffer minTerm();

   public abstract ByteBuffer maxTerm();
}
