package org.apache.cassandra.index.sasi.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

public abstract class MemIndex {
   protected final AbstractType<?> keyValidator;
   protected final ColumnIndex columnIndex;

   protected MemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex) {
      this.keyValidator = keyValidator;
      this.columnIndex = columnIndex;
   }

   public abstract long add(DecoratedKey var1, ByteBuffer var2);

   public abstract RangeIterator<Long, Token> search(Expression var1);

   public static MemIndex forColumn(AbstractType<?> keyValidator, ColumnIndex columnIndex) {
      return (MemIndex)(columnIndex.isLiteral()?new TrieMemIndex(keyValidator, columnIndex):new SkipListMemIndex(keyValidator, columnIndex));
   }
}
