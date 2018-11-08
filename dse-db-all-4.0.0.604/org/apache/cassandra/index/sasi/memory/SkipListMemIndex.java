package org.apache.cassandra.index.sasi.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;

public class SkipListMemIndex extends MemIndex {
   public static final int CSLM_OVERHEAD = 128;
   private final ConcurrentSkipListMap<ByteBuffer, ConcurrentSkipListSet<DecoratedKey>> index;

   public SkipListMemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex) {
      super(keyValidator, columnIndex);
      this.index = new ConcurrentSkipListMap(columnIndex.getValidator());
   }

   public long add(DecoratedKey key, ByteBuffer value) {
      long overhead = 128L;
      ConcurrentSkipListSet<DecoratedKey> keys = (ConcurrentSkipListSet)this.index.get(value);
      if(keys == null) {
         ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet(DecoratedKey.comparator);
         keys = (ConcurrentSkipListSet)this.index.putIfAbsent(value, newKeys);
         if(keys == null) {
            overhead += (long)(128 + value.remaining());
            keys = newKeys;
         }
      }

      keys.add(key);
      return overhead;
   }

   public RangeIterator<Long, Token> search(Expression expression) {
      ByteBuffer min = expression.lower == null?null:expression.lower.value;
      ByteBuffer max = expression.upper == null?null:expression.upper.value;
      if(min == null && max == null) {
         throw new IllegalArgumentException();
      } else {
         ConcurrentNavigableMap search;
         if(min != null && max != null) {
            search = this.index.subMap(min, expression.lower.inclusive, max, expression.upper.inclusive);
         } else if(min == null) {
            search = this.index.headMap(max, expression.upper.inclusive);
         } else {
            search = this.index.tailMap(min, expression.lower.inclusive);
         }

         RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
         search.values().stream().filter((keys) -> {
            return !keys.isEmpty();
         }).forEach((keys) -> {
            builder.add(new KeyRangeIterator(keys));
         });
         return builder.build();
      }
   }
}
