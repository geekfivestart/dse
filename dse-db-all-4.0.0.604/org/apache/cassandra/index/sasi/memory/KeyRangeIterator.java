package org.apache.cassandra.index.sasi.memory;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.collect.PeekingIterator;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

public class KeyRangeIterator extends RangeIterator<Long, Token> {
   private final KeyRangeIterator.DKIterator iterator;

   public KeyRangeIterator(ConcurrentSkipListSet<DecoratedKey> keys) {
      super((Long)((DecoratedKey)keys.first()).getToken().getTokenValue(), (Long)((DecoratedKey)keys.last()).getToken().getTokenValue(), (long)keys.size());
      this.iterator = new KeyRangeIterator.DKIterator(keys.iterator());
   }

   protected Token computeNext() {
      return (Token)(this.iterator.hasNext()?new KeyRangeIterator.DKToken((DecoratedKey)this.iterator.next()):(Token)this.endOfData());
   }

   protected void performSkipTo(Long nextToken) {
      while(true) {
         if(this.iterator.hasNext()) {
            DecoratedKey key = (DecoratedKey)this.iterator.peek();
            if(Long.compare(((Long)key.getToken().getTokenValue()).longValue(), nextToken.longValue()) < 0) {
               this.iterator.next();
               continue;
            }
         }

         return;
      }
   }

   public void close() throws IOException {
   }

   private static class DKToken extends Token {
      private final SortedSet<DecoratedKey> keys;

      public DKToken(final DecoratedKey key) {
         super(((Long)key.getToken().getTokenValue()).longValue());
         this.keys = new TreeSet<DecoratedKey>(DecoratedKey.comparator) {
            {
               this.add(key);
            }
         };
      }

      public LongSet getOffsets() {
         LongSet offsets = new LongHashSet(4);
         Iterator var2 = this.keys.iterator();

         while(var2.hasNext()) {
            DecoratedKey key = (DecoratedKey)var2.next();
            offsets.add(((Long)key.getToken().getTokenValue()).longValue());
         }

         return offsets;
      }

      public void merge(CombinedValue<Long> other) {
         if(other instanceof Token) {
            Token o = (Token)other;

            assert o.get().equals(Long.valueOf(this.token));

            if(o instanceof KeyRangeIterator.DKToken) {
               this.keys.addAll(((KeyRangeIterator.DKToken)o).keys);
            } else {
               Iterator var3 = o.iterator();

               while(var3.hasNext()) {
                  DecoratedKey key = (DecoratedKey)var3.next();
                  this.keys.add(key);
               }
            }

         }
      }

      public Iterator<DecoratedKey> iterator() {
         return this.keys.iterator();
      }
   }

   private static class DKIterator extends AbstractIterator<DecoratedKey> implements PeekingIterator<DecoratedKey> {
      private final Iterator<DecoratedKey> keys;

      public DKIterator(Iterator<DecoratedKey> keys) {
         this.keys = keys;
      }

      protected DecoratedKey computeNext() {
         return this.keys.hasNext()?(DecoratedKey)this.keys.next():(DecoratedKey)this.endOfData();
      }
   }
}
