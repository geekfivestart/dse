package org.apache.cassandra.index.sasi.conf.view;

import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.trie.KeyAnalyzer;
import org.apache.cassandra.index.sasi.utils.trie.PatriciaTrie;
import org.apache.cassandra.index.sasi.utils.trie.Trie;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class PrefixTermTree extends RangeTermTree {
   private final OnDiskIndexBuilder.Mode mode;
   private final Trie<ByteBuffer, Set<SSTableIndex>> trie;

   public PrefixTermTree(ByteBuffer min, ByteBuffer max, Trie<ByteBuffer, Set<SSTableIndex>> trie, IntervalTree<RangeTermTree.Term, SSTableIndex, Interval<RangeTermTree.Term, SSTableIndex>> ranges, OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator) {
      super(min, max, ranges, comparator);
      this.mode = mode;
      this.trie = trie;
   }

   public Set<SSTableIndex> search(Expression e) {
      Map<ByteBuffer, Set<SSTableIndex>> indexes = e != null && e.lower != null && this.mode != OnDiskIndexBuilder.Mode.CONTAINS?this.trie.prefixMap(e.lower.value):this.trie;
      Set<SSTableIndex> view = new HashSet(((Map)indexes).size());
      (indexes).values().forEach(view::addAll);
      return Sets.union(view, super.search(e));
   }

   private static class ByteBufferKeyAnalyzer implements KeyAnalyzer<ByteBuffer> {
      private final AbstractType<?> comparator;
      private static final int MSB = 128;

      public ByteBufferKeyAnalyzer(AbstractType<?> comparator) {
         this.comparator = comparator;
      }

      public int compare(ByteBuffer a, ByteBuffer b) {
         return this.comparator.compare(a, b);
      }

      public int lengthInBits(ByteBuffer o) {
         return o.remaining() * 8;
      }

      public boolean isBitSet(ByteBuffer key, int bitIndex) {
         if(bitIndex >= this.lengthInBits(key)) {
            return false;
         } else {
            int index = bitIndex / 8;
            int bit = bitIndex % 8;
            return (key.get(index) & this.mask(bit)) != 0;
         }
      }

      public int bitIndex(ByteBuffer key, ByteBuffer otherKey) {
         int length = Math.max(key.remaining(), otherKey.remaining());
         boolean allNull = true;

         for(int i = 0; i < length; ++i) {
            byte b1 = this.valueAt(key, i);
            byte b2 = this.valueAt(otherKey, i);
            if(b1 != b2) {
               int xor = b1 ^ b2;

               for(int j = 0; j < 8; ++j) {
                  if((xor & this.mask(j)) != 0) {
                     return i * 8 + j;
                  }
               }
            }

            if(b1 != 0) {
               allNull = false;
            }
         }

         return allNull?-1:-2;
      }

      public boolean isPrefix(ByteBuffer key, ByteBuffer prefix) {
         if(key.remaining() < prefix.remaining()) {
            return false;
         } else {
            for(int i = 0; i < prefix.remaining(); ++i) {
               if(key.get(i) != prefix.get(i)) {
                  return false;
               }
            }

            return true;
         }
      }

      private byte valueAt(ByteBuffer value, int index) {
         return index >= 0 && index < value.remaining()?value.get(index):0;
      }

      private int mask(int bit) {
         return 128 >>> bit;
      }
   }

   public static class Builder extends RangeTermTree.Builder {
      private final PatriciaTrie<ByteBuffer, Set<SSTableIndex>> trie;

      protected Builder(OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator) {
         super(mode, comparator);
         this.trie = new PatriciaTrie(new PrefixTermTree.ByteBufferKeyAnalyzer(comparator));
      }

      public void addIndex(SSTableIndex index) {
         super.addIndex(index);
         this.addTerm(index.minTerm(), index);
         this.addTerm(index.maxTerm(), index);
      }

      public TermTree build() {
         return new PrefixTermTree(this.min, this.max, this.trie, IntervalTree.build(this.intervals), this.mode, this.comparator);
      }

      private void addTerm(ByteBuffer term, SSTableIndex index) {
         Set<SSTableIndex> indexes = (Set)this.trie.get(term);
         if(indexes == null) {
            this.trie.put(term, indexes = new HashSet());
         }

         ((Set)indexes).add(index);
      }
   }
}
