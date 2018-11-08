package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;
import org.apache.cassandra.utils.obs.IBitSet;

public class BloomFilter extends WrappedSharedCloseable implements IFilter {
   private static final FastThreadLocal<long[]> reusableIndexes = new FastThreadLocal<long[]>() {
      protected long[] initialValue() {
         return new long[21];
      }
   };
   public final IBitSet bitset;
   public final int hashCount;

   BloomFilter(int hashCount, IBitSet bitset) {
      super((AutoCloseable)bitset);
      this.hashCount = hashCount;
      this.bitset = bitset;
   }

   private BloomFilter(BloomFilter copy) {
      super((WrappedSharedCloseable)copy);
      this.hashCount = copy.hashCount;
      this.bitset = copy.bitset;
   }

   public long serializedSize() {
      return BloomFilterSerializer.serializedSize(this);
   }

   @VisibleForTesting
   public long[] getHashBuckets(IFilter.FilterKey key, int hashCount, long max) {
      long[] hash = new long[2];
      key.filterHash(hash);
      long[] indexes = new long[hashCount];
      this.setIndexes(hash[1], hash[0], hashCount, max, indexes);
      return indexes;
   }

   private long[] indexes(IFilter.FilterKey key) {
      long[] indexes = (long[])reusableIndexes.get();
      key.filterHash(indexes);
      this.setIndexes(indexes[1], indexes[0], this.hashCount, this.bitset.capacity(), indexes);
      return indexes;
   }

   private void setIndexes(long base, long inc, int count, long max, long[] results) {
      for(int i = 0; i < count; ++i) {
         results[i] = FBUtilities.abs(base % max);
         base += inc;
      }

   }

   public void add(IFilter.FilterKey key) {
      long[] indexes = this.indexes(key);

      for(int i = 0; i < this.hashCount; ++i) {
         this.bitset.set(indexes[i]);
      }

   }

   public final boolean isPresent(IFilter.FilterKey key) {
      long[] indexes = this.indexes(key);

      for(int i = 0; i < this.hashCount; ++i) {
         if(!this.bitset.get(indexes[i])) {
            return false;
         }
      }

      return true;
   }

   public void clear() {
      this.bitset.clear();
   }

   public IFilter sharedCopy() {
      return new BloomFilter(this);
   }

   public long offHeapSize() {
      return this.bitset.offHeapSize();
   }

   public String toString() {
      return "BloomFilter[hashCount=" + this.hashCount + ";capacity=" + this.bitset.capacity() + ']';
   }

   public void addTo(Ref.IdentityCollection identities) {
      super.addTo(identities);
      this.bitset.addTo(identities);
   }
}
