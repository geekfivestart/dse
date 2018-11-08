package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFactory {
   public static final IFilter AlwaysPresent = new AlwaysPresentFilter();
   private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
   private static final long BITSET_EXCESS = 20L;

   public FilterFactory() {
   }

   public static void serialize(IFilter bf, DataOutputPlus output) throws IOException {
      BloomFilterSerializer.serialize((BloomFilter)bf, output);
   }

   public static IFilter deserialize(DataInput input, boolean offheap) throws IOException {
      return BloomFilterSerializer.deserialize(input, offheap);
   }

   public static IFilter getFilter(long numElements, int targetBucketsPerElem, boolean offheap) {
      int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
      int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
      if(bucketsPerElement < targetBucketsPerElem) {
         logger.warn("Cannot provide an optimal BloomFilter for {} elements ({}/{} buckets per element).", new Object[]{Long.valueOf(numElements), Integer.valueOf(bucketsPerElement), Integer.valueOf(targetBucketsPerElem)});
      }

      BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
      return createFilter(spec.K, numElements, spec.bucketsPerElement, offheap);
   }

   public static IFilter getFilter(long numElements, double maxFalsePosProbability, boolean offheap) {
      assert maxFalsePosProbability <= 1.0D : "Invalid probability";

      if(maxFalsePosProbability == 1.0D) {
         return new AlwaysPresentFilter();
      } else {
         int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
         BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
         return createFilter(spec.K, numElements, spec.bucketsPerElement, offheap);
      }
   }

   private static IFilter createFilter(int hash, long numElements, int bucketsPer, boolean offheap) {
      long numBits = numElements * (long)bucketsPer + 20L;
      IBitSet bitset = offheap?new OffHeapBitSet(numBits):new OpenBitSet(numBits);
      return new BloomFilter(hash, (IBitSet)bitset);
   }
}
