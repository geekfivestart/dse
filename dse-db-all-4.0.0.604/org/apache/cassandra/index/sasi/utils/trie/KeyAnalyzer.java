package org.apache.cassandra.index.sasi.utils.trie;

import java.util.Comparator;

public interface KeyAnalyzer<K> extends Comparator<K> {
   int NULL_BIT_KEY = -1;
   int EQUAL_BIT_KEY = -2;
   int OUT_OF_BOUNDS_BIT_KEY = -3;

   int lengthInBits(K var1);

   boolean isBitSet(K var1, int var2);

   int bitIndex(K var1, K var2);

   boolean isPrefix(K var1, K var2);
}
