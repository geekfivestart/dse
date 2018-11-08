package org.apache.cassandra.utils;

import java.util.Arrays;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class RangeHash {
   public final Range<Token> range;
   public final byte[] hash;

   public RangeHash(Range<Token> range, byte[] hash) {
      this.range = range;
      this.hash = hash;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         RangeHash rangeHash = (RangeHash)o;
         return !this.range.equals(rangeHash.range)?false:Arrays.equals(this.hash, rangeHash.hash);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.range.hashCode();
      result = 31 * result + Arrays.hashCode(this.hash);
      return result;
   }

   public boolean isNonEmpty() {
      return this.hash != null && this.hash != MerkleTree.EMPTY_HASH;
   }
}
