package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;

public abstract class DecoratedKey extends PartitionPosition implements IFilter.FilterKey {
   public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>() {
      public int compare(DecoratedKey o1, DecoratedKey o2) {
         return o1.compareTo((PartitionPosition)o2);
      }
   };
   private int hashCode = -1;

   public DecoratedKey(Token token) {
      super(token, PartitionPosition.Kind.ROW_KEY);

      assert token != null;

   }

   public int hashCode() {
      int currHashCode = this.hashCode;
      if(currHashCode == -1) {
         currHashCode = this.getKey().hashCode();
         this.hashCode = currHashCode;
      }

      return currHashCode;
   }

   public boolean equals(Object obj) {
      if(this == obj) {
         return true;
      } else if(obj != null && obj instanceof DecoratedKey) {
         DecoratedKey other = (DecoratedKey)obj;
         return ByteBufferUtil.compareUnsigned(this.getKey(), other.getKey()) == 0;
      } else {
         return false;
      }
   }

   public int compareTo(PartitionPosition pos) {
      if(this == pos) {
         return 0;
      } else {
         int cmp = this.token.compareTo(pos.token);
         return cmp != 0?cmp:(pos.kind != PartitionPosition.Kind.ROW_KEY?-pos.compareTo(this):ByteBufferUtil.compareUnsigned(this.getKey(), ((DecoratedKey)pos).getKey()));
      }
   }

   public static int compareTo(IPartitioner partitioner, ByteBuffer key, PartitionPosition position) {
      if(!(position instanceof DecoratedKey)) {
         return -position.compareTo(partitioner.decorateKey(key));
      } else {
         DecoratedKey otherKey = (DecoratedKey)position;
         int cmp = partitioner.getToken(key).compareTo(otherKey.getToken());
         return cmp == 0?ByteBufferUtil.compareUnsigned(key, otherKey.getKey()):cmp;
      }
   }

   public ByteSource asByteComparableSource() {
      return ByteSource.of(new ByteSource[]{this.token.asByteComparableSource(), ByteSource.of(this.getKey())});
   }

   public IPartitioner getPartitioner() {
      return this.token.getPartitioner();
   }

   public Token.KeyBound minValue() {
      return this.getPartitioner().getMinimumToken().minKeyBound();
   }

   public boolean isMinimum() {
      return false;
   }

   public String toString() {
      String keystring = this.getKey() == null?"null":ByteBufferUtil.bytesToHex(this.getKey());
      return "DecoratedKey(" + this.getToken() + ", " + keystring + ")";
   }

   public abstract ByteBuffer getKey();

   public void filterHash(long[] dest) {
      ByteBuffer key = this.getKey();
      MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0L, dest);
   }
}
