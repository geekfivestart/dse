package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongSet;
import com.google.common.primitives.Longs;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.utils.CombinedValue;

public abstract class Token implements CombinedValue<Long>, Iterable<DecoratedKey> {
   protected final long token;

   public Token(long token) {
      this.token = token;
   }

   public Long get() {
      return Long.valueOf(this.token);
   }

   public abstract LongSet getOffsets();

   public int compareTo(CombinedValue<Long> o) {
      return Longs.compare(this.token, ((Token)o).token);
   }
}
