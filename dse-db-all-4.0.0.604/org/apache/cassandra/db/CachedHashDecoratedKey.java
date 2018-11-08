package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.dht.Token;

public class CachedHashDecoratedKey extends BufferDecoratedKey {
   long hash0;
   long hash1;
   volatile boolean hashCached = false;

   public CachedHashDecoratedKey(Token token, ByteBuffer key) {
      super(token, key);
   }

   public void filterHash(long[] dest) {
      if(this.hashCached) {
         dest[0] = this.hash0;
         dest[1] = this.hash1;
      } else {
         super.filterHash(dest);
         this.hash0 = dest[0];
         this.hash1 = dest[1];
         this.hashCached = true;
      }

   }
}
