package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;

public class MutableKeyCacheKey extends KeyCacheKey {
   public MutableKeyCacheKey(TableMetadata metadata, Descriptor desc, ByteBuffer key) {
      super(metadata, desc, key, false);
   }

   public void mutate(Descriptor desc, ByteBuffer key) {
      this.desc = desc;

      assert !key.isDirect();

      this.key = key.array();
      this.keyOffset = key.arrayOffset() + key.position();
      this.keyLength = key.remaining();
   }
}
