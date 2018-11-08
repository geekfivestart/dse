package org.apache.cassandra.cache;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public final class RowCacheKey extends CacheKey {
   public final byte[] key;
   private static final long EMPTY_SIZE = ObjectSizes.measure(new RowCacheKey((TableId)null, (String)null, new byte[0]));

   public RowCacheKey(TableId tableId, String indexName, byte[] key) {
      super(tableId, indexName);
      this.key = key;
   }

   public RowCacheKey(TableMetadata metadata, DecoratedKey key) {
      super(metadata);
      this.key = ByteBufferUtil.getArray(key.getKey());

      assert this.key != null;

   }

   @VisibleForTesting
   public RowCacheKey(TableId tableId, String indexName, ByteBuffer key) {
      super(tableId, indexName);
      this.key = ByteBufferUtil.getArray(key);

      assert this.key != null;

   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + ObjectSizes.sizeOfArray(this.key);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         RowCacheKey that = (RowCacheKey)o;
         return this.tableId.equals(that.tableId) && Objects.equals(this.indexName, that.indexName) && Arrays.equals(this.key, that.key);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.tableId.hashCode();
      result = 31 * result + Objects.hashCode(this.indexName);
      result = 31 * result + (this.key != null?Arrays.hashCode(this.key):0);
      return result;
   }

   public String toString() {
      TableMetadataRef tableRef = Schema.instance.getTableMetadataRef(this.tableId);
      return String.format("RowCacheKey(%s, %s, key:%s)", new Object[]{tableRef, this.indexName, Arrays.toString(this.key)});
   }
}
