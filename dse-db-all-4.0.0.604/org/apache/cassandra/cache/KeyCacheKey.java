package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class KeyCacheKey extends CacheKey {
   public Descriptor desc;
   private static final long EMPTY_SIZE;
   byte[] key;
   boolean copyKey;
   int keyOffset;
   int keyLength;

   public KeyCacheKey(TableMetadata tableMetadata, Descriptor desc, ByteBuffer key) {
      this(tableMetadata, desc, key, true);
   }

   public KeyCacheKey(TableMetadata tableMetadata, Descriptor desc, ByteBuffer key, boolean copyKey) {
      super(tableMetadata);
      this.desc = desc;
      this.copyKey = copyKey || key.isDirect();
      if(this.copyKey) {
         this.key = ByteBufferUtil.getArray(key);
         this.keyOffset = 0;
         this.keyLength = this.key.length;
      } else {
         this.key = key.array();
         this.keyOffset = key.arrayOffset() + key.position();
         this.keyLength = this.keyOffset + key.remaining();
      }

   }

   public byte[] key() {
      assert this.copyKey;

      return this.key;
   }

   public Descriptor desc() {
      return this.desc;
   }

   public String toString() {
      return String.format("KeyCacheKey(%s, %s)", new Object[]{this.desc, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(this.key, this.keyOffset, this.keyLength))});
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + (this.copyKey?ObjectSizes.sizeOfArray(this.key):0L);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && o.getClass().isAssignableFrom(this.getClass())) {
         KeyCacheKey that = (KeyCacheKey)o;
         return this.tableId.equals(that.tableId) && Objects.equals(this.indexName, that.indexName) && this.desc.equals(that.desc) && Arrays.equals(this.key, that.key);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.tableId.hashCode();
      result = 31 * result + Objects.hashCode(this.indexName);
      result = 31 * result + this.desc.hashCode();
      result = 31 * result + this.keyHashCode();
      return result;
   }

   private int keyHashCode() {
      if(this.key == null) {
         return 0;
      } else {
         int result = 1;
         int i = this.keyOffset;

         for(int length = this.keyLength; i < length; ++i) {
            result = 31 * result + this.key[i];
         }

         return result;
      }
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(new KeyCacheKey(TableMetadata.builder("ks", "tab").addPartitionKeyColumn((String)"pk", UTF8Type.instance).build(), (Descriptor)null, ByteBufferUtil.EMPTY_BYTE_BUFFER));
   }
}
