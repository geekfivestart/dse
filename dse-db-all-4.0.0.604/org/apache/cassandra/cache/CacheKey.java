package org.apache.cassandra.cache;

import java.util.Objects;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public abstract class CacheKey implements IMeasurableMemory {
   public final TableId tableId;
   public final String indexName;

   protected CacheKey(TableId tableId, String indexName) {
      this.tableId = tableId;
      this.indexName = indexName;
   }

   public CacheKey(TableMetadata metadata) {
      this(metadata.id, (String)metadata.indexName().orElse((Object)null));
   }

   public boolean sameTable(TableMetadata tableMetadata) {
      return this.tableId.equals(tableMetadata.id) && Objects.equals(this.indexName, tableMetadata.indexName().orElse((Object)null));
   }
}
