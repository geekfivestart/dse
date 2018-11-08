package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

public class KeysIndex extends CassandraIndex {
   public KeysIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
   }

   public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder, TableMetadataRef baseMetadata, ColumnMetadata cfDef) {
      return builder;
   }

   protected CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey, ClusteringPrefix prefix, CellPath path) {
      CBuilder builder = CBuilder.create(this.getIndexComparator());
      builder.add(partitionKey);
      return builder;
   }

   protected ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path, ByteBuffer cellValue) {
      return cellValue;
   }

   public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry) {
      throw new UnsupportedOperationException("KEYS indexes do not use a specialized index entry format");
   }

   public boolean isStale(Row row, ByteBuffer indexValue, int nowInSec) {
      if(row == null) {
         return true;
      } else {
         Cell cell = row.getCell(this.indexedColumn);
         return cell == null || !cell.isLive(nowInSec) || this.indexedColumn.type.compare(indexValue, cell.value()) != 0;
      }
   }
}
