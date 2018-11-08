package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

public class RegularColumnIndex extends CassandraIndex {
   public RegularColumnIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
   }

   public ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path, ByteBuffer cellValue) {
      return cellValue;
   }

   public CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey, ClusteringPrefix prefix, CellPath path) {
      CBuilder builder = CBuilder.create(this.getIndexComparator());
      builder.add(partitionKey);

      for(int i = 0; i < prefix.size(); ++i) {
         builder.add(prefix.get(i));
      }

      return builder;
   }

   public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry) {
      Clustering clustering = indexEntry.clustering();
      Clustering indexedEntryClustering = null;
      if(this.getIndexedColumn().isStatic()) {
         indexedEntryClustering = Clustering.STATIC_CLUSTERING;
      } else {
         ClusteringComparator baseComparator = this.baseCfs.getComparator();
         CBuilder builder = CBuilder.create(baseComparator);

         for(int i = 0; i < baseComparator.size(); ++i) {
            builder.add(clustering.get(i + 1));
         }

         indexedEntryClustering = builder.build();
      }

      return new IndexEntry(indexedValue, clustering, indexEntry.primaryKeyLivenessInfo().timestamp(), clustering.get(0), indexedEntryClustering);
   }

   public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
      Cell cell = data.getCell(this.indexedColumn);
      return cell == null || !cell.isLive(nowInSec) || this.indexedColumn.type.compare(indexValue, cell.value()) != 0;
   }
}
