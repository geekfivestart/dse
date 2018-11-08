package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

public abstract class CollectionKeyIndexBase extends CassandraIndex {
   public CollectionKeyIndexBase(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
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
         int count = 1 + this.baseCfs.metadata().clusteringColumns().size();
         CBuilder builder = CBuilder.create(this.baseCfs.getComparator());

         for(int i = 0; i < count - 1; ++i) {
            builder.add(clustering.get(i + 1));
         }

         indexedEntryClustering = builder.build();
      }

      return new IndexEntry(indexedValue, clustering, indexEntry.primaryKeyLivenessInfo().timestamp(), clustering.get(0), indexedEntryClustering);
   }
}
