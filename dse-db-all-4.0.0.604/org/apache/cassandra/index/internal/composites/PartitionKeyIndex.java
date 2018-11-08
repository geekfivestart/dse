package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

public class PartitionKeyIndex extends CassandraIndex {
   private final boolean enforceStrictLiveness;

   public PartitionKeyIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
      this.enforceStrictLiveness = baseCfs.metadata.get().enforceStrictLiveness();
   }

   public ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path, ByteBuffer cellValue) {
      CompositeType keyComparator = (CompositeType)this.baseCfs.metadata().partitionKeyType;
      ByteBuffer[] components = keyComparator.split(partitionKey);
      return components[this.indexedColumn.position()];
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
      int ckCount = this.baseCfs.metadata().clusteringColumns().size();
      Clustering clustering = indexEntry.clustering();
      CBuilder builder = CBuilder.create(this.baseCfs.getComparator());

      for(int i = 0; i < ckCount; ++i) {
         builder.add(clustering.get(i + 1));
      }

      return new IndexEntry(indexedValue, clustering, indexEntry.primaryKeyLivenessInfo().timestamp(), clustering.get(0), builder.build());
   }

   public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
      return !data.hasLiveData(nowInSec, this.enforceStrictLiveness);
   }
}
