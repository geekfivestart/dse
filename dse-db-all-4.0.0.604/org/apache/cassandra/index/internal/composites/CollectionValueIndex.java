package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

public class CollectionValueIndex extends CassandraIndex {
   public CollectionValueIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
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

      if(prefix.size() == this.baseCfs.metadata().clusteringColumns().size() && path != null) {
         builder.add(path.get(0));
      }

      return builder;
   }

   public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry) {
      Clustering clustering = indexEntry.clustering();
      Clustering indexedEntryClustering = null;
      if(this.getIndexedColumn().isStatic()) {
         indexedEntryClustering = Clustering.STATIC_CLUSTERING;
      } else {
         CBuilder builder = CBuilder.create(this.baseCfs.getComparator());

         for(int i = 0; i < this.baseCfs.getComparator().size(); ++i) {
            builder.add(clustering.get(i + 1));
         }

         indexedEntryClustering = builder.build();
      }

      return new IndexEntry(indexedValue, clustering, indexEntry.primaryKeyLivenessInfo().timestamp(), clustering.get(0), indexedEntryClustering);
   }

   public boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator) {
      return operator == Operator.CONTAINS && !(indexedColumn.type instanceof SetType);
   }

   public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
      ColumnMetadata columnDef = this.indexedColumn;
      ComplexColumnData complexData = data.getComplexColumnData(columnDef);
      if(complexData == null) {
         return true;
      } else {
         Iterator var6 = complexData.iterator();

         Cell cell;
         do {
            if(!var6.hasNext()) {
               return true;
            }

            cell = (Cell)var6.next();
         } while(!cell.isLive(nowInSec) || ((CollectionType)columnDef.type).valueComparator().compare(indexValue, cell.value()) != 0);

         return false;
      }
   }
}
