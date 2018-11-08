package org.apache.cassandra.index.internal;

import com.google.common.collect.UnmodifiableIterator;
import java.util.List;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.index.internal.composites.ClusteringColumnIndex;
import org.apache.cassandra.index.internal.composites.CollectionEntryIndex;
import org.apache.cassandra.index.internal.composites.CollectionKeyIndex;
import org.apache.cassandra.index.internal.composites.CollectionValueIndex;
import org.apache.cassandra.index.internal.composites.PartitionKeyIndex;
import org.apache.cassandra.index.internal.composites.RegularColumnIndex;
import org.apache.cassandra.index.internal.keys.KeysIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

public interface CassandraIndexFunctions {
   CassandraIndexFunctions KEYS_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new KeysIndex(baseCfs, indexMetadata);
      }
   };
   CassandraIndexFunctions REGULAR_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new RegularColumnIndex(baseCfs, indexMetadata);
      }
   };
   CassandraIndexFunctions CLUSTERING_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new ClusteringColumnIndex(baseCfs, indexMetadata);
      }

      public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder, TableMetadata baseMetadata, ColumnMetadata columnDef) {
         List<ColumnMetadata> cks = baseMetadata.clusteringColumns();

         int i;
         ColumnMetadata def;
         for(i = 0; i < columnDef.position(); ++i) {
            def = (ColumnMetadata)cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
         }

         for(i = columnDef.position() + 1; i < cks.size(); ++i) {
            def = (ColumnMetadata)cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
         }

         return builder;
      }
   };
   CassandraIndexFunctions PARTITION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new PartitionKeyIndex(baseCfs, indexMetadata);
      }
   };
   CassandraIndexFunctions COLLECTION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new CollectionKeyIndex(baseCfs, indexMetadata);
      }

      public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn) {
         return ((CollectionType)indexedColumn.type).nameComparator();
      }
   };
   CassandraIndexFunctions COLLECTION_VALUE_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new CollectionValueIndex(baseCfs, indexMetadata);
      }

      public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn) {
         return ((CollectionType)indexedColumn.type).valueComparator();
      }

      public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder, TableMetadata baseMetadata, ColumnMetadata columnDef) {
         UnmodifiableIterator var4 = baseMetadata.clusteringColumns().iterator();

         while(var4.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var4.next();
            builder.addClusteringColumn(def.name, def.type);
         }

         builder.addClusteringColumn("cell_path", ((CollectionType)columnDef.type).nameComparator());
         return builder;
      }
   };
   CassandraIndexFunctions COLLECTION_ENTRY_INDEX_FUNCTIONS = new CassandraIndexFunctions() {
      public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
         return new CollectionEntryIndex(baseCfs, indexMetadata);
      }

      public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn) {
         CollectionType colType = (CollectionType)indexedColumn.type;
         return CompositeType.getInstance(new AbstractType[]{colType.nameComparator(), colType.valueComparator()});
      }
   };

   CassandraIndex newIndexInstance(ColumnFamilyStore var1, IndexMetadata var2);

   default AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn) {
      return indexedColumn.type;
   }

   default TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder, TableMetadata baseMetadata, ColumnMetadata cfDef) {
      UnmodifiableIterator var4 = baseMetadata.clusteringColumns().iterator();

      while(var4.hasNext()) {
         ColumnMetadata def = (ColumnMetadata)var4.next();
         builder.addClusteringColumn(def.name, def.type);
      }

      return builder;
   }
}
