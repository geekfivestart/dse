package com.datastax.bdp.db.nodesync;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeSyncState {
   private static final Logger logger = LoggerFactory.getLogger(NodeSyncState.class);
   private final NodeSyncService service;
   private final LoadingCache<TableId, TableState> tableStates = CacheBuilder.newBuilder().weakValues().build(CacheLoader.from(this::load));

   NodeSyncState(NodeSyncService service) {
      this.service = service;
   }

   NodeSyncService service() {
      return this.service;
   }

   @Nullable
   public TableState get(TableMetadata table) {
      return (TableState)this.tableStates.getIfPresent(table.id);
   }

   TableState getOrLoad(TableMetadata table) {
      try {
         return (TableState)this.tableStates.get(table.id);
      } catch (ExecutionException var3) {
         if(var3.getCause() instanceof UnknownTableException) {
            throw (UnknownTableException)var3.getCause();
         } else {
            throw Throwables.propagate(var3.getCause());
         }
      }
   }

   private TableState load(TableId id) {
      ColumnFamilyStore table = Schema.instance.getColumnFamilyStoreInstance(id);
      if(table == null) {
         throw new UnknownTableException(id);
      } else {
         TableMetadata metadata = table.metadata();
         Collection<Range<Token>> localRanges = NodeSyncHelpers.localRanges(metadata.keyspace);
         int depth = Segments.depth(table, localRanges.size());
         return TableState.load(this.service, table.metadata(), localRanges, depth);
      }
   }
}
