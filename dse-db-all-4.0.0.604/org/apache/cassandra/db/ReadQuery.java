package org.apache.cassandra.db;

import javax.annotation.Nullable;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;

public interface ReadQuery extends Monitorable {
   static default ReadQuery empty(final TableMetadata metadata) {
      return new ReadQuery() {
         public TableMetadata metadata() {
            return metadata;
         }

         public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
            return Flow.empty();
         }

         public Flow<FlowablePartition> executeInternal(Monitor monitor) {
            return Flow.empty();
         }

         public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor) {
            return Flow.empty();
         }

         public boolean isEmpty() {
            return true;
         }

         public ReadExecutionController executionController() {
            return ReadExecutionController.empty();
         }

         public DataLimits limits() {
            return DataLimits.cqlLimits(0);
         }

         public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion) {
            return QueryPager.EMPTY;
         }

         public boolean selectsKey(DecoratedKey key) {
            return false;
         }

         public boolean selectsClustering(DecoratedKey key, Clustering clustering) {
            return false;
         }

         public int nowInSec() {
            return FBUtilities.nowInSeconds();
         }

         public boolean selectsFullPartition() {
            return false;
         }

         public boolean queriesOnlyLocalData() {
            return true;
         }

         public String toCQLString() {
            return "<EMPTY>";
         }
      };
   }

   TableMetadata metadata();

   ReadExecutionController executionController();

   Flow<FlowablePartition> execute(ReadContext var1) throws RequestExecutionException;

   Flow<FlowablePartition> executeInternal(@Nullable Monitor var1);

   default Flow<FlowablePartition> executeInternal() {
      return this.executeInternal((Monitor)null);
   }

   Flow<FlowableUnfilteredPartition> executeLocally(@Nullable Monitor var1);

   default Flow<FlowableUnfilteredPartition> executeLocally() {
      return this.executeLocally((Monitor)null);
   }

   QueryPager getPager(PagingState var1, ProtocolVersion var2);

   DataLimits limits();

   boolean selectsKey(DecoratedKey var1);

   boolean selectsClustering(DecoratedKey var1, Clustering var2);

   int nowInSec();

   boolean queriesOnlyLocalData();

   String toCQLString();

   default String name() {
      return this.toCQLString();
   }

   boolean selectsFullPartition();

   default UnfilteredPartitionIterator executeForTests() {
      return FlowablePartitions.toPartitions(FlowablePartitions.skipEmptyUnfilteredPartitions(this.executeLocally()), this.metadata());
   }

   default PartitionIterator executeInternalForTests() {
      return FlowablePartitions.toPartitionsFiltered(this.executeInternal());
   }

   default boolean isEmpty() {
      return false;
   }

   default ReadContext.Builder applyDefaults(ReadContext.Builder ctx) {
      return ctx;
   }
}
