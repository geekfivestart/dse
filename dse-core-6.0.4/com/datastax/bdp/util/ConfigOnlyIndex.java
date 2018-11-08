package com.datastax.bdp.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.index.Index.Searcher;
import org.apache.cassandra.index.transactions.IndexTransaction.Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.apache.cassandra.utils.flow.Flow;

public class ConfigOnlyIndex implements Index {
   private IndexMetadata indexMetadata;
   private ColumnFamilyStore baseCfs;

   public void reset() {
   }

   public ConfigOnlyIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata) {
      this.baseCfs = baseCfs;
      this.indexMetadata = metadata;
      if(!((Boolean)TPCUtils.blockingGet(SystemKeyspace.isIndexBuilt(baseCfs.name, this.getIndexName()))).booleanValue()) {
         TPCUtils.blockingAwait(SystemKeyspace.setIndexBuilt(baseCfs.name, this.getIndexName()));
      }

   }

   public boolean supportsExpression(ColumnMetadata column, Operator operator) {
      return false;
   }

   public RowFilter getPostIndexQueryFilter(RowFilter filter) {
      return filter;
   }

   public Indexer indexerFor(DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, Group opGroup, Type transactionType) {
      return null;
   }

   public Callable<?> getInitializationTask() {
      return null;
   }

   public IndexMetadata getIndexMetadata() {
      return this.indexMetadata;
   }

   public String getIndexName() {
      return this.indexMetadata != null?this.indexMetadata.name:"config-only";
   }

   public void register(IndexRegistry registry) {
      registry.registerIndex(this);
   }

   public Optional<ColumnFamilyStore> getBackingTable() {
      return Optional.empty();
   }

   public Collection<ColumnMetadata> getIndexedColumns() {
      return Collections.emptySet();
   }

   public Callable<?> getBlockingFlushTask() {
      return null;
   }

   public Callable<?> getTruncateTask(long truncatedAt) {
      return null;
   }

   public Callable<?> getInvalidateTask() {
      return null;
   }

   public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
      return null;
   }

   public long getEstimatedResultRows() {
      return 0L;
   }

   public void validate(PartitionUpdate update) throws InvalidRequestException {
   }

   public Searcher searcherFor(ReadCommand command) {
      return null;
   }

   public BiFunction<Flow<FlowablePartition>, ReadCommand, Flow<FlowablePartition>> postProcessorFor(ReadCommand command) {
      return null;
   }

   public boolean shouldBuildBlocking() {
      return false;
   }

   public AbstractType<?> customExpressionValueType() {
      return null;
   }

   public boolean dependsOn(ColumnMetadata column) {
      return false;
   }
}
