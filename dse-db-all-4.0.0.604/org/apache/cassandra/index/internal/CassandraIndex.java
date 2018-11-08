package org.apache.cassandra.index.internal;

import com.google.common.collect.ImmutableSet;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.internal.composites.CompositesSearcher;
import org.apache.cassandra.index.internal.composites.StaticColumnsSearcher;
import org.apache.cassandra.index.internal.keys.KeysSearcher;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CassandraIndex implements Index {
   private static final Logger logger = LoggerFactory.getLogger(CassandraIndex.class);
   public final ColumnFamilyStore baseCfs;
   protected IndexMetadata metadata;
   protected ColumnFamilyStore indexCfs;
   protected ColumnMetadata indexedColumn;
   protected CassandraIndexFunctions functions;

   protected CassandraIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      this.baseCfs = baseCfs;
      this.setMetadata(indexDef);
   }

   protected boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator) {
      return operator == Operator.EQ;
   }

   protected abstract CBuilder buildIndexClusteringPrefix(ByteBuffer var1, ClusteringPrefix var2, CellPath var3);

   public abstract IndexEntry decodeEntry(DecoratedKey var1, Row var2);

   public abstract boolean isStale(Row var1, ByteBuffer var2, int var3);

   protected abstract ByteBuffer getIndexedValue(ByteBuffer var1, Clustering var2, CellPath var3, ByteBuffer var4);

   public ColumnMetadata getIndexedColumn() {
      return this.indexedColumn;
   }

   public ClusteringComparator getIndexComparator() {
      return this.indexCfs.metadata().comparator;
   }

   public ColumnFamilyStore getIndexCfs() {
      return this.indexCfs;
   }

   public void register(IndexRegistry registry) {
      registry.registerIndex(this);
   }

   public Callable<?> getInitializationTask() {
      return !this.isBuilt() && !this.baseCfs.isEmpty()?this.getBuildIndexTask():null;
   }

   public IndexMetadata getIndexMetadata() {
      return this.metadata;
   }

   public Optional<ColumnFamilyStore> getBackingTable() {
      return this.indexCfs == null?Optional.empty():Optional.of(this.indexCfs);
   }

   public Callable<Void> getBlockingFlushTask() {
      return () -> {
         this.indexCfs.forceBlockingFlush();
         return null;
      };
   }

   public Callable<?> getInvalidateTask() {
      return () -> {
         this.invalidate();
         return null;
      };
   }

   public Callable<?> getMetadataReloadTask(IndexMetadata indexDef) {
      return () -> {
         this.indexCfs.reload();
         return null;
      };
   }

   public void validate(ReadCommand command) throws InvalidRequestException {
      Optional<RowFilter.Expression> target = this.getTargetExpression(command.rowFilter().getExpressions());
      if(target.isPresent()) {
         ByteBuffer indexValue = ((RowFilter.Expression)target.get()).getIndexValue();
         RequestValidations.checkFalse(indexValue.remaining() > '\uffff', "Index expression values may not be larger than 64K");
      }

   }

   private void setMetadata(IndexMetadata indexDef) {
      this.metadata = indexDef;
      Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(this.baseCfs.metadata(), indexDef);
      this.functions = getFunctions(indexDef, target);
      TableMetadataRef tableRef = TableMetadataRef.forOfflineTools(indexCfsMetadata(this.baseCfs.metadata(), indexDef));
      this.indexCfs = ColumnFamilyStore.createColumnFamilyStore(this.baseCfs.keyspace, tableRef.name, tableRef, this.baseCfs.getTracker().loadsstables);
      this.indexedColumn = (ColumnMetadata)target.left;
   }

   public Callable<?> getTruncateTask(long truncatedAt) {
      return () -> {
         this.indexCfs.discardSSTables(truncatedAt);
         return null;
      };
   }

   public boolean shouldBuildBlocking() {
      return true;
   }

   public boolean dependsOn(ColumnMetadata column) {
      return this.indexedColumn.name.equals(column.name);
   }

   public boolean supportsExpression(ColumnMetadata column, Operator operator) {
      return this.indexedColumn.name.equals(column.name) && this.supportsOperator(this.indexedColumn, operator);
   }

   private boolean supportsExpression(RowFilter.Expression expression) {
      return this.supportsExpression(expression.column(), expression.operator());
   }

   public AbstractType<?> customExpressionValueType() {
      return null;
   }

   public long getEstimatedResultRows() {
      return (long)this.indexCfs.getMeanCells();
   }

   public BiFunction<Flow<FlowablePartition>, ReadCommand, Flow<FlowablePartition>> postProcessorFor(ReadCommand command) {
      return (partitions, readCommand) -> {
         return partitions;
      };
   }

   public RowFilter getPostIndexQueryFilter(RowFilter filter) {
      Optional var10000 = this.getTargetExpression(filter.getExpressions());
      filter.getClass();
      return (RowFilter)var10000.map(filter::without).orElse(filter);
   }

   private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions) {
      return expressions.stream().filter(this::supportsExpression).findFirst();
   }

   public Index.Searcher searcherFor(ReadCommand command) {
      Optional<RowFilter.Expression> target = this.getTargetExpression(command.rowFilter().getExpressions());
      if(target.isPresent()) {
         switch(null.$SwitchMap$org$apache$cassandra$schema$IndexMetadata$Kind[this.getIndexMetadata().kind.ordinal()]) {
         case 1:
            if(!this.indexedColumn.isStatic()) {
               return new CompositesSearcher(command, (RowFilter.Expression)target.get(), this);
            }

            return new StaticColumnsSearcher(command, (RowFilter.Expression)target.get(), this);
         case 2:
            return new KeysSearcher(command, (RowFilter.Expression)target.get(), this);
         default:
            throw new IllegalStateException(String.format("Unsupported index type %s for index %s on %s", new Object[]{this.metadata.kind, this.metadata.name, this.indexedColumn.name.toString()}));
         }
      } else {
         return null;
      }
   }

   public void validate(PartitionUpdate update) throws InvalidRequestException {
      switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[this.indexedColumn.kind.ordinal()]) {
      case 1:
         this.validatePartitionKey(update.partitionKey());
         break;
      case 2:
         this.validateClusterings(update);
         break;
      case 3:
         if(update.columns().regulars.contains(this.indexedColumn)) {
            this.validateRows(update);
         }
         break;
      case 4:
         if(update.columns().statics.contains(this.indexedColumn)) {
            this.validateRows(Collections.singleton(update.staticRow()));
         }
      }

   }

   public Index.Indexer indexerFor(final DecoratedKey key, RegularAndStaticColumns columns, final int nowInSec, final OpOrder.Group opGroup, IndexTransaction.Type transactionType) {
      return !this.isPrimaryKeyIndex() && !columns.contains(this.indexedColumn)?null:new Index.Indexer() {
         public void begin() {
         }

         public Completable partitionDelete(DeletionTime deletionTime) {
            return Completable.complete();
         }

         public Completable rangeTombstone(RangeTombstone tombstone) {
            return Completable.complete();
         }

         public Completable insertRow(Row row) {
            return row.isStatic() && !CassandraIndex.this.indexedColumn.isStatic() && !CassandraIndex.this.indexedColumn.isPartitionKey()?Completable.complete():(CassandraIndex.this.isPrimaryKeyIndex()?this.indexPrimaryKey(row.clustering(), this.getPrimaryKeyIndexLiveness(row), row.deletion()):(CassandraIndex.this.indexedColumn.isComplex()?this.indexCells(row.clustering(), row.getComplexColumnData(CassandraIndex.this.indexedColumn)):this.indexCell(row.clustering(), row.getCell(CassandraIndex.this.indexedColumn))));
         }

         public Completable removeRow(Row row) {
            return CassandraIndex.this.isPrimaryKeyIndex()?Completable.complete():(CassandraIndex.this.indexedColumn.isComplex()?this.removeCells(row.clustering(), row.getComplexColumnData(CassandraIndex.this.indexedColumn)):this.removeCell(row.clustering(), row.getCell(CassandraIndex.this.indexedColumn)));
         }

         public Completable updateRow(Row oldRow, Row newRow) {
            assert oldRow.isStatic() == newRow.isStatic();

            Completable result = Completable.complete();
            if(newRow.isStatic() != CassandraIndex.this.indexedColumn.isStatic()) {
               return result;
            } else {
               if(CassandraIndex.this.isPrimaryKeyIndex()) {
                  result = this.indexPrimaryKey(newRow.clustering(), newRow.primaryKeyLivenessInfo(), newRow.deletion());
               }

               return CassandraIndex.this.indexedColumn.isComplex()?result.concatWith(Completable.concatArray(new CompletableSource[]{this.indexCells(newRow.clustering(), newRow.getComplexColumnData(CassandraIndex.this.indexedColumn)), this.removeCells(oldRow.clustering(), oldRow.getComplexColumnData(CassandraIndex.this.indexedColumn))})):result.concatWith(Completable.concatArray(new CompletableSource[]{this.indexCell(newRow.clustering(), newRow.getCell(CassandraIndex.this.indexedColumn)), this.removeCell(oldRow.clustering(), oldRow.getCell(CassandraIndex.this.indexedColumn))}));
            }
         }

         public Completable finish() {
            return Completable.complete();
         }

         private Completable indexCells(Clustering clustering, Iterable<Cell> cells) {
            if(cells == null) {
               return Completable.complete();
            } else {
               Completable result = Completable.complete();

               Cell cell;
               for(Iterator var4 = cells.iterator(); var4.hasNext(); result = result.concatWith(this.indexCell(clustering, cell))) {
                  cell = (Cell)var4.next();
               }

               return result;
            }
         }

         private Completable indexCell(Clustering clustering, Cell cell) {
            return cell != null && cell.isLive(nowInSec)?CassandraIndex.this.insert(key.getKey(), clustering, cell, LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime()), opGroup):Completable.complete();
         }

         private Completable removeCells(Clustering clustering, Iterable<Cell> cells) {
            if(cells == null) {
               return Completable.complete();
            } else {
               Completable result = Completable.complete();

               Cell cell;
               for(Iterator var4 = cells.iterator(); var4.hasNext(); result = result.concatWith(this.removeCell(clustering, cell))) {
                  cell = (Cell)var4.next();
               }

               return result;
            }
         }

         private Completable removeCell(Clustering clustering, Cell cell) {
            return cell != null && cell.isLive(nowInSec)?CassandraIndex.this.delete(key.getKey(), clustering, cell, opGroup, nowInSec):Completable.complete();
         }

         private Completable indexPrimaryKey(Clustering clustering, LivenessInfo liveness, Row.Deletion deletion) {
            Completable result = Completable.complete();
            if(liveness.timestamp() != -9223372036854775808L) {
               result = result.concatWith(CassandraIndex.this.insert(key.getKey(), clustering, (Cell)null, liveness, opGroup));
            }

            if(!deletion.isLive()) {
               result = result.concatWith(CassandraIndex.this.delete(key.getKey(), clustering, deletion.time(), opGroup));
            }

            return result;
         }

         private LivenessInfo getPrimaryKeyIndexLiveness(Row row) {
            long timestamp = row.primaryKeyLivenessInfo().timestamp();
            int ttl = row.primaryKeyLivenessInfo().ttl();
            Iterator var5 = row.cells().iterator();

            while(var5.hasNext()) {
               Cell cell = (Cell)var5.next();
               long cellTimestamp = cell.timestamp();
               if(cell.isLive(nowInSec) && cellTimestamp > timestamp) {
                  timestamp = cellTimestamp;
                  ttl = cell.ttl();
               }
            }

            return LivenessInfo.create(timestamp, ttl, nowInSec);
         }
      };
   }

   public Completable deleteStaleEntry(DecoratedKey indexKey, Clustering indexClustering, DeletionTime deletion, OpOrder.Group opGroup) {
      logger.trace("Removed index entry for stale value {}", indexKey);
      return this.doDelete(indexKey, indexClustering, deletion, opGroup);
   }

   private Completable insert(ByteBuffer rowKey, Clustering clustering, Cell cell, LivenessInfo info, OpOrder.Group opGroup) {
      DecoratedKey valueKey = this.getIndexKeyFor(this.getIndexedValue(rowKey, clustering, cell));
      Row row = ArrayBackedRow.noCellLiveRow(this.buildIndexClustering(rowKey, clustering, cell), info);
      PartitionUpdate upd = this.partitionUpdate(valueKey, row);
      logger.trace("Inserting entry into index for value {}", valueKey);
      return this.indexCfs.apply(upd, UpdateTransaction.NO_OP, opGroup, (CommitLogPosition)null);
   }

   private Completable delete(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec) {
      DecoratedKey valueKey = this.getIndexKeyFor(this.getIndexedValue(rowKey, clustering, cell));
      return this.doDelete(valueKey, this.buildIndexClustering(rowKey, clustering, cell), new DeletionTime(cell.timestamp(), nowInSec), opGroup);
   }

   private Completable delete(ByteBuffer rowKey, Clustering clustering, DeletionTime deletion, OpOrder.Group opGroup) {
      DecoratedKey valueKey = this.getIndexKeyFor(this.getIndexedValue(rowKey, clustering, (Cell)null));
      return this.doDelete(valueKey, this.buildIndexClustering(rowKey, clustering, (Cell)null), deletion, opGroup);
   }

   private Completable doDelete(DecoratedKey indexKey, Clustering indexClustering, DeletionTime deletion, OpOrder.Group opGroup) {
      Row row = ArrayBackedRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletion));
      PartitionUpdate upd = this.partitionUpdate(indexKey, row);
      logger.trace("Removing index entry for value {}", indexKey);
      return this.indexCfs.apply(upd, UpdateTransaction.NO_OP, opGroup, (CommitLogPosition)null);
   }

   private void validatePartitionKey(DecoratedKey partitionKey) throws InvalidRequestException {
      assert this.indexedColumn.isPartitionKey();

      this.validateIndexedValue(this.getIndexedValue(partitionKey.getKey(), (Clustering)null, (Cell)null));
   }

   private void validateClusterings(PartitionUpdate update) throws InvalidRequestException {
      assert this.indexedColumn.isClusteringColumn();

      Iterator var2 = update.iterator();

      while(var2.hasNext()) {
         Row row = (Row)var2.next();
         this.validateIndexedValue(this.getIndexedValue((ByteBuffer)null, row.clustering(), (Cell)null));
      }

   }

   private void validateRows(Iterable<Row> rows) {
      assert !this.indexedColumn.isPrimaryKeyColumn();

      Iterator var2 = rows.iterator();

      while(true) {
         ComplexColumnData data;
         label30:
         do {
            while(var2.hasNext()) {
               Row row = (Row)var2.next();
               if(this.indexedColumn.isComplex()) {
                  data = row.getComplexColumnData(this.indexedColumn);
                  continue label30;
               }

               this.validateIndexedValue(this.getIndexedValue((ByteBuffer)null, (Clustering)null, row.getCell(this.indexedColumn)));
            }

            return;
         } while(data == null);

         Iterator var5 = data.iterator();

         while(var5.hasNext()) {
            Cell cell = (Cell)var5.next();
            this.validateIndexedValue(this.getIndexedValue((ByteBuffer)null, (Clustering)null, cell.path(), cell.value()));
         }
      }
   }

   private void validateIndexedValue(ByteBuffer value) {
      if(value != null && value.remaining() >= '\uffff') {
         throw new InvalidRequestException(String.format("Cannot index value of size %d for index %s on %s(%s) (maximum allowed size=%d)", new Object[]{Integer.valueOf(value.remaining()), this.metadata.name, this.baseCfs.metadata, this.indexedColumn.name.toString(), Integer.valueOf('\uffff')}));
      }
   }

   private ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, Cell cell) {
      return this.getIndexedValue(rowKey, clustering, cell == null?null:cell.path(), cell == null?null:cell.value());
   }

   private Clustering buildIndexClustering(ByteBuffer rowKey, Clustering clustering, Cell cell) {
      return this.buildIndexClusteringPrefix(rowKey, clustering, cell == null?null:cell.path()).build();
   }

   private DecoratedKey getIndexKeyFor(ByteBuffer value) {
      return this.indexCfs.decorateKey(value);
   }

   private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row) {
      return PartitionUpdate.singleRowUpdate(this.indexCfs.metadata(), valueKey, row);
   }

   private void invalidate() {
      Collection<ColumnFamilyStore> cfss = Collections.singleton(this.indexCfs);
      CompactionManager.instance.interruptCompactionForCFs(cfss);
      CompactionManager.instance.waitForCessation(cfss);
      Keyspace.writeOrder.awaitNewBarrier();
      this.indexCfs.forceBlockingFlush();
      this.indexCfs.readOrdering.awaitNewBarrier();
      this.indexCfs.invalidate();
   }

   private boolean isBuilt() {
      return ((Boolean)TPCUtils.blockingGet(SystemKeyspace.isIndexBuilt(this.baseCfs.keyspace.getName(), this.metadata.name))).booleanValue();
   }

   private boolean isPrimaryKeyIndex() {
      return this.indexedColumn.isPrimaryKeyColumn();
   }

   private Callable<?> getBuildIndexTask() {
      return () -> {
         this.buildBlocking();
         return null;
      };
   }

   private void buildBlocking() {
      this.baseCfs.forceBlockingFlush();
      ColumnFamilyStore.RefViewFragment viewFragment = this.baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
      Throwable var2 = null;

      label243: {
         try {
            Refs<SSTableReader> sstables = viewFragment.refs;
            Throwable var4 = null;

            try {
               if(!sstables.isEmpty()) {
                  logger.info("Submitting index build of {} for data in {}", this.metadata.name, getSSTableNames(sstables));
                  SecondaryIndexBuilder builder = new CollatedViewIndexBuilder(this.baseCfs, Collections.singleton(this), new ReducingKeyIterator(sstables));
                  Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
                  FBUtilities.waitOnFuture(future);
                  this.indexCfs.forceBlockingFlush();
                  break label243;
               }

               logger.info("No SSTable data for {}.{} to build index {} from, marking empty index as built", new Object[]{this.baseCfs.metadata.keyspace, this.baseCfs.metadata.name, this.metadata.name});
            } catch (Throwable var31) {
               var4 = var31;
               throw var31;
            } finally {
               if(sstables != null) {
                  if(var4 != null) {
                     try {
                        sstables.close();
                     } catch (Throwable var30) {
                        var4.addSuppressed(var30);
                     }
                  } else {
                     sstables.close();
                  }
               }

            }
         } catch (Throwable var33) {
            var2 = var33;
            throw var33;
         } finally {
            if(viewFragment != null) {
               if(var2 != null) {
                  try {
                     viewFragment.close();
                  } catch (Throwable var29) {
                     var2.addSuppressed(var29);
                  }
               } else {
                  viewFragment.close();
               }
            }

         }

         return;
      }

      logger.info("Index build of {} complete", this.metadata.name);
   }

   private static String getSSTableNames(Collection<SSTableReader> sstables) {
      return (String)StreamSupport.stream(sstables.spliterator(), false).map(SSTable::toString).collect(Collectors.joining(", "));
   }

   public static TableMetadata indexCfsMetadata(TableMetadata baseCfsMetadata, IndexMetadata indexMetadata) {
      Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfsMetadata, indexMetadata);
      CassandraIndexFunctions utils = getFunctions(indexMetadata, target);
      ColumnMetadata indexedColumn = (ColumnMetadata)target.left;
      AbstractType<?> indexedValueType = utils.getIndexedValueType(indexedColumn);
      TableMetadata.Builder builder = TableMetadata.builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id).isDense(indexMetadata.isKeys()).isCompound(!indexMetadata.isKeys()).partitioner(new LocalPartitioner(indexedValueType)).addPartitionKeyColumn(indexedColumn.name, indexedValueType).addClusteringColumn("partition_key", baseCfsMetadata.partitioner.partitionOrdering());
      if(indexMetadata.isKeys()) {
         CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(ImmutableSet.of(indexedColumn.name.toString(), "partition_key"));
         builder.addRegularColumn((String)names.defaultCompactValueName(), EmptyType.instance);
      } else {
         utils.addIndexClusteringColumns(builder, baseCfsMetadata, indexedColumn);
      }

      return builder.build().updateIndexTableMetadata(baseCfsMetadata.params);
   }

   public static CassandraIndex newIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata) {
      return getFunctions(indexMetadata, TargetParser.parse(baseCfs.metadata(), indexMetadata)).newIndexInstance(baseCfs, indexMetadata);
   }

   static CassandraIndexFunctions getFunctions(IndexMetadata indexDef, Pair<ColumnMetadata, IndexTarget.Type> target) {
      if(indexDef.isKeys()) {
         return CassandraIndexFunctions.KEYS_INDEX_FUNCTIONS;
      } else {
         ColumnMetadata indexedColumn = (ColumnMetadata)target.left;
         if(indexedColumn.type.isCollection() && indexedColumn.type.isMultiCell()) {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)indexedColumn.type).kind.ordinal()]) {
            case 1:
               return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
            case 2:
               return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
            case 3:
               switch(null.$SwitchMap$org$apache$cassandra$cql3$statements$IndexTarget$Type[((IndexTarget.Type)target.right).ordinal()]) {
               case 1:
                  return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
               case 2:
                  return CassandraIndexFunctions.COLLECTION_ENTRY_INDEX_FUNCTIONS;
               case 3:
                  return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
               default:
                  throw new AssertionError();
               }
            }
         }

         switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[indexedColumn.kind.ordinal()]) {
         case 1:
            return CassandraIndexFunctions.PARTITION_KEY_INDEX_FUNCTIONS;
         case 2:
            return CassandraIndexFunctions.CLUSTERING_COLUMN_INDEX_FUNCTIONS;
         case 3:
         case 4:
            return CassandraIndexFunctions.REGULAR_COLUMN_INDEX_FUNCTIONS;
         default:
            throw new AssertionError();
         }
      }
   }
}
