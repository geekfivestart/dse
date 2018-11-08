package org.apache.cassandra.index.sasi;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.googlecode.concurrenttrees.common.Iterables;
import io.reactivex.Completable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.conf.IndexMode;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sasi.plan.QueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.MemtableSwitchedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

public class SASIIndex implements Index, INotificationConsumer {
   public static final String USAGE_WARNING = "SASI index was enabled for '%s.%s'. SASI is still in beta, take extra caution when using it in production.";
   private static final SASIIndex.SASIIndexBuildingSupport INDEX_BUILDER_SUPPORT = new SASIIndex.SASIIndexBuildingSupport(null);
   private final ColumnFamilyStore baseCfs;
   private final IndexMetadata config;
   private final ColumnIndex index;

   public SASIIndex(ColumnFamilyStore baseCfs, IndexMetadata config) {
      this.baseCfs = baseCfs;
      this.config = config;
      ColumnMetadata column = (ColumnMetadata)TargetParser.parse(baseCfs.metadata(), config).left;
      this.index = new ColumnIndex(baseCfs.metadata().partitionKeyType, column, config);
      Tracker tracker = baseCfs.getTracker();
      tracker.subscribe(this);
      SortedMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> toRebuild = new TreeMap((a, b) -> {
         return Integer.compare(a.descriptor.generation, b.descriptor.generation);
      });

      Object perSSTable;
      for(Iterator var6 = this.index.init(tracker.getView().liveSSTables()).iterator(); var6.hasNext(); ((Multimap)perSSTable).put(this.index.getDefinition(), this.index)) {
         SSTableReader sstable = (SSTableReader)var6.next();
         perSSTable = (Multimap)toRebuild.get(sstable);
         if(perSSTable == null) {
            toRebuild.put(sstable, perSSTable = HashMultimap.create());
         }
      }

      CompactionManager.instance.submitIndexBuild(new SASIIndexBuilder(baseCfs, toRebuild));
   }

   public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata) {
      if(!(metadata.partitioner instanceof Murmur3Partitioner)) {
         throw new ConfigurationException("SASI only supports Murmur3Partitioner.");
      } else {
         String targetColumn = (String)options.get("target");
         if(targetColumn == null) {
            throw new ConfigurationException("unknown target column");
         } else {
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(metadata, targetColumn);
            if(target == null) {
               throw new ConfigurationException("failed to retrieve target column for: " + targetColumn);
            } else if(((ColumnMetadata)target.left).isComplex()) {
               throw new ConfigurationException("complex columns are not yet supported by SASI");
            } else if(((ColumnMetadata)target.left).isPartitionKey()) {
               throw new ConfigurationException("partition key columns are not yet supported by SASI");
            } else {
               IndexMode.validateAnalyzer(options);
               IndexMode mode = IndexMode.getMode((ColumnMetadata)target.left, options);
               if(mode.mode == OnDiskIndexBuilder.Mode.SPARSE) {
                  if(mode.isLiteral) {
                     throw new ConfigurationException("SPARSE mode is only supported on non-literal columns.");
                  }

                  if(mode.isAnalyzed) {
                     throw new ConfigurationException("SPARSE mode doesn't support analyzers.");
                  }
               }

               return Collections.emptyMap();
            }
         }
      }
   }

   public void register(IndexRegistry registry) {
      registry.registerIndex(this);
   }

   public IndexMetadata getIndexMetadata() {
      return this.config;
   }

   public Callable<?> getInitializationTask() {
      return null;
   }

   public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
      return null;
   }

   public Callable<?> getBlockingFlushTask() {
      return null;
   }

   public Callable<?> getInvalidateTask() {
      return this.getTruncateTask(FBUtilities.timestampMicros());
   }

   public Callable<?> getTruncateTask(long truncatedAt) {
      return () -> {
         this.index.dropData(truncatedAt);
         return null;
      };
   }

   public boolean shouldBuildBlocking() {
      return true;
   }

   public Optional<ColumnFamilyStore> getBackingTable() {
      return Optional.empty();
   }

   public boolean indexes(RegularAndStaticColumns columns) {
      return columns.contains(this.index.getDefinition());
   }

   public boolean dependsOn(ColumnMetadata column) {
      return this.index.getDefinition().compareTo(column) == 0;
   }

   public boolean supportsExpression(ColumnMetadata column, Operator operator) {
      return this.dependsOn(column) && this.index.supports(operator);
   }

   public AbstractType<?> customExpressionValueType() {
      return null;
   }

   public RowFilter getPostIndexQueryFilter(RowFilter filter) {
      return filter.withoutExpressions();
   }

   public long getEstimatedResultRows() {
      return -9223372036854775808L;
   }

   public void validate(PartitionUpdate update) throws InvalidRequestException {
   }

   public Index.Indexer indexerFor(final DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, OpOrder.Group opGroup, final IndexTransaction.Type transactionType) {
      return new Index.Indexer() {
         public void begin() {
         }

         public Completable partitionDelete(DeletionTime deletionTime) {
            return Completable.complete();
         }

         public Completable rangeTombstone(RangeTombstone tombstone) {
            return Completable.complete();
         }

         public Completable insertRow(Row row) {
            return this.isNewData()?this.adjustMemtableSize(SASIIndex.this.index.index(key, row)):Completable.complete();
         }

         public Completable updateRow(Row oldRow, Row newRow) {
            return this.insertRow(newRow);
         }

         public Completable removeRow(Row row) {
            return Completable.complete();
         }

         public Completable finish() {
            return Completable.complete();
         }

         private boolean isNewData() {
            return transactionType == IndexTransaction.Type.UPDATE;
         }

         public Completable adjustMemtableSize(long additionalSpace) {
            SASIIndex.this.baseCfs.getTracker().getView().getCurrentMemtable().allocateExtraOnHeap(additionalSpace);
            return Completable.complete();
         }
      };
   }

   public Index.Searcher searcherFor(ReadCommand command) throws InvalidRequestException {
      TableMetadata config = command.metadata();
      ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(config.id);
      return (controller) -> {
         return (new QueryPlan(cfs, command, DatabaseDescriptor.getRangeRpcTimeout())).execute(controller);
      };
   }

   public SSTableFlushObserver getFlushObserver(Descriptor descriptor, OperationType opType) {
      Multimap<ColumnMetadata, ColumnIndex> indexes = HashMultimap.create();
      indexes.put(this.index.getDefinition(), this.index);
      return newWriter(this.baseCfs.metadata().partitionKeyType, descriptor, indexes, opType);
   }

   public BiFunction<Flow<FlowablePartition>, ReadCommand, Flow<FlowablePartition>> postProcessorFor(ReadCommand command) {
      return (partitions, readCommand) -> {
         return partitions;
      };
   }

   public Index.IndexBuildingSupport getBuildTaskSupport() {
      return INDEX_BUILDER_SUPPORT;
   }

   public void handleNotification(INotification notification, Object sender) {
      if(notification instanceof SSTableAddedNotification) {
         SSTableAddedNotification notice = (SSTableAddedNotification)notification;
         this.index.update(Collections.emptyList(), Iterables.toList(notice.added));
      } else if(notification instanceof SSTableListChangedNotification) {
         SSTableListChangedNotification notice = (SSTableListChangedNotification)notification;
         this.index.update(notice.removed, notice.added);
      } else if(notification instanceof MemtableRenewedNotification) {
         this.index.switchMemtable();
      } else if(notification instanceof MemtableSwitchedNotification) {
         this.index.switchMemtable(((MemtableSwitchedNotification)notification).memtable);
      } else if(notification instanceof MemtableDiscardedNotification) {
         this.index.discardMemtable(((MemtableDiscardedNotification)notification).memtable);
      }

   }

   public ColumnIndex getIndex() {
      return this.index;
   }

   protected static PerSSTableIndexWriter newWriter(AbstractType<?> keyValidator, Descriptor descriptor, Multimap<ColumnMetadata, ColumnIndex> indexes, OperationType opType) {
      return new PerSSTableIndexWriter(keyValidator, descriptor, opType, indexes);
   }

   private static class SASIIndexBuildingSupport implements Index.IndexBuildingSupport {
      private SASIIndexBuildingSupport() {
      }

      public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild) {
         NavigableMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> sstables = new TreeMap(Comparator.comparingInt((a) -> {
            return a.descriptor.generation;
         }));
         indexes.stream().filter((i) -> {
            return i instanceof SASIIndex;
         }).forEach((i) -> {
            SASIIndex sasi = (SASIIndex)i;
            sasi.index.dropData(sstablesToRebuild);
            sstablesToRebuild.stream().filter((sstable) -> {
               return !sasi.index.hasSSTable(sstable);
            }).forEach((sstable) -> {
               Multimap<ColumnMetadata, ColumnIndex> toBuild = (Multimap)sstables.get(sstable);
               if(toBuild == null) {
                  sstables.put(sstable, toBuild = HashMultimap.create());
               }

               ((Multimap)toBuild).put(sasi.index.getDefinition(), sasi.index);
            });
         });
         return new SASIIndexBuilder(cfs, sstables);
      }
   }
}
