package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Function;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscription;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.apache.cassandra.utils.flow.FlowTransform;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataLimits {
   private static final Logger logger = LoggerFactory.getLogger(DataLimits.class);
   public static final Versioned<ReadVerbs.ReadVersion, DataLimits.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x) -> {
      return new DataLimits.Serializer(x);
   });
   public static final int NO_ROWS_LIMIT = 2147483647;
   public static final int NO_BYTES_LIMIT = 2147483647;
   public static final DataLimits NONE = new DataLimits.CQLLimits(2147483647) {
      public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         return false;
      }
   };
   public static final DataLimits DISTINCT_NONE = new DataLimits.CQLLimits(2147483647, 1, true);

   public DataLimits() {
   }

   public static DataLimits cqlLimits(int cqlRowLimit) {
      return (DataLimits)(cqlRowLimit == 2147483647?NONE:new DataLimits.CQLLimits(cqlRowLimit));
   }

   public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit) {
      return (DataLimits)(cqlRowLimit == 2147483647 && perPartitionLimit == 2147483647?NONE:new DataLimits.CQLLimits(cqlRowLimit, perPartitionLimit));
   }

   private static DataLimits cqlLimits(int bytesLimit, int cqlRowLimit, int perPartitionLimit, boolean isDistinct) {
      return (DataLimits)(bytesLimit == 2147483647 && cqlRowLimit == 2147483647 && perPartitionLimit == 2147483647 && !isDistinct?NONE:new DataLimits.CQLLimits(bytesLimit, cqlRowLimit, perPartitionLimit, isDistinct));
   }

   public static DataLimits groupByLimits(int groupLimit, int groupPerPartitionLimit, int rowLimit, AggregationSpecification groupBySpec) {
      return new DataLimits.CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
   }

   public static DataLimits distinctLimits(int cqlRowLimit) {
      return DataLimits.CQLLimits.distinct(cqlRowLimit);
   }

   public abstract DataLimits.Kind kind();

   public abstract boolean isUnlimited();

   public abstract boolean isDistinct();

   public boolean isGroupByLimit() {
      return false;
   }

   public boolean isExhausted(DataLimits.Counter counter) {
      return counter.bytesCounted() < this.bytes() && counter.counted() < this.count();
   }

   public abstract DataLimits forPaging(PageSize var1);

   public abstract DataLimits forPaging(PageSize var1, ByteBuffer var2, int var3);

   public abstract DataLimits forShortReadRetry(int var1);

   public DataLimits forGroupByInternalPaging(GroupingState state) {
      throw new UnsupportedOperationException();
   }

   public abstract boolean hasEnoughLiveData(CachedPartition var1, int var2, boolean var3, boolean var4);

   public abstract DataLimits.Counter newCounter(int var1, boolean var2, boolean var3, boolean var4);

   public abstract int bytes();

   public abstract int count();

   public abstract int perPartitionCount();

   public abstract DataLimits withoutState();

   public abstract DataLimits withCount(int var1);

   public abstract DataLimits duplicate();

   public abstract float estimateTotalResults(ColumnFamilyStore var1);

   public static Flow<FlowableUnfilteredPartition> countUnfilteredPartitions(Flow<FlowableUnfilteredPartition> partitions, DataLimits.Counter counter) {
      return partitions.map((partition) -> {
         return countUnfilteredPartition(partition, counter);
      });
   }

   public static Flow<Unfiltered> countUnfilteredRows(Flow<Unfiltered> rows, DataLimits.Counter counter) {
      return rows.map((u) -> {
         counter.newUnfiltered(u);
         return u;
      });
   }

   private static FlowableUnfilteredPartition countUnfilteredPartition(FlowableUnfilteredPartition partition, DataLimits.Counter counter) {
      counter.newPartition(partition.partitionKey(), partition.staticRow());
      counter.newStaticRow(partition.staticRow());
      return partition.mapContent((u) -> {
         counter.newUnfiltered(u);
         return u;
      });
   }

   public Flow<FlowableUnfilteredPartition> truncateUnfiltered(Flow<FlowableUnfilteredPartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
      DataLimits.Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      return truncateUnfiltered(partitions, counter);
   }

   public static Flow<FlowableUnfilteredPartition> truncateUnfiltered(final Flow<FlowableUnfilteredPartition> partitions, final DataLimits.Counter counter) {
      class Truncate extends FlowTransform<FlowableUnfilteredPartition, FlowableUnfilteredPartition> {
         Truncate() {
            super(partitions);
         }

         public void requestFirst(FlowSubscriber<FlowableUnfilteredPartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            if(counter.isDone()) {
               this.source = FlowSubscription.DONE;
               subscriber.onComplete();
            } else {
               this.sourceFlow.requestFirst(this, this);
            }

         }

         public void onNext(FlowableUnfilteredPartition item) {
            item = DataLimits.truncateUnfiltered(counter, item);
            if(counter.isDone()) {
               this.subscriber.onFinal(item);
            } else {
               this.subscriber.onNext(item);
            }

         }

         public void onFinal(FlowableUnfilteredPartition item) {
            item = DataLimits.truncateUnfiltered(counter, item);
            this.subscriber.onFinal(item);
         }

         public void requestNext() {
            if(counter.isDone()) {
               this.subscriber.onComplete();
            } else {
               this.source.requestNext();
            }

         }

         public void close() throws Exception {
            counter.endOfIteration();
            super.close();
         }
      }

      return new Truncate();
   }

   public FlowableUnfilteredPartition truncateUnfiltered(FlowableUnfilteredPartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
      DataLimits.Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      return truncateUnfiltered(counter, partition);
   }

   public static FlowableUnfilteredPartition truncateUnfiltered(final DataLimits.Counter counter, final FlowableUnfilteredPartition partition) {
      counter.newPartition(partition.partitionKey(), partition.staticRow());
      class Truncate extends FlowableUnfilteredPartition.FlowTransform {
         Truncate() {
            super(partition.content(), counter.newStaticRow(partition.staticRow()), partition.header());
         }

         public void requestFirst(FlowSubscriber<Unfiltered> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            if(counter.isDoneForPartition()) {
               subscriber.onComplete();
            } else {
               this.sourceFlow.requestFirst(this, this);
            }

         }

         public void onNext(Unfiltered item) {
            try {
               item = counter.newUnfiltered(item);
            } catch (Throwable var3) {
               this.subscriber.onError(var3);
               return;
            }

            if(counter.isDoneForPartition()) {
               if(item != null) {
                  this.subscriber.onFinal(item);
               } else {
                  this.subscriber.onComplete();
               }
            } else {
               assert item != null;

               this.subscriber.onNext(item);
            }

         }

         public void onFinal(Unfiltered item) {
            try {
               item = counter.newUnfiltered(item);
            } catch (Throwable var3) {
               this.subscriber.onError(var3);
               return;
            }

            if(item != null) {
               this.subscriber.onFinal(item);
            } else {
               this.subscriber.onComplete();
            }

         }

         public void close() throws Exception {
            counter.endOfPartition();
            if(this.source != null) {
               this.source.close();
            }

         }
      }

      return new Truncate();
   }

   public Flow<FlowablePartition> truncateFiltered(Flow<FlowablePartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
      DataLimits.Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      return truncateFiltered(partitions, counter);
   }

   public static Flow<FlowablePartition> truncateFiltered(final Flow<FlowablePartition> partitions, final DataLimits.Counter counter) {
      class Truncate extends FlowTransform<FlowablePartition, FlowablePartition> {
         Truncate() {
            super(partitions);
         }

         public void requestFirst(FlowSubscriber<FlowablePartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            if(counter.isDone()) {
               this.source = FlowSubscription.DONE;
               subscriber.onComplete();
            } else {
               this.sourceFlow.requestFirst(this, this);
            }

         }

         public void onNext(FlowablePartition item) {
            item = DataLimits.truncateFiltered(counter, item);
            if(counter.isDone()) {
               this.subscriber.onFinal(item);
            } else {
               this.subscriber.onNext(item);
            }

         }

         public void onFinal(FlowablePartition item) {
            item = DataLimits.truncateFiltered(counter, item);
            this.subscriber.onFinal(item);
         }

         public void requestNext() {
            if(counter.isDone()) {
               this.subscriber.onComplete();
            } else {
               this.source.requestNext();
            }

         }

         public void close() throws Exception {
            counter.endOfIteration();
            super.close();
         }
      }

      return new Truncate();
   }

   public FlowablePartition truncateFiltered(FlowablePartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
      DataLimits.Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      return truncateFiltered(counter, partition);
   }

   public static FlowablePartition truncateFiltered(final DataLimits.Counter counter, final FlowablePartition partition) {
      counter.newPartition(partition.partitionKey(), partition.staticRow());
      class Truncate extends FlowablePartition.FlowTransform {
         Truncate() {
            super(partition.content(), counter.newStaticRow(partition.staticRow()), partition.header());
         }

         public void requestFirst(FlowSubscriber<Row> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            if(counter.isDoneForPartition()) {
               subscriber.onComplete();
            } else {
               this.sourceFlow.requestFirst(this, this);
            }

         }

         public void onNext(Row item) {
            try {
               item = counter.newRow(item);
            } catch (Throwable var3) {
               this.subscriber.onError(var3);
               return;
            }

            if(counter.isDoneForPartition()) {
               if(item != null) {
                  this.subscriber.onFinal(item);
               } else {
                  this.subscriber.onComplete();
               }
            } else {
               assert item != null;

               this.subscriber.onNext(item);
            }

         }

         public void onFinal(Row item) {
            try {
               item = counter.newRow(item);
            } catch (Throwable var3) {
               this.subscriber.onError(var3);
               return;
            }

            if(item != null) {
               this.subscriber.onFinal(item);
            } else {
               this.subscriber.onComplete();
            }

         }

         public void close() throws Exception {
            counter.endOfPartition();
            if(this.source != null) {
               this.source.close();
            }

         }
      }

      return new Truncate();
   }

   public static class Serializer
           extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(DataLimits limits, DataOutputPlus out, ClusteringComparator comparator) throws IOException {
         out.writeByte(limits.kind().ordinal());
         switch (limits.kind()) {
            case CQL_LIMIT:
            case CQL_PAGING_LIMIT: {
               CQLLimits cqlLimits = (CQLLimits)limits;
               out.writeUnsignedVInt(cqlLimits.rowLimit);
               out.writeUnsignedVInt(cqlLimits.perPartitionLimit);
               if (((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0) {
                  out.writeUnsignedVInt(cqlLimits.bytesLimit);
               }
               out.writeBoolean(cqlLimits.isDistinct);
               if (limits.kind() != Kind.CQL_PAGING_LIMIT) break;
               CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
               ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
               out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
               break;
            }
            case CQL_GROUP_BY_LIMIT:
            case CQL_GROUP_BY_PAGING_LIMIT: {
               CQLGroupByLimits groupByLimits = (CQLGroupByLimits)limits;
               out.writeUnsignedVInt(groupByLimits.groupLimit);
               out.writeUnsignedVInt(groupByLimits.groupPerPartitionLimit);
               out.writeUnsignedVInt(groupByLimits.rowLimit);
               if (((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0) {
                  out.writeUnsignedVInt(groupByLimits.bytesLimit);
               }
               AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
               AggregationSpecification.serializers.get((ReadVerbs.ReadVersion)this.version).serialize(groupBySpec, out);
               GroupingState.serializers.get((ReadVerbs.ReadVersion)this.version).serialize(groupByLimits.state, out, comparator);
               if (limits.kind() != Kind.CQL_GROUP_BY_PAGING_LIMIT) break;
               CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits)groupByLimits;
               ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
               out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
            }
         }
      }

      public DataLimits deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         Kind kind = Kind.values()[in.readUnsignedByte()];
         switch (kind) {
            case CQL_LIMIT:
            case CQL_PAGING_LIMIT: {
               int rowLimit = (int)in.readUnsignedVInt();
               int perPartitionLimit = (int)in.readUnsignedVInt();
               int bytesLimit = ((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0 ? (int)in.readUnsignedVInt() : Integer.MAX_VALUE;
               boolean isDistinct = in.readBoolean();
               if (kind == Kind.CQL_LIMIT) {
                  return DataLimits.cqlLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
               }
               ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
               int lastRemaining = (int)in.readUnsignedVInt();
               return new CQLPagingLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
            }
            case CQL_GROUP_BY_LIMIT:
            case CQL_GROUP_BY_PAGING_LIMIT: {
               int groupLimit = (int)in.readUnsignedVInt();
               int groupPerPartitionLimit = (int)in.readUnsignedVInt();
               int rowLimit = (int)in.readUnsignedVInt();
               int bytesLimit = ((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0 ? (int)in.readUnsignedVInt() : Integer.MAX_VALUE;
               AggregationSpecification groupBySpec = AggregationSpecification.serializers.get((ReadVerbs.ReadVersion)this.version).deserialize(in, metadata);
               GroupingState state = GroupingState.serializers.get((ReadVerbs.ReadVersion)this.version).deserialize(in, metadata.comparator);
               if (kind == Kind.CQL_GROUP_BY_LIMIT) {
                  return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, bytesLimit, rowLimit, groupBySpec, state);
               }
               ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
               int lastRemaining = (int)in.readUnsignedVInt();
               return new CQLGroupByPagingLimits(groupLimit, groupPerPartitionLimit, bytesLimit, rowLimit, groupBySpec, state, lastKey, lastRemaining);
            }
         }
         throw new AssertionError();
      }

      public long serializedSize(DataLimits limits, ClusteringComparator comparator) {
         long size = TypeSizes.sizeof((byte)limits.kind().ordinal());
         switch (limits.kind()) {
            case CQL_LIMIT:
            case CQL_PAGING_LIMIT: {
               CQLLimits cqlLimits = (CQLLimits)limits;
               size += (long)TypeSizes.sizeofUnsignedVInt(cqlLimits.rowLimit);
               size += (long)TypeSizes.sizeofUnsignedVInt(cqlLimits.perPartitionLimit);
               if (((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0) {
                  size += (long)TypeSizes.sizeofUnsignedVInt(cqlLimits.bytesLimit);
               }
               size += (long)TypeSizes.sizeof(cqlLimits.isDistinct);
               if (limits.kind() != Kind.CQL_PAGING_LIMIT) break;
               CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
               size += (long)ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
               size += (long)TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
               break;
            }
            case CQL_GROUP_BY_LIMIT:
            case CQL_GROUP_BY_PAGING_LIMIT: {
               CQLGroupByLimits groupByLimits = (CQLGroupByLimits)limits;
               size += (long)TypeSizes.sizeofUnsignedVInt(groupByLimits.groupLimit);
               size += (long)TypeSizes.sizeofUnsignedVInt(groupByLimits.groupPerPartitionLimit);
               size += (long)TypeSizes.sizeofUnsignedVInt(groupByLimits.rowLimit);
               if (((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0) {
                  size += (long)TypeSizes.sizeofUnsignedVInt(groupByLimits.bytesLimit);
               }
               AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
               size += AggregationSpecification.serializers.get((ReadVerbs.ReadVersion)this.version).serializedSize(groupBySpec);
               size += GroupingState.serializers.get((ReadVerbs.ReadVersion)this.version).serializedSize(groupByLimits.state, comparator);
               if (limits.kind() != Kind.CQL_GROUP_BY_PAGING_LIMIT) break;
               CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits)groupByLimits;
               size += (long)ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
               size += (long)TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
               break;
            }
            default: {
               throw new AssertionError();
            }
         }
         return size;
      }
   }


   private static class CQLGroupByPagingLimits extends DataLimits.CQLGroupByLimits {
      private final ByteBuffer lastReturnedKey;
      private final int lastReturnedKeyRemaining;

      public CQLGroupByPagingLimits(int groupLimit, int groupPerPartitionLimit, int bytesLimit, int rowLimit, AggregationSpecification groupBySpec, GroupingState state, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         super(groupLimit, groupPerPartitionLimit, bytesLimit, rowLimit, groupBySpec, state);
         this.lastReturnedKey = lastReturnedKey;
         this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
      }

      public DataLimits.Kind kind() {
         return DataLimits.Kind.CQL_GROUP_BY_PAGING_LIMIT;
      }

      public DataLimits forPaging(PageSize pageSize) {
         throw new UnsupportedOperationException();
      }

      public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         throw new UnsupportedOperationException();
      }

      public DataLimits forGroupByInternalPaging(GroupingState state) {
         throw new UnsupportedOperationException();
      }

      public DataLimits.Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         assert this.state == GroupingState.EMPTY_STATE || this.lastReturnedKey.equals(this.state.partitionKey());

         return new DataLimits.CQLGroupByPagingLimits.PagingGroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      }

      public DataLimits withoutState() {
         return new DataLimits.CQLGroupByLimits(this.groupLimit, this.groupPerPartitionLimit, this.rowLimit, this.groupBySpec);
      }

      public DataLimits withCount(int count) {
         return new DataLimits.CQLGroupByPagingLimits(count, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, this.state, this.lastReturnedKey, this.lastReturnedKeyRemaining);
      }

      public DataLimits duplicate() {
         return new DataLimits.CQLGroupByPagingLimits(this.groupLimit, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, this.state, this.lastReturnedKey, this.lastReturnedKeyRemaining);
      }

      public boolean equals(Object other) {
         if(!(other instanceof DataLimits.CQLGroupByPagingLimits)) {
            return false;
         } else {
            DataLimits.CQLGroupByPagingLimits that = (DataLimits.CQLGroupByPagingLimits)other;
            return this.isSame(that) && Objects.equals(this.lastReturnedKey, that.lastReturnedKey) && this.lastReturnedKeyRemaining == that.lastReturnedKeyRemaining;
         }
      }

      private class PagingGroupByAwareCounter extends DataLimits.CQLGroupByLimits.GroupByAwareCounter {
         private PagingGroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
            super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
         }

         public void newPartition(DecoratedKey partitionKey, Row staticRow) {
            if(DataLimits.logger.isTraceEnabled()) {
               DataLimits.logger.trace("{} - CQLGroupByPagingLimits.applyToPartition {}", Integer.valueOf(this.hashCode()), ByteBufferUtil.bytesToHex(partitionKey.getKey()));
            }

            if(partitionKey.getKey().equals(CQLGroupByPagingLimits.this.lastReturnedKey)) {
               this.currentPartitionKey = partitionKey;
               this.groupCountedInCurrentPartition = 0;
               this.previouslyCountedInCurrentPartition = CQLGroupByPagingLimits.this.groupPerPartitionLimit - CQLGroupByPagingLimits.this.lastReturnedKeyRemaining;
               this.hasReturnedRowsFromCurrentPartition = true;
               this.staticRowBytes = -1;
               this.hasGroupStarted = CQLGroupByPagingLimits.this.state.hasClustering();
            } else {
               super.newPartition(partitionKey, staticRow);
            }

         }
      }
   }

   private static class CQLGroupByLimits extends DataLimits.CQLLimits {
      protected final GroupingState state;
      protected final AggregationSpecification groupBySpec;
      protected final int groupLimit;
      protected final int groupPerPartitionLimit;

      public CQLGroupByLimits(int groupLimit, int groupPerPartitionLimit, int rowLimit, AggregationSpecification groupBySpec) {
         this(groupLimit, groupPerPartitionLimit, 2147483647, rowLimit, groupBySpec, GroupingState.EMPTY_STATE);
      }

      private CQLGroupByLimits(int groupLimit, int groupPerPartitionLimit, int bytesLimit, int rowLimit, AggregationSpecification groupBySpec, GroupingState state) {
         super(bytesLimit, rowLimit, 2147483647, false);
         this.groupLimit = groupLimit;
         this.groupPerPartitionLimit = groupPerPartitionLimit;
         this.groupBySpec = groupBySpec;
         this.state = state;
      }

      public DataLimits.Kind kind() {
         return DataLimits.Kind.CQL_GROUP_BY_LIMIT;
      }

      public boolean isGroupByLimit() {
         return true;
      }

      public boolean isUnlimited() {
         return this.groupLimit == 2147483647 && this.groupPerPartitionLimit == 2147483647 && super.isUnlimited();
      }

      public DataLimits forShortReadRetry(int toFetch) {
         return new DataLimits.CQLLimits(toFetch);
      }

      public float estimateTotalResults(ColumnFamilyStore cfs) {
         return super.estimateTotalResults(cfs);
      }

      public DataLimits forPaging(PageSize pageSize) {
         if(DataLimits.logger.isTraceEnabled()) {
            DataLimits.logger.trace("{} forPaging({})", Integer.valueOf(this.hashCode()), pageSize);
         }

         return pageSize.isInBytes()?new DataLimits.CQLGroupByLimits(this.groupLimit, this.groupPerPartitionLimit, pageSize.rawSize(), this.rowLimit, this.groupBySpec, this.state):new DataLimits.CQLGroupByLimits(Math.min(this.groupLimit, pageSize.rawSize()), this.groupPerPartitionLimit, 2147483647, this.rowLimit, this.groupBySpec, this.state);
      }

      public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         if(DataLimits.logger.isTraceEnabled()) {
            DataLimits.logger.trace("{} forPaging({}, {}, {}) vs state {}/{}", new Object[]{Integer.valueOf(this.hashCode()), pageSize, lastReturnedKey == null?"null":ByteBufferUtil.bytesToHex(lastReturnedKey), Integer.valueOf(lastReturnedKeyRemaining), this.state.partitionKey() == null?"null":ByteBufferUtil.bytesToHex(this.state.partitionKey()), this.state.clustering() == null?"null":this.state.clustering().toBinaryString()});
         }

         return pageSize.isInBytes()?new DataLimits.CQLGroupByPagingLimits(this.groupLimit, this.groupPerPartitionLimit, pageSize.rawSize(), this.rowLimit, this.groupBySpec, this.state, lastReturnedKey, lastReturnedKeyRemaining):new DataLimits.CQLGroupByPagingLimits(Math.min(this.groupLimit, pageSize.rawSize()), this.groupPerPartitionLimit, 2147483647, this.rowLimit, this.groupBySpec, this.state, lastReturnedKey, lastReturnedKeyRemaining);
      }

      public DataLimits forGroupByInternalPaging(GroupingState state) {
         return new DataLimits.CQLGroupByLimits(this.rowLimit, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, state);
      }

      public DataLimits.Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         return new DataLimits.CQLGroupByLimits.GroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      }

      public int count() {
         return this.groupLimit;
      }

      public int perPartitionCount() {
         return this.groupPerPartitionLimit;
      }

      public DataLimits withoutState() {
         return this.state == GroupingState.EMPTY_STATE?this:new DataLimits.CQLGroupByLimits(this.groupLimit, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, GroupingState.EMPTY_STATE);
      }

      public DataLimits withCount(int count) {
         return new DataLimits.CQLGroupByLimits(count, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, this.state);
      }

      public DataLimits duplicate() {
         return new DataLimits.CQLGroupByLimits(this.groupLimit, this.groupPerPartitionLimit, this.bytesLimit, this.rowLimit, this.groupBySpec, this.state);
      }

      protected boolean isSame(DataLimits.CQLLimits other) {
         if(!(other instanceof DataLimits.CQLGroupByLimits)) {
            return false;
         } else {
            DataLimits.CQLGroupByLimits that = (DataLimits.CQLGroupByLimits)other;
            return super.isSame(that) && this.state.equals(that.state) && this.groupBySpec.equals(that.groupBySpec) && this.groupLimit == that.groupLimit && this.groupPerPartitionLimit == that.groupPerPartitionLimit;
         }
      }

      public boolean equals(Object other) {
         return other != null && this.getClass().equals(other.getClass())?this.isSame((DataLimits.CQLGroupByLimits)other):false;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         if(this.groupLimit != 2147483647) {
            sb.append("GROUP LIMIT ").append(this.groupLimit);
            if(this.groupPerPartitionLimit != 2147483647 || this.rowLimit != 2147483647) {
               sb.append(' ');
            }
         }

         if(this.groupPerPartitionLimit != 2147483647) {
            sb.append("GROUP PER PARTITION LIMIT ").append(this.groupPerPartitionLimit);
            if(this.bytesLimit != 2147483647) {
               sb.append(' ');
            }
         }

         if(this.bytesLimit != 2147483647) {
            sb.append("BYTES LIMIT ").append(this.bytesLimit);
            if(this.rowLimit != 2147483647) {
               sb.append(' ');
            }
         }

         if(this.rowLimit != 2147483647) {
            sb.append("LIMIT ").append(this.rowLimit);
         }

         return sb.toString();
      }

      public boolean isExhausted(DataLimits.Counter counter) {
         return counter.bytesCounted() < this.bytesLimit && counter.rowCounted() < this.rowLimit && counter.counted() < this.groupLimit;
      }

      protected class GroupByAwareCounter extends DataLimits.Counter {
         private final GroupMaker groupMaker;
         protected final boolean countPartitionsWithOnlyStaticData;
         protected DecoratedKey currentPartitionKey;
         protected int bytesCounted;
         protected int rowCounted;
         protected int rowCountedInCurrentPartition;
         protected int groupCounted;
         protected int groupCountedInCurrentPartition;
         protected int previouslyCountedInCurrentPartition;
         protected int staticRowBytes;
         protected boolean hasGroupStarted;
         protected boolean hasReturnedRowsFromCurrentPartition;

         private GroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
            super(nowInSec, assumeLiveData, enforceStrictLiveness);
            this.groupMaker = CQLGroupByLimits.this.groupBySpec.newGroupMaker(CQLGroupByLimits.this.state);
            this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;
            this.hasGroupStarted = CQLGroupByLimits.this.state.hasClustering();
         }

         public void newPartition(DecoratedKey partitionKey, Row staticRow) {
            if(DataLimits.logger.isTraceEnabled()) {
               DataLimits.logger.trace("{} - GroupByAwareCounter.newPartition {} with state {}", new Object[]{Integer.valueOf(this.hashCode()), ByteBufferUtil.bytesToHex(partitionKey.getKey()), CQLGroupByLimits.this.state.partitionKey() != null?ByteBufferUtil.bytesToHex(CQLGroupByLimits.this.state.partitionKey()):"null"});
            }

            if(partitionKey.getKey().equals(CQLGroupByLimits.this.state.partitionKey())) {
               this.staticRowBytes = -1;
               this.hasReturnedRowsFromCurrentPartition = true;
               this.hasGroupStarted = true;
            } else {
               if(this.hasGroupStarted && this.groupMaker.isNewGroup(partitionKey, Clustering.STATIC_CLUSTERING)) {
                  this.incrementGroupCount();
                  if(this.isDone()) {
                     this.incrementGroupInCurrentPartitionCount();
                  }

                  this.hasGroupStarted = false;
               }

               this.hasReturnedRowsFromCurrentPartition = false;
               this.staticRowBytes = !staticRow.isEmpty() && this.isLive(staticRow)?staticRow.dataSize():-1;
            }

            this.currentPartitionKey = partitionKey;
            if(!this.isDone()) {
               this.previouslyCountedInCurrentPartition = 0;
               this.groupCountedInCurrentPartition = 0;
               this.rowCountedInCurrentPartition = 0;
            }

         }

         public Row newStaticRow(Row row) {
            if(DataLimits.logger.isTraceEnabled()) {
               DataLimits.logger.trace("{} - GroupByAwareCounter.applyToStatic {}/{}", new Object[]{Integer.valueOf(this.hashCode()), ByteBufferUtil.bytesToHex(this.currentPartitionKey.getKey()), row == null?"null":row.clustering().toBinaryString()});
            }

            if(this.isDone()) {
               this.staticRowBytes = -1;
               return Rows.EMPTY_STATIC_ROW;
            } else {
               return row;
            }
         }

         public Row newRow(Row row) {
            if(DataLimits.logger.isTraceEnabled()) {
               DataLimits.logger.trace("{} - GroupByAwareCounter.applyToRow {}/{}", new Object[]{Integer.valueOf(this.hashCode()), ByteBufferUtil.bytesToHex(this.currentPartitionKey.getKey()), row.clustering().toBinaryString()});
            }

            if(this.groupMaker.isNewGroup(this.currentPartitionKey, row.clustering())) {
               if(this.hasGroupStarted) {
                  this.incrementGroupCount();
                  this.incrementGroupInCurrentPartitionCount();
               }

               this.hasGroupStarted = false;
            }

            if(this.isDoneForPartition()) {
               this.hasGroupStarted = false;
               return null;
            } else {
               if(this.isLive(row)) {
                  this.hasGroupStarted = true;
                  this.incrementRowCount(row.dataSize());
                  this.hasReturnedRowsFromCurrentPartition = true;
               }

               return row;
            }
         }

         public int counted() {
            return this.groupCounted;
         }

         public int countedInCurrentPartition() {
            return this.groupCountedInCurrentPartition;
         }

         public int bytesCounted() {
            return this.bytesCounted;
         }

         public int rowCounted() {
            return this.rowCounted;
         }

         public int rowCountedInCurrentPartition() {
            return this.rowCountedInCurrentPartition;
         }

         protected void incrementRowCount(int rowSize) {
            this.bytesCounted += rowSize;
            ++this.rowCountedInCurrentPartition;
            ++this.rowCounted;
         }

         private void incrementGroupCount() {
            ++this.groupCounted;
         }

         private void incrementGroupInCurrentPartitionCount() {
            ++this.groupCountedInCurrentPartition;
         }

         public boolean isDoneForPartition() {
            return this.isDone() || this.previouslyCountedInCurrentPartition + this.groupCountedInCurrentPartition >= CQLGroupByLimits.this.groupPerPartitionLimit;
         }

         public boolean isEmpty() {
            return this.groupCounted == 0;
         }

         public boolean isDone() {
            return this.groupCounted >= CQLGroupByLimits.this.groupLimit;
         }

         public void endOfPartition() {
            if(this.countPartitionsWithOnlyStaticData && this.staticRowBytes > 0 && !this.hasReturnedRowsFromCurrentPartition) {
               this.incrementRowCount(this.staticRowBytes);
               this.incrementGroupCount();
               this.incrementGroupInCurrentPartitionCount();
               this.hasGroupStarted = false;
            }

         }

         public void endOfIteration() {
            if(this.hasGroupStarted && this.groupCounted < CQLGroupByLimits.this.groupLimit && this.bytesCounted < CQLGroupByLimits.this.bytesLimit && this.rowCounted < CQLGroupByLimits.this.rowLimit) {
               this.incrementGroupCount();
               this.incrementGroupInCurrentPartitionCount();
            }

         }

         public String toString() {
            return String.format("[counted(bytes,groups,perPartition): (%d,%d,%d), count(bytes,groups,perPartition): (%d,%d,%d)", new Object[]{Integer.valueOf(this.bytesCounted()), Integer.valueOf(this.groupCounted), Integer.valueOf(this.groupCountedInCurrentPartition), Integer.valueOf(CQLGroupByLimits.this.bytes()), Integer.valueOf(CQLGroupByLimits.this.count()), Integer.valueOf(CQLGroupByLimits.this.perPartitionCount())});
         }
      }
   }

   private static class CQLPagingLimits extends DataLimits.CQLLimits {
      private final ByteBuffer lastReturnedKey;
      private final int lastReturnedKeyRemaining;

      public CQLPagingLimits(int bytesLimit, int rowLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         super(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
         this.lastReturnedKey = lastReturnedKey;
         this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
      }

      public DataLimits.Kind kind() {
         return DataLimits.Kind.CQL_PAGING_LIMIT;
      }

      public DataLimits forPaging(PageSize pageSize) {
         throw new UnsupportedOperationException();
      }

      public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         throw new UnsupportedOperationException();
      }

      public DataLimits withoutState() {
         return new DataLimits.CQLLimits(this.bytesLimit, this.rowLimit, this.perPartitionLimit, this.isDistinct);
      }

      public DataLimits withCount(int count) {
         return new DataLimits.CQLPagingLimits(this.bytesLimit, count, this.perPartitionLimit, this.isDistinct, this.lastReturnedKey, this.lastReturnedKeyRemaining);
      }

      public DataLimits duplicate() {
         return new DataLimits.CQLPagingLimits(this.bytesLimit, this.rowLimit, this.perPartitionLimit, this.isDistinct, this.lastReturnedKey, this.lastReturnedKeyRemaining);
      }

      public DataLimits.Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         return new DataLimits.CQLPagingLimits.PagingAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      }

      public boolean equals(Object other) {
         if(!(other instanceof DataLimits.CQLPagingLimits)) {
            return false;
         } else {
            DataLimits.CQLPagingLimits that = (DataLimits.CQLPagingLimits)other;
            return this.isSame(that) && Objects.equals(this.lastReturnedKey, that.lastReturnedKey) && this.lastReturnedKeyRemaining == that.lastReturnedKeyRemaining;
         }
      }

      private class PagingAwareCounter extends DataLimits.CQLLimits.CQLCounter {
         private PagingAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
            super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
         }

         public void newPartition(DecoratedKey partitionKey, Row staticRow) {
            if(partitionKey.getKey().equals(CQLPagingLimits.this.lastReturnedKey)) {
               this.rowCountedInCurrentPartition = 0;
               this.previouslyCountedInCurrentPartition = CQLPagingLimits.this.perPartitionLimit - CQLPagingLimits.this.lastReturnedKeyRemaining;
               this.staticRowBytes = -1;
            } else {
               super.newPartition(partitionKey, staticRow);
            }

         }
      }
   }

   private static class CQLLimits extends DataLimits {
      protected final int bytesLimit;
      protected final int rowLimit;
      protected final int perPartitionLimit;
      protected final boolean isDistinct;

      private CQLLimits(int rowLimit) {
         this(rowLimit, 2147483647);
      }

      private CQLLimits(int rowLimit, int perPartitionLimit) {
         this(2147483647, rowLimit, perPartitionLimit, false);
      }

      private CQLLimits(int rowLimit, int perPartitionLimit, boolean isDistinct) {
         this(2147483647, rowLimit, perPartitionLimit, isDistinct);
      }

      private CQLLimits(int bytesLimit, int rowLimit, int perPartitionLimit, boolean isDistinct) {
         this.bytesLimit = bytesLimit;
         this.rowLimit = rowLimit;
         this.perPartitionLimit = perPartitionLimit;
         this.isDistinct = isDistinct;
      }

      private static DataLimits.CQLLimits distinct(int rowLimit) {
         return new DataLimits.CQLLimits(rowLimit, 1, true);
      }

      public DataLimits.Kind kind() {
         return DataLimits.Kind.CQL_LIMIT;
      }

      public boolean isUnlimited() {
         return this.bytesLimit == 2147483647 && this.rowLimit == 2147483647 && this.perPartitionLimit == 2147483647;
      }

      public boolean isDistinct() {
         return this.isDistinct;
      }

      public DataLimits forPaging(PageSize pageSize) {
         return pageSize.isInBytes()?new DataLimits.CQLLimits(pageSize.rawSize(), this.rowLimit, this.perPartitionLimit, this.isDistinct):new DataLimits.CQLLimits(2147483647, Math.min(this.rowLimit, pageSize.rawSize()), this.perPartitionLimit, this.isDistinct);
      }

      public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining) {
         return pageSize.isInBytes()?new DataLimits.CQLPagingLimits(pageSize.rawSize(), this.rowLimit, this.perPartitionLimit, this.isDistinct, lastReturnedKey, lastReturnedKeyRemaining):new DataLimits.CQLPagingLimits(2147483647, Math.min(this.rowLimit, pageSize.rawSize()), this.perPartitionLimit, this.isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
      }

      public DataLimits forShortReadRetry(int toFetch) {
         return new DataLimits.CQLLimits(this.bytesLimit, toFetch, this.perPartitionLimit, this.isDistinct);
      }

      public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         if(cached.rowsWithNonExpiringCells() >= this.rowLimit) {
            return true;
         } else if(cached.rowCount() < this.rowLimit) {
            return false;
         } else {
            FlowableUnfilteredPartition cachePart = cached.unfilteredPartition(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
            DataLimits.Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            FlowableUnfilteredPartition partition = DataLimits.truncateUnfiltered(counter, cachePart);
            partition.content().process().blockingSingle();
            return counter.isDone();
         }
      }

      public DataLimits.Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
         return new DataLimits.CQLLimits.CQLCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
      }

      public int bytes() {
         return this.bytesLimit;
      }

      public int count() {
         return this.rowLimit;
      }

      public int perPartitionCount() {
         return this.perPartitionLimit;
      }

      public DataLimits withoutState() {
         return this;
      }

      public DataLimits withCount(int count) {
         return new DataLimits.CQLLimits(this.bytesLimit, count, this.perPartitionLimit, this.isDistinct);
      }

      public DataLimits duplicate() {
         return new DataLimits.CQLLimits(this.bytesLimit, this.rowLimit, this.perPartitionLimit, this.isDistinct);
      }

      public float estimateTotalResults(ColumnFamilyStore cfs) {
         float rowsPerPartition = (float)cfs.getMeanCells() / (float)cfs.metadata().regularColumns().size();
         return rowsPerPartition * (float)cfs.estimateKeys();
      }

      protected boolean isSame(DataLimits.CQLLimits that) {
         return this.bytesLimit == that.bytesLimit && this.rowLimit == that.rowLimit && this.perPartitionLimit == that.perPartitionLimit && this.isDistinct == that.isDistinct;
      }

      public boolean equals(Object other) {
         return other != null && this.getClass().equals(other.getClass())?this.isSame((DataLimits.CQLLimits)other):false;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         if(this.bytesLimit != 2147483647) {
            sb.append("BYTES ").append(this.bytesLimit);
            if(this.rowLimit != 2147483647) {
               sb.append(' ');
            }
         }

         if(this.rowLimit != 2147483647) {
            sb.append("LIMIT ").append(this.rowLimit);
            if(this.perPartitionLimit != 2147483647) {
               sb.append(' ');
            }
         }

         if(this.perPartitionLimit != 2147483647) {
            sb.append("PER PARTITION LIMIT ").append(this.perPartitionLimit);
         }

         return sb.toString();
      }

      protected class CQLCounter extends DataLimits.Counter {
         protected volatile int bytesCounted;
         protected volatile int rowCounted;
         protected volatile int rowCountedInCurrentPartition;
         protected volatile int previouslyCountedInCurrentPartition;
         protected final boolean countPartitionsWithOnlyStaticData;
         protected volatile int staticRowBytes;

         public CQLCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness) {
            super(nowInSec, assumeLiveData, enforceStrictLiveness);
            this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;
         }

         public void newPartition(DecoratedKey partitionKey, Row staticRow) {
            this.rowCountedInCurrentPartition = 0;
            this.previouslyCountedInCurrentPartition = 0;
            this.staticRowBytes = !staticRow.isEmpty() && this.isLive(staticRow)?staticRow.dataSize():-1;
         }

         public Row newRow(Row row) {
            if(this.isLive(row)) {
               this.incrementRowCount(row.dataSize());
            }

            return row;
         }

         public Row newStaticRow(Row row) {
            return row;
         }

         public void endOfPartition() {
            if(this.countPartitionsWithOnlyStaticData && this.staticRowBytes > 0 && this.previouslyCountedInCurrentPartition + this.rowCountedInCurrentPartition == 0) {
               this.incrementRowCount(this.staticRowBytes);
            }

         }

         public void endOfIteration() {
            if(DataLimits.logger.isTraceEnabled()) {
               DataLimits.logger.trace("{} - counter done: {}", Integer.valueOf(this.hashCode()), this.toString());
            }

         }

         protected void incrementRowCount(int rowSize) {
            this.bytesCounted += rowSize;
            ++this.rowCounted;
            ++this.rowCountedInCurrentPartition;
         }

         public int counted() {
            return this.rowCounted;
         }

         public int countedInCurrentPartition() {
            return this.rowCountedInCurrentPartition;
         }

         public int bytesCounted() {
            return this.bytesCounted;
         }

         public int rowCounted() {
            return this.rowCounted;
         }

         public int rowCountedInCurrentPartition() {
            return this.rowCountedInCurrentPartition;
         }

         public boolean isEmpty() {
            return this.bytesCounted == 0 && this.rowCounted == 0;
         }

         public boolean isDone() {
            return this.bytesCounted >= CQLLimits.this.bytesLimit || this.rowCounted >= CQLLimits.this.rowLimit;
         }

         public boolean isDoneForPartition() {
            return this.isDone() || this.previouslyCountedInCurrentPartition + this.rowCountedInCurrentPartition >= CQLLimits.this.perPartitionLimit;
         }

         public String toString() {
            return String.format("[counted(bytes,rows,perPartition): (%d,%d,%d), count(bytes,rows,perPartition): (%d,%d,%d)", new Object[]{Integer.valueOf(this.bytesCounted()), Integer.valueOf(this.rowCounted()), Integer.valueOf(this.rowCountedInCurrentPartition()), Integer.valueOf(CQLLimits.this.bytes()), Integer.valueOf(CQLLimits.this.count()), Integer.valueOf(CQLLimits.this.perPartitionCount())});
         }
      }
   }

   public abstract class Counter {
      protected final int nowInSec;
      protected final boolean assumeLiveData;
      private final boolean enforceStrictLiveness;

      protected Counter(int nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness) {
         this.nowInSec = nowInSec;
         this.assumeLiveData = assumeLiveData;
         this.enforceStrictLiveness = enforceStrictLiveness;
      }

      abstract void newPartition(DecoratedKey var1, Row var2);

      abstract Row newRow(Row var1);

      abstract Row newStaticRow(Row var1);

      public abstract void endOfPartition();

      public abstract void endOfIteration();

      Unfiltered newUnfiltered(Unfiltered unfiltered) {
         return (Unfiltered)(unfiltered instanceof Row?this.newRow((Row)unfiltered):unfiltered);
      }

      public abstract int counted();

      public abstract int countedInCurrentPartition();

      public abstract int bytesCounted();

      public abstract int rowCounted();

      public abstract int rowCountedInCurrentPartition();

      public abstract boolean isEmpty();

      public abstract boolean isDone();

      public abstract boolean isDoneForPartition();

      protected boolean isLive(Row row) {
         return this.assumeLiveData || row.hasLiveData(this.nowInSec, this.enforceStrictLiveness);
      }
   }

   public static enum Kind {
      CQL_LIMIT,
      CQL_PAGING_LIMIT,
      CQL_GROUP_BY_LIMIT,
      CQL_GROUP_BY_PAGING_LIMIT;

      private Kind() {
      }
   }
}
