package org.apache.cassandra.db;

import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractReadCommandBuilder<T extends ReadCommand> {
   protected final ColumnFamilyStore cfs;
   protected int nowInSeconds;
   private int rowLimit = -1;
   private int partitionLimit = -1;
   private PageSize pagingLimit;
   protected boolean reversed = false;
   protected Set<ColumnIdentifier> columns;
   protected final RowFilter filter = RowFilter.create();
   private ClusteringBound lowerClusteringBound;
   private ClusteringBound upperClusteringBound;
   private NavigableSet<Clustering> clusterings;
   protected boolean isInternal;

   AbstractReadCommandBuilder(ColumnFamilyStore cfs) {
      this.cfs = cfs;
      this.nowInSeconds = FBUtilities.nowInSeconds();
   }

   public AbstractReadCommandBuilder<T> internal() {
      this.isInternal = true;
      return this;
   }

   public AbstractReadCommandBuilder<T> withNowInSeconds(int nowInSec) {
      this.nowInSeconds = nowInSec;
      return this;
   }

   public AbstractReadCommandBuilder<T> fromIncl(Object... values) {
      assert this.lowerClusteringBound == null && this.clusterings == null;

      this.lowerClusteringBound = ClusteringBound.create(this.cfs.metadata().comparator, true, true, values);
      return this;
   }

   public AbstractReadCommandBuilder<T> fromExcl(Object... values) {
      assert this.lowerClusteringBound == null && this.clusterings == null;

      this.lowerClusteringBound = ClusteringBound.create(this.cfs.metadata().comparator, true, false, values);
      return this;
   }

   public AbstractReadCommandBuilder<T> toIncl(Object... values) {
      assert this.upperClusteringBound == null && this.clusterings == null;

      this.upperClusteringBound = ClusteringBound.create(this.cfs.metadata().comparator, false, true, values);
      return this;
   }

   public AbstractReadCommandBuilder<T> toExcl(Object... values) {
      assert this.upperClusteringBound == null && this.clusterings == null;

      this.upperClusteringBound = ClusteringBound.create(this.cfs.metadata().comparator, false, false, values);
      return this;
   }

   public AbstractReadCommandBuilder<T> includeRow(Object... values) {
      assert this.lowerClusteringBound == null && this.upperClusteringBound == null;

      if(this.clusterings == null) {
         this.clusterings = new TreeSet(this.cfs.metadata().comparator);
      }

      this.clusterings.add(this.cfs.metadata().comparator.make(values));
      return this;
   }

   public AbstractReadCommandBuilder<T> clusterings(NavigableSet<Clustering> clusterings) {
      this.clusterings = clusterings;
      return this;
   }

   public AbstractReadCommandBuilder<T> reverse() {
      this.reversed = true;
      return this;
   }

   public AbstractReadCommandBuilder<T> withLimit(int newLimit) {
      this.rowLimit = newLimit;
      return this;
   }

   public AbstractReadCommandBuilder<T> withPartitionLimit(int newLimit) {
      this.partitionLimit = newLimit;
      return this;
   }

   public AbstractReadCommandBuilder<T> withPagingLimit(PageSize newLimit) {
      this.pagingLimit = newLimit;
      return this;
   }

   public AbstractReadCommandBuilder<T> columns(String... columns) {
      if(this.columns == null) {
         this.columns = Sets.newHashSetWithExpectedSize(columns.length);
      }

      String[] var2 = columns;
      int var3 = columns.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String column = var2[var4];
         this.columns.add(ColumnIdentifier.getInterned(column, true));
      }

      return this;
   }

   private ByteBuffer bb(Object value, AbstractType<?> type) {
      return value instanceof ByteBuffer?(ByteBuffer)value:type.decompose(value);
   }

   private AbstractType<?> forValues(AbstractType<?> collectionType) {
      assert collectionType instanceof CollectionType;

      CollectionType ct = (CollectionType)collectionType;
      switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[ct.kind.ordinal()]) {
      case 1:
      case 2:
         return ct.valueComparator();
      case 3:
         return ct.nameComparator();
      default:
         throw new AssertionError();
      }
   }

   private AbstractType<?> forKeys(AbstractType<?> collectionType) {
      assert collectionType instanceof CollectionType;

      CollectionType ct = (CollectionType)collectionType;
      switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[ct.kind.ordinal()]) {
      case 1:
      case 2:
         return ct.nameComparator();
      default:
         throw new AssertionError();
      }
   }

   public AbstractReadCommandBuilder<T> filterOn(String column, Operator op, Object value) {
      ColumnMetadata def = this.cfs.metadata().getColumn(ColumnIdentifier.getInterned(column, true));

      assert def != null;

      AbstractType<?> type = def.type;
      if(op == Operator.CONTAINS) {
         type = this.forValues(type);
      } else if(op == Operator.CONTAINS_KEY) {
         type = this.forKeys(type);
      }

      this.filter.add(def, op, this.bb(value, type));
      return this;
   }

   protected ColumnFilter makeColumnFilter() {
      if(this.columns != null && !this.columns.isEmpty()) {
         ColumnFilter.Builder filter = ColumnFilter.selectionBuilder();
         Iterator var2 = this.columns.iterator();

         while(var2.hasNext()) {
            ColumnIdentifier column = (ColumnIdentifier)var2.next();
            filter.add(this.cfs.metadata().getColumn(column));
         }

         return filter.build();
      } else {
         return ColumnFilter.all(this.cfs.metadata());
      }
   }

   protected ClusteringIndexFilter makeFilter() {
      if(this.clusterings != null) {
         return new ClusteringIndexNamesFilter(this.clusterings, this.reversed);
      } else {
         Slice slice = Slice.make(this.lowerClusteringBound == null?ClusteringBound.BOTTOM:this.lowerClusteringBound, this.upperClusteringBound == null?ClusteringBound.TOP:this.upperClusteringBound);
         return new ClusteringIndexSliceFilter(Slices.with(this.cfs.metadata().comparator, slice), this.reversed);
      }
   }

   protected DataLimits makeLimits() {
      DataLimits limits = DataLimits.NONE;
      if(this.rowLimit >= 0 && this.partitionLimit >= 0) {
         limits = DataLimits.cqlLimits(this.rowLimit, this.partitionLimit);
      } else if(this.rowLimit >= 0) {
         limits = DataLimits.cqlLimits(this.rowLimit);
      } else if(this.partitionLimit >= 0) {
         limits = DataLimits.cqlLimits(2147483647, this.partitionLimit);
      }

      if(this.pagingLimit != null) {
         limits = limits.forPaging(this.pagingLimit);
      }

      return limits;
   }

   public abstract T build();

   public static class PartitionRangeBuilder extends AbstractReadCommandBuilder<PartitionRangeReadCommand> {
      private DecoratedKey startKey;
      private boolean startInclusive;
      private DecoratedKey endKey;
      private boolean endInclusive;

      public PartitionRangeBuilder(ColumnFamilyStore cfs) {
         super(cfs);
      }

      public AbstractReadCommandBuilder.PartitionRangeBuilder fromKeyIncl(Object... values) {
         assert this.startKey == null;

         this.startInclusive = true;
         this.startKey = makeKey(this.cfs.metadata(), values);
         return this;
      }

      public AbstractReadCommandBuilder.PartitionRangeBuilder fromKeyExcl(Object... values) {
         assert this.startKey == null;

         this.startInclusive = false;
         this.startKey = makeKey(this.cfs.metadata(), values);
         return this;
      }

      public AbstractReadCommandBuilder.PartitionRangeBuilder toKeyIncl(Object... values) {
         assert this.endKey == null;

         this.endInclusive = true;
         this.endKey = makeKey(this.cfs.metadata(), values);
         return this;
      }

      public AbstractReadCommandBuilder.PartitionRangeBuilder toKeyExcl(Object... values) {
         assert this.endKey == null;

         this.endInclusive = false;
         this.endKey = makeKey(this.cfs.metadata(), values);
         return this;
      }

      public PartitionRangeReadCommand build() {
         PartitionPosition start = this.startKey;
         if(start == null) {
            start = this.cfs.getPartitioner().getMinimumToken().maxKeyBound();
            this.startInclusive = false;
         }

         PartitionPosition end = this.endKey;
         if(end == null) {
            end = this.cfs.getPartitioner().getMinimumToken().maxKeyBound();
            this.endInclusive = true;
         }

         Object bounds;
         if(((PartitionPosition)start).compareTo(end) > 0) {
            bounds = start;
            start = end;
            end = bounds;
         }

         if(this.startInclusive && this.endInclusive) {
            bounds = new Bounds((RingPosition)start, (RingPosition)end);
         } else if(this.startInclusive && !this.endInclusive) {
            bounds = new IncludingExcludingBounds((RingPosition)start, (RingPosition)end);
         } else if(!this.startInclusive && this.endInclusive) {
            bounds = new Range((RingPosition)start, (RingPosition)end);
         } else {
            bounds = new ExcludingBounds((RingPosition)start, (RingPosition)end);
         }

         return this.isInternal?PartitionRangeReadCommand.create(this.cfs.metadata(), this.nowInSeconds, this.makeColumnFilter(), this.filter, this.makeLimits(), new DataRange((AbstractBounds)bounds, this.makeFilter()), TPCTaskType.READ_RANGE_INTERNAL):PartitionRangeReadCommand.create(this.cfs.metadata(), this.nowInSeconds, this.makeColumnFilter(), this.filter, this.makeLimits(), new DataRange((AbstractBounds)bounds, this.makeFilter()));
      }

      static DecoratedKey makeKey(TableMetadata metadata, Object... partitionKey) {
         if(partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey) {
            return (DecoratedKey)partitionKey[0];
         } else {
            ByteBuffer key = metadata.partitionKeyAsClusteringComparator().make(partitionKey).serializeAsPartitionKey();
            return metadata.partitioner.decorateKey(key);
         }
      }
   }

   public static class SinglePartitionBuilder extends AbstractReadCommandBuilder<SinglePartitionReadCommand> {
      private final DecoratedKey partitionKey;

      public SinglePartitionBuilder(ColumnFamilyStore cfs, DecoratedKey key) {
         super(cfs);
         this.partitionKey = key;
      }

      public SinglePartitionReadCommand build() {
         return this.isInternal?SinglePartitionReadCommand.create(this.cfs.metadata(), this.nowInSeconds, this.makeColumnFilter(), this.filter, this.makeLimits(), this.partitionKey, this.makeFilter(), TPCTaskType.READ_INTERNAL):SinglePartitionReadCommand.create(this.cfs.metadata(), this.nowInSeconds, this.makeColumnFilter(), this.filter, this.makeLimits(), this.partitionKey, this.makeFilter());
      }
   }
}
