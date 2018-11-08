package org.apache.cassandra.db.partitions;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredPartitionSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionUpdate extends ArrayBackedPartition {
   protected static final Logger logger = LoggerFactory.getLogger(PartitionUpdate.class);
   private static final Row[] EMPTY_ROWS = new Row[0];
   public static final Versioned<EncodingVersion, PartitionUpdate.PartitionUpdateSerializer> serializers = EncodingVersion.versioned((x$0) -> {
      return new PartitionUpdate.PartitionUpdateSerializer(x$0);
   });
   private final int createdAtInSec;
   private volatile boolean isBuilt;
   private boolean canReOpen;
   private List<Row> rowBuilder;
   private boolean needsSort;
   private final boolean canHaveShadowedData;

   private PartitionUpdate(TableMetadata metadata, DecoratedKey key, RegularAndStaticColumns columns, MutableDeletionInfo deletionInfo, int initialRowCapacity, boolean canHaveShadowedData) {
      super(key, new ArrayBackedPartition.Holder(columns, EMPTY_ROWS, 0, DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS), metadata);
      this.createdAtInSec = FBUtilities.nowInSeconds();
      this.canReOpen = true;
      this.canHaveShadowedData = canHaveShadowedData;
      this.holder.deletionInfo = deletionInfo;
      this.rowBuilder = new ArrayList(initialRowCapacity);
      this.needsSort = false;
   }

   private PartitionUpdate(TableMetadata metadata, DecoratedKey key, ArrayBackedPartition.Holder holder, MutableDeletionInfo deletionInfo, boolean canHaveShadowedData) {
      super(key, holder, metadata);
      this.createdAtInSec = FBUtilities.nowInSeconds();
      this.canReOpen = true;
      this.isBuilt = true;
      this.holder.deletionInfo = deletionInfo;
      this.canHaveShadowedData = canHaveShadowedData;
   }

   public PartitionUpdate(TableMetadata metadata, DecoratedKey key, RegularAndStaticColumns columns, int initialRowCapacity) {
      this(metadata, key, columns, MutableDeletionInfo.live(), initialRowCapacity, true);
   }

   public PartitionUpdate(TableMetadata metadata, ByteBuffer key, RegularAndStaticColumns columns, int initialRowCapacity) {
      this(metadata, metadata.partitioner.decorateKey(key), columns, initialRowCapacity);
   }

   public static PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey key) {
      MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
      ArrayBackedPartition.Holder holder = new ArrayBackedPartition.Holder(RegularAndStaticColumns.NONE, 0, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
      return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
   }

   public static PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec) {
      MutableDeletionInfo deletionInfo = new MutableDeletionInfo(timestamp, nowInSec);
      ArrayBackedPartition.Holder holder = new ArrayBackedPartition.Holder(RegularAndStaticColumns.NONE, 0, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
      return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
   }

   public static PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row) {
      MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
      ArrayBackedPartition.Holder holder;
      if(row.isStatic()) {
         holder = new ArrayBackedPartition.Holder(new RegularAndStaticColumns(Columns.from(row.columns()), Columns.NONE), 0, row, EncodingStats.NO_STATS);
         return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
      } else {
         holder = new ArrayBackedPartition.Holder(new RegularAndStaticColumns(Columns.NONE, Columns.from(row.columns())), 1, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
         holder.rows[holder.length++] = row;
         return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
      }
   }

   public static PartitionUpdate singleRowUpdate(TableMetadata metadata, ByteBuffer key, Row row) {
      return singleRowUpdate(metadata, metadata.partitioner.decorateKey(key), row);
   }

   public static PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter) {
      iterator = UnfilteredRowIterators.withOnlyQueriedData(iterator, filter);
      ArrayBackedPartition p = create(iterator, 16);
      MutableDeletionInfo deletionInfo = (MutableDeletionInfo)p.deletionInfo();
      return new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), p.holder, deletionInfo, false);
   }

   protected boolean canHaveShadowedData() {
      return this.canHaveShadowedData;
   }

   public static PartitionUpdate fromBytes(ByteBuffer bytes, EncodingVersion version) {
      if(bytes == null) {
         return null;
      } else {
         try {
            return ((PartitionUpdate.PartitionUpdateSerializer)serializers.get(version)).deserialize(new DataInputBuffer(bytes, true), SerializationHelper.Flag.LOCAL);
         } catch (IOException var3) {
            throw new RuntimeException(var3);
         }
      }
   }

   public static ByteBuffer toBytes(PartitionUpdate update, EncodingVersion version) {
      try {
         DataOutputBuffer out = new DataOutputBuffer();
         Throwable var3 = null;

         ByteBuffer var4;
         try {
            ((PartitionUpdate.PartitionUpdateSerializer)serializers.get(version)).serialize(update, out);
            var4 = out.asNewBuffer();
         } catch (Throwable var14) {
            var3 = var14;
            throw var14;
         } finally {
            if(out != null) {
               if(var3 != null) {
                  try {
                     out.close();
                  } catch (Throwable var13) {
                     var3.addSuppressed(var13);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var4;
      } catch (IOException var16) {
         throw new RuntimeException(var16);
      }
   }

   public static PartitionUpdate fullPartitionDelete(TableMetadata metadata, ByteBuffer key, long timestamp, int nowInSec) {
      return fullPartitionDelete(metadata, metadata.partitioner.decorateKey(key), timestamp, nowInSec);
   }

   public static PartitionUpdate merge(List<PartitionUpdate> updates) {
      assert !updates.isEmpty();

      int size = updates.size();
      if(size == 1) {
         return (PartitionUpdate)Iterables.getOnlyElement(updates);
      } else {
         int nowInSecs = FBUtilities.nowInSeconds();
         List<UnfilteredRowIterator> asIterators = Lists.transform(updates, ArrayBackedPartition::unfilteredIterator);
         return fromIterator(UnfilteredRowIterators.merge(asIterators, nowInSecs), ColumnFilter.all(((PartitionUpdate)updates.get(0)).metadata()));
      }
   }

   public DeletionInfo deletionInfo() {
      return this.holder.deletionInfo;
   }

   public void updateAllTimestamp(long newTimestamp) {
      ArrayBackedPartition.Holder holder = this.holder();
      holder.deletionInfo = ((MutableDeletionInfo)holder.deletionInfo).updateAllTimestamp(newTimestamp - 1L);

      for(int i = 0; i < holder.length; ++i) {
         holder.rows[i] = holder.rows[i].updateAllTimestamp(newTimestamp);
      }

      Row staticRow = holder.staticRow.updateAllTimestamp(newTimestamp);
      EncodingStats newStats = EncodingStats.Collector.collect(staticRow, this.iterator(), holder.deletionInfo);
      holder.stats = newStats;
      holder.staticRow = staticRow;
   }

   public int operationCount() {
      return this.rowCount() + (this.staticRow().isEmpty()?0:1) + this.holder.deletionInfo.rangeCount() + (this.holder.deletionInfo.getPartitionDeletion().isLive()?0:1);
   }

   public int dataSize() {
      Integer size = Integer.valueOf(0);
      ArrayBackedPartition.Holder holder = this.holder();

      for(int i = 0; i < holder.length; ++i) {
         Row row = holder.rows[i];
         size = Integer.valueOf(size.intValue() + row.clustering().dataSize());
         size = Integer.valueOf(size.intValue() + ((Integer)row.reduce(Integer.valueOf(0), (v, cd) -> {
            return Integer.valueOf(v.intValue() + cd.dataSize());
         })).intValue());
      }

      return size.intValue();
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   public RegularAndStaticColumns columns() {
      return this.holder.columns;
   }

   protected ArrayBackedPartition.Holder holder() {
      this.maybeBuild();
      return this.holder;
   }

   public EncodingStats stats() {
      return this.holder().stats;
   }

   public synchronized void allowNewUpdates() {
      if(!this.canReOpen) {
         throw new IllegalStateException("You cannot do more updates on collectCounterMarks has been called");
      } else {
         this.isBuilt = false;
         this.rowBuilder.clear();
         this.needsSort = false;
      }
   }

   public Iterator<Row> iterator() {
      this.maybeBuild();
      return super.iterator();
   }

   public void validate() {
      ArrayBackedPartition.Holder holder = this.holder();

      for(int i = 0; i < holder.length; ++i) {
         Row row = holder.rows[i];
         this.metadata().comparator.validate(row.clustering());
         row.apply((cd) -> {
            cd.validate();
         }, false);
      }

   }

   public long maxTimestamp() {
      ArrayBackedPartition.Holder holder = this.holder();
      Long maxTimestamp = Long.valueOf(holder.deletionInfo.maxTimestamp());

      for(int i = 0; i < holder.length; ++i) {
         Row row = holder.rows[i];
         maxTimestamp = Long.valueOf(Math.max(maxTimestamp.longValue(), row.primaryKeyLivenessInfo().timestamp()));
         maxTimestamp = (Long)row.reduce(maxTimestamp, (ts, cd) -> {
            if(cd.column().isSimple()) {
               return Long.valueOf(Math.max(ts.longValue(), ((Cell)cd).timestamp()));
            } else {
               ComplexColumnData complexData = (ComplexColumnData)cd;
               ts = Long.valueOf(Math.max(ts.longValue(), complexData.complexDeletion().markedForDeleteAt()));
               ts = (Long)complexData.reduce(ts, (ts2, cell) -> {
                  return Long.valueOf(Math.max(ts2.longValue(), cell.timestamp()));
               });
               return ts;
            }
         });
      }

      return maxTimestamp.longValue();
   }

   public List<PartitionUpdate.CounterMark> collectCounterMarks() {
      assert this.metadata().isCounter();

      this.maybeBuild();
      this.canReOpen = false;
      List<PartitionUpdate.CounterMark> marks = new ArrayList();
      this.addMarksForRow(this.staticRow(), marks);
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         Row row = (Row)var2.next();
         this.addMarksForRow(row, marks);
      }

      return marks;
   }

   private void addMarksForRow(Row row, List<PartitionUpdate.CounterMark> marks) {
      Iterator var3 = row.cells().iterator();

      while(var3.hasNext()) {
         Cell cell = (Cell)var3.next();
         if(cell.isCounterCell()) {
            marks.add(new PartitionUpdate.CounterMark(row, cell.column(), cell.path()));
         }
      }

   }

   private void assertNotBuilt() {
      if(this.isBuilt) {
         throw new IllegalStateException("An update should not be written again once it has been read");
      }
   }

   public void addPartitionDeletion(DeletionTime deletionTime) {
      this.assertNotBuilt();
      ((MutableDeletionInfo)this.holder.deletionInfo).add(deletionTime);
   }

   public void add(RangeTombstone range) {
      this.assertNotBuilt();
      ((MutableDeletionInfo)this.holder.deletionInfo).add(range, this.metadata().comparator);
   }

   public void add(Row row) {
      if(!row.isEmpty()) {
         this.assertNotBuilt();
         if(row.isStatic()) {
            assert this.columns().statics.containsAll(row.columns()) : this.columns().statics + " is not superset of " + row.columns();

            Row staticRow = this.holder.staticRow.isEmpty()?row:Rows.merge(this.holder.staticRow, row, this.createdAtInSec);
            this.holder.staticRow = staticRow;
         } else {
            assert this.columns().regulars.containsAll(row.columns()) : this.columns().regulars + " is not superset of " + row.columns();

            if(!this.needsSort) {
               int size = this.rowBuilder.size();
               this.needsSort = size > 0 && this.metadata.comparator.compare((Clusterable)((Row)this.rowBuilder.get(size - 1)).clustering(), (Clusterable)row) > 0;
            }

            this.rowBuilder.add(row);
         }

      }
   }

   private void maybeBuild() {
      if(!this.isBuilt) {
         this.build();
      }
   }

   private synchronized void build() {
      if(!this.isBuilt) {
         if(this.needsSort) {
            this.rowBuilder.sort((a, b) -> {
               return this.metadata.comparator.compare(a.clustering(), b.clustering());
            });
         }

         this.holder.rows = new Row[this.rowBuilder.size()];
         Iterator var1 = this.rowBuilder.iterator();

         while(var1.hasNext()) {
            Row row = (Row)var1.next();
            int lastIdx = this.holder.length - 1;
            boolean append = lastIdx < 0 || this.metadata.comparator.compare((Clusterable)this.holder.rows[lastIdx], (Clusterable)row) != 0;
            if(append) {
               this.append(row);
            } else {
               this.holder.rows[lastIdx] = Rows.merge(this.holder.rows[lastIdx], row, this.createdAtInSec);
            }
         }

         EncodingStats newStats = EncodingStats.Collector.collect(this.holder.staticRow, super.iterator(), this.holder.deletionInfo);
         this.holder.stats = newStats;
         this.rowBuilder.clear();
         this.needsSort = false;
         this.isBuilt = true;
      }
   }

   public String toString() {
      if(this.isBuilt) {
         return super.toString();
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append(String.format("[%s] key=%s columns=%s", new Object[]{this.metadata.toString(), this.metadata.partitionKeyType.getString(this.partitionKey().getKey()), this.columns()}));
         sb.append("\n    deletionInfo=").append(this.holder.deletionInfo);
         sb.append(" (not built)");
         return sb.toString();
      }
   }

   public static PartitionUpdate.SimpleBuilder simpleBuilder(TableMetadata metadata, Object... partitionKeyValues) {
      return new SimpleBuilders.PartitionUpdateBuilder(metadata, partitionKeyValues);
   }

   public static class CounterMark {
      private final Row row;
      private final ColumnMetadata column;
      private final CellPath path;

      private CounterMark(Row row, ColumnMetadata column, CellPath path) {
         this.row = row;
         this.column = column;
         this.path = path;
      }

      public Clustering clustering() {
         return this.row.clustering();
      }

      public ColumnMetadata column() {
         return this.column;
      }

      public CellPath path() {
         return this.path;
      }

      public ByteBuffer value() {
         return this.path == null?this.row.getCell(this.column).value():this.row.getCell(this.column, this.path).value();
      }

      public void setValue(ByteBuffer value) {
         assert this.row instanceof AbstractRow;

         ((AbstractRow)this.row).setValue(this.column, this.path, value);
      }
   }

   public static class PartitionUpdateSerializer extends VersionDependent<EncodingVersion> {
      private PartitionUpdateSerializer(EncodingVersion version) {
         super(version);
      }

      public void serialize(PartitionUpdate update, DataOutputPlus out) throws IOException {
         UnfilteredRowIterator iter = update.unfilteredIterator();
         Throwable var4 = null;

         try {
            assert !iter.isReverseOrder();

            update.metadata().id.serialize(out);
            ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).serialize((UnfilteredRowIterator)iter, (ColumnFilter)null, out, update.rowCount());
         } catch (Throwable var13) {
            var4 = var13;
            throw var13;
         } finally {
            if(iter != null) {
               if(var4 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var12) {
                     var4.addSuppressed(var12);
                  }
               } else {
                  iter.close();
               }
            }

         }

      }

      public PartitionUpdate deserialize(DataInputPlus in, SerializationHelper.Flag flag) throws IOException {
         TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
         UnfilteredPartitionSerializer.Header header = ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).deserializeHeader(metadata, (ColumnFilter)null, in, flag);
         if(header.isEmpty) {
            return PartitionUpdate.emptyUpdate(metadata, header.key);
         } else {
            assert !header.isReversed;

            assert header.rowEstimate >= 0;

            MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionDeletion, metadata.comparator, false);
            ArrayBackedPartition.Holder holder = new ArrayBackedPartition.Holder(header.sHeader.columns(), header.rowEstimate, header.staticRow, header.sHeader.stats());
            PartitionUpdate p = new PartitionUpdate(metadata, header.key, holder, (MutableDeletionInfo)null, false);
            UnfilteredRowIterator partition = ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).deserializeToIt(in, metadata, flag, header);
            Throwable var9 = null;

            try {
               while(partition.hasNext()) {
                  Unfiltered unfiltered = (Unfiltered)partition.next();
                  if(unfiltered.kind() == Unfiltered.Kind.ROW) {
                     p.append((Row)unfiltered);
                  } else {
                     deletionBuilder.add((RangeTombstoneMarker)unfiltered);
                  }
               }
            } catch (Throwable var18) {
               var9 = var18;
               throw var18;
            } finally {
               if(partition != null) {
                  if(var9 != null) {
                     try {
                        partition.close();
                     } catch (Throwable var17) {
                        var9.addSuppressed(var17);
                     }
                  } else {
                     partition.close();
                  }
               }

            }

            holder.deletionInfo = deletionBuilder.build();
            return p;
         }
      }

      public long serializedSize(PartitionUpdate update) {
         UnfilteredRowIterator iter = update.unfilteredIterator();
         Throwable var3 = null;

         long var4;
         try {
            var4 = (long)update.metadata.id.serializedSize() + ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).serializedSize(iter, (ColumnFilter)null, update.rowCount());
         } catch (Throwable var14) {
            var3 = var14;
            throw var14;
         } finally {
            if(iter != null) {
               if(var3 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var13) {
                     var3.addSuppressed(var13);
                  }
               } else {
                  iter.close();
               }
            }

         }

         return var4;
      }
   }

   public interface SimpleBuilder {
      TableMetadata metadata();

      PartitionUpdate.SimpleBuilder timestamp(long var1);

      PartitionUpdate.SimpleBuilder ttl(int var1);

      PartitionUpdate.SimpleBuilder nowInSec(int var1);

      Row.SimpleBuilder row(Object... var1);

      PartitionUpdate.SimpleBuilder delete();

      PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder addRangeTombstone();

      PartitionUpdate build();

      Mutation buildAsMutation();

      public interface RangeTombstoneBuilder {
         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder start(Object... var1);

         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder end(Object... var1);

         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder inclStart();

         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder exclStart();

         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder inclEnd();

         PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder exclEnd();
      }
   }
}
