package org.apache.cassandra.db.partitions;

import io.reactivex.functions.Function;
import java.io.IOException;
import java.util.Iterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredPartitionSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.Version;

public class CachedBTreePartition extends ImmutableBTreePartition implements CachedPartition {
   private final int createdAtInSec;
   private final int cachedLiveRows;
   private final int rowsWithNonExpiringCells;

   private CachedBTreePartition(TableMetadata metadata, DecoratedKey partitionKey, AbstractBTreePartition.Holder holder, int createdAtInSec, int cachedLiveRows, int rowsWithNonExpiringCells) {
      super(metadata, partitionKey, holder);
      this.createdAtInSec = createdAtInSec;
      this.cachedLiveRows = cachedLiveRows;
      this.rowsWithNonExpiringCells = rowsWithNonExpiringCells;
   }

   public static CachedBTreePartition create(UnfilteredRowIterator iterator, int nowInSec) {
      return create((UnfilteredRowIterator)iterator, 16, nowInSec);
   }

   public static CachedBTreePartition create(UnfilteredRowIterator iterator, int initialRowCapacity, int nowInSec) {
      AbstractBTreePartition.Holder holder = ImmutableBTreePartition.build(iterator, initialRowCapacity);
      int cachedLiveRows = 0;
      int rowsWithNonExpiringCells = 0;
      boolean enforceStrictLiveness = iterator.metadata().enforceStrictLiveness();
      Iterator var7 = BTree.iterable(holder.tree).iterator();

      while(var7.hasNext()) {
         Row row = (Row)var7.next();
         if(row.hasLiveData(nowInSec, enforceStrictLiveness)) {
            ++cachedLiveRows;
         }

         boolean hasNonExpiringLiveCell = false;
         Iterator var10 = row.cells().iterator();

         while(var10.hasNext()) {
            Cell cell = (Cell)var10.next();
            if(!cell.isTombstone() && !cell.isExpiring()) {
               hasNonExpiringLiveCell = true;
               break;
            }
         }

         if(hasNonExpiringLiveCell) {
            ++rowsWithNonExpiringCells;
         }
      }

      return new CachedBTreePartition(iterator.metadata(), iterator.partitionKey(), holder, nowInSec, cachedLiveRows, rowsWithNonExpiringCells);
   }

   public static Flow<CachedBTreePartition> create(FlowableUnfilteredPartition partition, int nowInSec) {
      return create((FlowableUnfilteredPartition)partition, 16, nowInSec);
   }

   public static Flow<CachedBTreePartition> create(FlowableUnfilteredPartition partition, int initialRowCapacity, int nowInSec) {
      return AbstractBTreePartition.build(partition, initialRowCapacity, true).map((holder) -> {
         int cachedLiveRows = 0;
         int rowsWithNonExpiringCells = 0;
         boolean enforceStrictLiveness = partition.metadata().enforceStrictLiveness();
         Iterator var6 = BTree.iterable(holder.tree).iterator();

         while(var6.hasNext()) {
            Row row = (Row)var6.next();
            if(row.hasLiveData(nowInSec, enforceStrictLiveness)) {
               ++cachedLiveRows;
            }

            boolean hasNonExpiringLiveCell = false;
            Iterator var9 = row.cells().iterator();

            while(var9.hasNext()) {
               Cell cell = (Cell)var9.next();
               if(!cell.isTombstone() && !cell.isExpiring()) {
                  hasNonExpiringLiveCell = true;
                  break;
               }
            }

            if(hasNonExpiringLiveCell) {
               ++rowsWithNonExpiringCells;
            }
         }

         return new CachedBTreePartition(partition.metadata(), partition.partitionKey(), holder, nowInSec, cachedLiveRows, rowsWithNonExpiringCells);
      });
   }

   public int cachedLiveRows() {
      return this.cachedLiveRows;
   }

   public int rowsWithNonExpiringCells() {
      return this.rowsWithNonExpiringCells;
   }

   static class Serializer implements ISerializer<CachedPartition> {
      private final EncodingVersion version = (EncodingVersion)Version.last(EncodingVersion.class);

      Serializer() {
      }

      public void serialize(CachedPartition partition, DataOutputPlus out) throws IOException {
         assert partition instanceof CachedBTreePartition;

         CachedBTreePartition p = (CachedBTreePartition)partition;
         out.writeInt(p.createdAtInSec);
         out.writeInt(p.cachedLiveRows);
         out.writeInt(p.rowsWithNonExpiringCells);
         partition.metadata().id.serialize(out);
         UnfilteredRowIterator iter = p.unfilteredIterator();
         Throwable var5 = null;

         try {
            ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).serialize((UnfilteredRowIterator)iter, (ColumnFilter)null, out, p.rowCount());
         } catch (Throwable var14) {
            var5 = var14;
            throw var14;
         } finally {
            if(iter != null) {
               if(var5 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var13) {
                     var5.addSuppressed(var13);
                  }
               } else {
                  iter.close();
               }
            }

         }

      }

      public CachedPartition deserialize(DataInputPlus in) throws IOException {
         int createdAtInSec = in.readInt();
         int cachedLiveRows = in.readInt();
         int rowsWithNonExpiringCells = in.readInt();
         TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
         UnfilteredPartitionSerializer.Header header = ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).deserializeHeader(metadata, (ColumnFilter)null, in, SerializationHelper.Flag.LOCAL);

         assert !header.isReversed && header.rowEstimate >= 0;

         UnfilteredRowIterator partition = ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).deserializeToIt(in, metadata, SerializationHelper.Flag.LOCAL, header);
         Throwable var9 = null;

         AbstractBTreePartition.Holder holder;
         try {
            holder = ImmutableBTreePartition.build(partition, header.rowEstimate);
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

         return new CachedBTreePartition(metadata, header.key, holder, createdAtInSec, cachedLiveRows, rowsWithNonExpiringCells);
      }

      public long serializedSize(CachedPartition partition) {
         assert partition instanceof CachedBTreePartition;

         CachedBTreePartition p = (CachedBTreePartition)partition;
         UnfilteredRowIterator iter = p.unfilteredIterator();
         Throwable var4 = null;

         long var5;
         try {
            var5 = (long)(TypeSizes.sizeof(p.createdAtInSec) + TypeSizes.sizeof(p.cachedLiveRows) + TypeSizes.sizeof(p.rowsWithNonExpiringCells) + partition.metadata().id.serializedSize()) + ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).serializedSize(iter, (ColumnFilter)null, p.rowCount());
         } catch (Throwable var15) {
            var4 = var15;
            throw var15;
         } finally {
            if(iter != null) {
               if(var4 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var14) {
                     var4.addSuppressed(var14);
                  }
               } else {
                  iter.close();
               }
            }

         }

         return var5;
      }
   }
}
