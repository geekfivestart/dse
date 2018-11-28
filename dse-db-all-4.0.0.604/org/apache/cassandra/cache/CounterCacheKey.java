package org.apache.cassandra.cache;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

public final class CounterCacheKey extends CacheKey {
   private static final long EMPTY_SIZE;
   private final byte[] partitionKey;
   private final byte[] cellName;

   private CounterCacheKey(TableMetadata tableMetadata, byte[] partitionKey, byte[] cellName) {
      super(tableMetadata);
      this.partitionKey = partitionKey;
      this.cellName = cellName;
   }

   private CounterCacheKey(TableMetadata tableMetadata, ByteBuffer partitionKey, ByteBuffer cellName) {
      this(tableMetadata, ByteBufferUtil.getArray(partitionKey), ByteBufferUtil.getArray(cellName));
   }

   public static CounterCacheKey create(TableMetadata tableMetadata, ByteBuffer partitionKey, Clustering clustering, ColumnMetadata c, CellPath path) {
      return new CounterCacheKey(tableMetadata, partitionKey, makeCellName(clustering, c, path));
   }

   private static ByteBuffer makeCellName(Clustering clustering, ColumnMetadata c, CellPath path) {
      int cs = clustering.size();
      ByteBuffer[] values = new ByteBuffer[cs + 1 + (path == null?0:path.size())];

      int i;
      for(i = 0; i < cs; ++i) {
         values[i] = clustering.get(i);
      }

      values[cs] = c.name.bytes;
      if(path != null) {
         for(i = 0; i < path.size(); ++i) {
            values[cs + 1 + i] = path.get(i);
         }
      }

      return CompositeType.build(values);
   }

   public ByteBuffer partitionKey() {
      return ByteBuffer.wrap(this.partitionKey);
   }

   public ByteBuffer readCounterValue(ColumnFamilyStore cfs) {
      TableMetadata metadata = cfs.metadata();

      assert metadata.id.equals(this.tableId) && Objects.equals(metadata.indexName().orElse(null), this.indexName);

      DecoratedKey key = cfs.decorateKey(this.partitionKey());
      int clusteringSize = metadata.comparator.size();
      List<ByteBuffer> buffers = CompositeType.splitName(ByteBuffer.wrap(this.cellName));

      assert buffers.size() >= clusteringSize + 1;

      Clustering clustering = Clustering.make((ByteBuffer[])buffers.subList(0, clusteringSize).toArray(ByteBufferUtil.EMPTY_BUFFER_ARRAY));
      ColumnMetadata column = metadata.getColumn((ByteBuffer)buffers.get(clusteringSize));
      if(column == null) {
         return null;
      } else {
         CellPath path = column.isComplex()?CellPath.create((ByteBuffer)buffers.get(buffers.size() - 1)):null;
         int nowInSec = FBUtilities.nowInSeconds();
         ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
         if(path == null) {
            builder.add(column);
         } else {
            builder.select(column, path);
         }

         ClusteringIndexFilter filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(clustering, metadata.comparator), false);
         SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(metadata, nowInSec, key, builder.build(), filter);
         ReadExecutionController controller = cmd.executionController();
         Throwable var14 = null;

         ByteBuffer var18;
         try {
            RowIterator iter = UnfilteredRowIterators.filter(FlowablePartitions.toIterator((FlowableUnfilteredPartition)cmd.deferredQuery(cfs, controller).blockingSingle()), nowInSec);
            Throwable var16 = null;

            try {
               ByteBuffer value = null;
               if(column.isStatic()) {
                  value = iter.staticRow().getCell(column).value();
               } else if(iter.hasNext()) {
                  value = ((Row)iter.next()).getCell(column).value();
               }

               var18 = value;
            } catch (Throwable var41) {
               var16 = var41;
               throw var41;
            } finally {
               if(iter != null) {
                  if(var16 != null) {
                     try {
                        iter.close();
                     } catch (Throwable var40) {
                        var16.addSuppressed(var40);
                     }
                  } else {
                     iter.close();
                  }
               }

            }
         } catch (Throwable var43) {
            var14 = var43;
            throw var43;
         } finally {
            if(controller != null) {
               if(var14 != null) {
                  try {
                     controller.close();
                  } catch (Throwable var39) {
                     var14.addSuppressed(var39);
                  }
               } else {
                  controller.close();
               }
            }

         }

         return var18;
      }
   }

   public void write(DataOutputPlus out) throws IOException {
      ByteBufferUtil.writeWithLength((byte[])this.partitionKey, (DataOutput)out);
      ByteBufferUtil.writeWithLength((byte[])this.cellName, (DataOutput)out);
   }

   public static CounterCacheKey read(TableMetadata tableMetadata, DataInputPlus in) throws IOException {
      return new CounterCacheKey(tableMetadata, ByteBufferUtil.readBytesWithLength(in), ByteBufferUtil.readBytesWithLength(in));
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + ObjectSizes.sizeOfArray(this.partitionKey) + ObjectSizes.sizeOfArray(this.cellName);
   }

   public String toString() {
      TableMetadataRef tableRef = Schema.instance.getTableMetadataRef(this.tableId);
      return String.format("CounterCacheKey(%s, %s, %s, %s)", new Object[]{tableRef, this.indexName, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(this.partitionKey)), ByteBufferUtil.bytesToHex(ByteBuffer.wrap(this.cellName))});
   }

   public int hashCode() {
      return Arrays.deepHashCode(new Object[]{this.tableId, this.indexName, this.partitionKey, this.cellName});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof CounterCacheKey)) {
         return false;
      } else {
         CounterCacheKey cck = (CounterCacheKey)o;
         return this.tableId.equals(cck.tableId) && Objects.equals(this.indexName, cck.indexName) && Arrays.equals(this.partitionKey, cck.partitionKey) && Arrays.equals(this.cellName, cck.cellName);
      }
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(new CounterCacheKey(TableMetadata.builder("ks", "tab").addPartitionKeyColumn((String)"pk", UTF8Type.instance).build(), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBuffer.allocate(1)));
   }
}
