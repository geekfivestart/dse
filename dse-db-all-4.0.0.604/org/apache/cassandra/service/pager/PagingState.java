package org.apache.cassandra.service.pager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringVersion;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PagingState {
   public final ByteBuffer partitionKey;
   public final PagingState.RowMark rowMark;
   public final int remaining;
   public final int remainingInPartition;
   public final boolean inclusive;

   public PagingState(ByteBuffer partitionKey, PagingState.RowMark rowMark, int remaining, int remainingInPartition, boolean inclusive) {
      this.partitionKey = partitionKey;
      this.rowMark = rowMark;
      this.remaining = remaining;
      this.remainingInPartition = remainingInPartition;
      this.inclusive = inclusive;
   }

   public static PagingState deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      if(bytes == null) {
         return null;
      } else {
         try {
            DataInputBuffer in = new DataInputBuffer(bytes, true);
            Throwable var3 = null;

            PagingState var9;
            try {
               boolean inclusive = false;
               ByteBuffer pk;
               PagingState.RowMark mark;
               int remaining;
               int remainingInPartition;
               if(protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)) {
                  pk = ByteBufferUtil.readWithShortLength(in);
                  mark = new PagingState.RowMark(ByteBufferUtil.readWithShortLength(in), protocolVersion);
                  remaining = in.readInt();
                  remainingInPartition = in.available() > 0?in.readInt():2147483647;
               } else {
                  pk = ByteBufferUtil.readWithVIntLength(in);
                  mark = new PagingState.RowMark(ByteBufferUtil.readWithVIntLength(in), protocolVersion);
                  remaining = (int)in.readUnsignedVInt();
                  remainingInPartition = (int)in.readUnsignedVInt();
                  if(ProtocolVersion.DSE_V1.compareTo(protocolVersion) <= 0) {
                     inclusive = in.readBoolean();
                  }
               }

               var9 = new PagingState(pk.hasRemaining()?pk:null, mark.mark.hasRemaining()?mark:null, remaining, remainingInPartition, inclusive);
            } catch (Throwable var19) {
               var3 = var19;
               throw var19;
            } finally {
               if(in != null) {
                  if(var3 != null) {
                     try {
                        in.close();
                     } catch (Throwable var18) {
                        var3.addSuppressed(var18);
                     }
                  } else {
                     in.close();
                  }
               }

            }

            return var9;
         } catch (IOException var21) {
            throw new ProtocolException("Invalid value for the paging state");
         }
      }
   }

   public ByteBuffer serialize(ProtocolVersion protocolVersion) {
      assert this.rowMark == null || protocolVersion == this.rowMark.protocolVersion : String.format("rowMark = %s protocolVersion = %d", new Object[]{this.rowMark, protocolVersion});

      try {
         DataOutputBuffer out = new DataOutputBufferFixed(this.serializedSize(protocolVersion));
         Throwable var3 = null;

         ByteBuffer var6;
         try {
            ByteBuffer pk = this.partitionKey == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:this.partitionKey;
            ByteBuffer mark = this.rowMark == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:this.rowMark.mark;
            if(protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)) {
               ByteBufferUtil.writeWithShortLength((ByteBuffer)pk, (DataOutputPlus)out);
               ByteBufferUtil.writeWithShortLength((ByteBuffer)mark, (DataOutputPlus)out);
               out.writeInt(this.remaining);
               out.writeInt(this.remainingInPartition);
            } else {
               ByteBufferUtil.writeWithVIntLength(pk, out);
               ByteBufferUtil.writeWithVIntLength(mark, out);
               out.writeUnsignedVInt((long)this.remaining);
               out.writeUnsignedVInt((long)this.remainingInPartition);
               if(ProtocolVersion.DSE_V1.compareTo(protocolVersion) <= 0) {
                  out.writeBoolean(this.inclusive);
               }
            }

            var6 = out.buffer();
         } catch (Throwable var16) {
            var3 = var16;
            throw var16;
         } finally {
            if(out != null) {
               if(var3 != null) {
                  try {
                     out.close();
                  } catch (Throwable var15) {
                     var3.addSuppressed(var15);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var6;
      } catch (IOException var18) {
         throw new RuntimeException(var18);
      }
   }

   public int serializedSize(ProtocolVersion protocolVersion) {
      assert this.rowMark == null || protocolVersion == this.rowMark.protocolVersion;

      ByteBuffer pk = this.partitionKey == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:this.partitionKey;
      ByteBuffer mark = this.rowMark == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:this.rowMark.mark;
      return protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)?ByteBufferUtil.serializedSizeWithShortLength(pk) + ByteBufferUtil.serializedSizeWithShortLength(mark) + 8:ByteBufferUtil.serializedSizeWithVIntLength(pk) + ByteBufferUtil.serializedSizeWithVIntLength(mark) + TypeSizes.sizeofUnsignedVInt((long)this.remaining) + TypeSizes.sizeofUnsignedVInt((long)this.remainingInPartition) + (ProtocolVersion.DSE_V1.compareTo(protocolVersion) <= 0?TypeSizes.sizeof(this.inclusive):0);
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{this.partitionKey, this.rowMark, Integer.valueOf(this.remaining), Integer.valueOf(this.remainingInPartition), Boolean.valueOf(this.inclusive)});
   }

   public final boolean equals(Object o) {
      if(!(o instanceof PagingState)) {
         return false;
      } else {
         PagingState that = (PagingState)o;
         return Objects.equals(this.partitionKey, that.partitionKey) && Objects.equals(this.rowMark, that.rowMark) && this.remaining == that.remaining && this.remainingInPartition == that.remainingInPartition && this.inclusive == that.inclusive;
      }
   }

   public String toString() {
      return String.format("PagingState(key=%s, cellname=%s, remaining=%d, remainingInPartition=%d, inclusive=%b", new Object[]{this.partitionKey != null?ByteBufferUtil.bytesToHex(this.partitionKey):null, this.rowMark, Integer.valueOf(this.remaining), Integer.valueOf(this.remainingInPartition), Boolean.valueOf(this.inclusive)});
   }

   public static class RowMark {
      private final ByteBuffer mark;
      private final ProtocolVersion protocolVersion;

      private RowMark(ByteBuffer mark, ProtocolVersion protocolVersion) {
         this.mark = mark;
         this.protocolVersion = protocolVersion;
      }

      private static List<AbstractType<?>> makeClusteringTypes(TableMetadata metadata) {
         int size = metadata.clusteringColumns().size();
         List<AbstractType<?>> l = new ArrayList(size);

         for(int i = 0; i < size; ++i) {
            l.add(BytesType.instance);
         }

         return l;
      }

      public static PagingState.RowMark create(TableMetadata metadata, Row row, ProtocolVersion protocolVersion) {
         ByteBuffer mark;
         if(protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)) {
            Iterator<Cell> cells = row.cellsInLegacyOrder(metadata, true).iterator();
            if(!cells.hasNext()) {
               assert !metadata.isCompactTable();

               mark = encodeCellName(metadata, row.clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, (ByteBuffer)null);
            } else {
               Cell cell = (Cell)cells.next();
               mark = encodeCellName(metadata, row.clustering(), cell.column().name.bytes, cell.column().isComplex()?cell.path().get(0):null);
            }
         } else {
            mark = Clustering.serializer.serialize(row.clustering(), ClusteringVersion.OSS_30, makeClusteringTypes(metadata));
         }

         return new PagingState.RowMark(mark, protocolVersion);
      }

      public Clustering clustering(TableMetadata metadata) {
         return this.mark == null?null:(this.protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)?decodeClustering(metadata, this.mark):Clustering.serializer.deserialize(this.mark, ClusteringVersion.OSS_30, makeClusteringTypes(metadata)));
      }

      private static ByteBuffer encodeCellName(TableMetadata metadata, Clustering clustering, ByteBuffer columnName, ByteBuffer collectionElement) {
         boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
         if(!metadata.isCompound()) {
            if(isStatic) {
               return columnName;
            } else {
               assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();

               return clustering.get(0);
            }
         } else {
            int clusteringSize = metadata.comparator.size();
            int size = clusteringSize + (metadata.isDense()?0:1) + (collectionElement == null?0:1);
            if(metadata.isSuper()) {
               size = clusteringSize + 1;
            }

            ByteBuffer[] values = new ByteBuffer[size];

            for(int i = 0; i < clusteringSize; ++i) {
               if(isStatic) {
                  values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
               } else {
                  ByteBuffer v = clustering.get(i);
                  if(v == null) {
                     return CompositeType.build((ByteBuffer[])Arrays.copyOfRange(values, 0, i));
                  }

                  values[i] = v;
               }
            }

            if(metadata.isSuper()) {
               assert columnName != null;

               values[clusteringSize] = columnName.equals(CompactTables.SUPER_COLUMN_MAP_COLUMN)?collectionElement:columnName;
            } else {
               if(!metadata.isDense()) {
                  values[clusteringSize] = columnName;
               }

               if(collectionElement != null) {
                  values[clusteringSize + 1] = collectionElement;
               }
            }

            return CompositeType.build(isStatic, values);
         }
      }

      private static Clustering decodeClustering(TableMetadata metadata, ByteBuffer value) {
         int csize = metadata.comparator.size();
         if(csize == 0) {
            return Clustering.EMPTY;
         } else if(metadata.isCompound() && CompositeType.isStaticName(value)) {
            return Clustering.STATIC_CLUSTERING;
         } else {
            List<ByteBuffer> components = metadata.isCompound()?CompositeType.splitName(value):Collections.singletonList(value);
            return Clustering.make((ByteBuffer[])components.subList(0, Math.min(csize, components.size())).toArray(ByteBufferUtil.EMPTY_BUFFER_ARRAY));
         }
      }

      public final int hashCode() {
         return Objects.hash(new Object[]{this.mark, this.protocolVersion});
      }

      public final boolean equals(Object o) {
         if(!(o instanceof PagingState.RowMark)) {
            return false;
         } else {
            PagingState.RowMark that = (PagingState.RowMark)o;
            return Objects.equals(this.mark, that.mark) && this.protocolVersion == that.protocolVersion;
         }
      }

      public String toString() {
         return String.format("[%s]", new Object[]{this.mark == null?"null":ByteBufferUtil.bytesToHex(this.mark)});
      }
   }
}
