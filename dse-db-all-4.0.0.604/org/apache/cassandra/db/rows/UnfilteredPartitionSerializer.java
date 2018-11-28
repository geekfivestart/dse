package org.apache.cassandra.db.rows;

import java.io.IOError;
import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnfilteredPartitionSerializer extends VersionDependent<EncodingVersion> {
   protected static final Logger logger = LoggerFactory.getLogger(UnfilteredPartitionSerializer.class);
   private static final int IS_EMPTY = 1;
   private static final int IS_REVERSED = 2;
   private static final int HAS_PARTITION_DELETION = 4;
   private static final int HAS_STATIC_ROW = 8;
   private static final int HAS_ROW_ESTIMATE = 16;
   public static final Versioned<EncodingVersion, UnfilteredPartitionSerializer> serializers = EncodingVersion.versioned(UnfilteredPartitionSerializer::new);
   private final UnfilteredSerializer unfilteredSerializer;

   private UnfilteredPartitionSerializer(EncodingVersion version) {
      super(version);
      this.unfilteredSerializer = (UnfilteredSerializer)UnfilteredSerializer.serializers.get(version);
   }

   public Flow<Void> serialize(FlowableUnfilteredPartition partition, ColumnFilter selection, DataOutputPlus out) throws IOException {
      return this.serialize((FlowableUnfilteredPartition)partition, selection, out, -1);
   }

   public Flow<Void> serialize(FlowableUnfilteredPartition partition, ColumnFilter selection, DataOutputPlus out, int rowEstimate) throws IOException {
      SerializationHeader header = new SerializationHeader(false, partition.header().metadata, partition.header().columns, partition.header().stats);
      this.serializeBeginningOfPartition(partition, header, selection, out, rowEstimate, false);
      return partition.content().process((unfiltered) -> {
         this.serialize(unfiltered, header, out);
      }).map((VOID) -> {
         this.serializeEndOfPartition(out);
         return VOID;
      });
   }

   public void serialize(UnfilteredRowIterator iterator, ColumnFilter selection, DataOutputPlus out) throws IOException {
      this.serialize((UnfilteredRowIterator)iterator, selection, out, -1);
   }

   public void serialize(UnfilteredRowIterator iterator, ColumnFilter selection, DataOutputPlus out, int rowEstimate) throws IOException {
      SerializationHeader header = new SerializationHeader(false, iterator.metadata(), iterator.columns(), iterator.stats());
      boolean isEmpty = iterator.isEmpty();
      this.serializeBeginningOfPartition(iterator, header, selection, out, rowEstimate, isEmpty);
      if(!isEmpty) {
         while(iterator.hasNext()) {
            this.serialize((Unfiltered)iterator.next(), header, out);
         }

         this.serializeEndOfPartition(out);
      }
   }

   private void serializeBeginningOfPartition(PartitionTrait partition, SerializationHeader header, ColumnFilter selection, DataOutputPlus out, int rowEstimate, boolean isEmpty) throws IOException {
      assert !header.isForSSTable();

      ByteBufferUtil.writeWithVIntLength(partition.partitionKey().getKey(), out);
      int flags = 0;
      if(partition.isReverseOrder()) {
         flags |= 2;
      }

      if(isEmpty) {
         out.writeByte((byte)(flags | 1));
      } else {
         DeletionTime partitionDeletion = partition.partitionLevelDeletion();
         if(!partitionDeletion.isLive()) {
            flags |= 4;
         }

         Row staticRow = partition.staticRow();
         boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
         if(hasStatic) {
            flags |= 8;
         }

         if(rowEstimate >= 0) {
            flags |= 16;
         }

         out.writeByte((byte)flags);
         SerializationHeader.serializer.serializeForMessaging(header, selection, out, hasStatic);
         if(!partitionDeletion.isLive()) {
            header.writeDeletionTime(partitionDeletion, out);
         }

         if(hasStatic) {
            this.unfilteredSerializer.serialize(staticRow, header, out);
         }

         if(rowEstimate >= 0) {
            out.writeUnsignedVInt((long)rowEstimate);
         }

      }
   }

   private void serialize(Unfiltered unfiltered, SerializationHeader header, DataOutputPlus out) throws IOException {
      this.unfilteredSerializer.serialize(unfiltered, header, out);
   }

   private void serializeEndOfPartition(DataOutputPlus out) throws IOException {
      this.unfilteredSerializer.writeEndOfPartition(out);
   }

   public long serializedSize(UnfilteredRowIterator iterator, ColumnFilter selection, int rowEstimate) {
      SerializationHeader header = new SerializationHeader(false, iterator.metadata(), iterator.columns(), iterator.stats());

      assert rowEstimate >= 0;

      long size = (long)(ByteBufferUtil.serializedSizeWithVIntLength(iterator.partitionKey().getKey()) + 1);
      if(iterator.isEmpty()) {
         return size;
      } else {
         DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
         Row staticRow = iterator.staticRow();
         boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
         size += SerializationHeader.serializer.serializedSizeForMessaging(header, selection, hasStatic);
         if(!partitionDeletion.isLive()) {
            size += header.deletionTimeSerializedSize(partitionDeletion);
         }

         if(hasStatic) {
            size += this.unfilteredSerializer.serializedSize(staticRow, header);
         }

         if(rowEstimate >= 0) {
            size += (long)TypeSizes.sizeofUnsignedVInt((long)rowEstimate);
         }

         while(iterator.hasNext()) {
            size += this.unfilteredSerializer.serializedSize((Unfiltered)iterator.next(), header);
         }

         size += this.unfilteredSerializer.serializedSizeEndOfPartition();
         return size;
      }
   }

   public UnfilteredPartitionSerializer.Header deserializeHeader(TableMetadata metadata, ColumnFilter selection, DataInputPlus in, SerializationHelper.Flag flag) throws IOException {
      DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
      int flags = in.readUnsignedByte();
      boolean isReversed = (flags & 2) != 0;
      if((flags & 1) != 0) {
         SerializationHeader sh = new SerializationHeader(false, metadata, RegularAndStaticColumns.NONE, EncodingStats.NO_STATS);
         return new UnfilteredPartitionSerializer.Header(sh, key, isReversed, true, (DeletionTime)null, (Row)null, 0);
      } else {
         boolean hasPartitionDeletion = (flags & 4) != 0;
         boolean hasStatic = (flags & 8) != 0;
         boolean hasRowEstimate = (flags & 16) != 0;
         SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, selection, hasStatic);
         DeletionTime partitionDeletion = hasPartitionDeletion?header.readDeletionTime(in):DeletionTime.LIVE;
         Row staticRow = Rows.EMPTY_STATIC_ROW;
         if(hasStatic) {
            staticRow = this.unfilteredSerializer.deserializeStaticRow(in, header, new SerializationHelper(metadata, (EncodingVersion)this.version, flag));
         }

         int rowEstimate = hasRowEstimate?(int)in.readUnsignedVInt():-1;
         return new UnfilteredPartitionSerializer.Header(header, key, isReversed, false, partitionDeletion, staticRow, rowEstimate);
      }
   }

   public UnfilteredRowIterator deserializeToIt(final DataInputPlus in, TableMetadata metadata, SerializationHelper.Flag flag, UnfilteredPartitionSerializer.Header header) throws IOException {
      if(header.isEmpty) {
         return EmptyIterators.unfilteredRow(metadata, header.key, header.isReversed);
      } else {
         final SerializationHelper helper = new SerializationHelper(metadata, (EncodingVersion)this.version, flag);
         final SerializationHeader sHeader = header.sHeader;
         return new AbstractUnfilteredRowIterator(metadata, header.key, header.partitionDeletion, sHeader.columns(), header.staticRow, header.isReversed, sHeader.stats()) {
            private final Row.Builder builder = Row.Builder.sorted();

            protected Unfiltered computeNext() {
               try {
                  Unfiltered unfiltered = UnfilteredPartitionSerializer.this.unfilteredSerializer.deserialize(in, sHeader, helper, this.builder);
                  return unfiltered == null?(Unfiltered)this.endOfData():unfiltered;
               } catch (IOException var2) {
                  throw new IOError(var2);
               }
            }
         };
      }
   }

   private FlowableUnfilteredPartition deserializeToFlow(DataInputPlus in, TableMetadata metadata, SerializationHelper.Flag flag, UnfilteredPartitionSerializer.Header header) {
      return (FlowableUnfilteredPartition)(header.isEmpty?FlowablePartitions.empty(metadata, header.key, header.isReversed):new UnfilteredPartitionSerializer.DeserializePartitionFlow(new PartitionHeader(metadata, header.key, header.partitionDeletion, header.sHeader.columns(), header.isReversed, header.sHeader.stats()), header.staticRow, in, metadata, flag, header));
   }

   public FlowableUnfilteredPartition deserializeToFlow(DataInputPlus in, TableMetadata metadata, ColumnFilter selection, SerializationHelper.Flag flag) throws IOException {
      return this.deserializeToFlow(in, metadata, flag, this.deserializeHeader(metadata, selection, in, flag));
   }

   public static class Header {
      public final SerializationHeader sHeader;
      public final DecoratedKey key;
      public final boolean isReversed;
      public final boolean isEmpty;
      public final DeletionTime partitionDeletion;
      public final Row staticRow;
      public final int rowEstimate;

      private Header(SerializationHeader sHeader, DecoratedKey key, boolean isReversed, boolean isEmpty, DeletionTime partitionDeletion, Row staticRow, int rowEstimate) {
         this.sHeader = sHeader;
         this.key = key;
         this.isReversed = isReversed;
         this.isEmpty = isEmpty;
         this.partitionDeletion = partitionDeletion;
         this.staticRow = staticRow;
         this.rowEstimate = rowEstimate;
      }

      public String toString() {
         return String.format("{header=%s, key=%s, isReversed=%b, isEmpty=%b, del=%s, staticRow=%s, rowEstimate=%d}", new Object[]{this.sHeader, this.key, Boolean.valueOf(this.isReversed), Boolean.valueOf(this.isEmpty), this.partitionDeletion, this.staticRow, Integer.valueOf(this.rowEstimate)});
      }
   }

   private class DeserializePartitionFlow extends FlowableUnfilteredPartition.FlowSource {
      private final Row.Builder builder;
      private final SerializationHelper helper;
      private final SerializationHeader sHeader;
      private final DataInputPlus in;
      private volatile boolean completed;

      private DeserializePartitionFlow(PartitionHeader ph, Row staticRow, DataInputPlus in, TableMetadata metadata, SerializationHelper.Flag flag, UnfilteredPartitionSerializer.Header header) {
         super(ph, staticRow);
         this.builder = Row.Builder.sorted();
         this.in = in;
         this.helper = new SerializationHelper(metadata, (EncodingVersion)UnfilteredPartitionSerializer.this.version, flag);
         this.sHeader = header.sHeader;
      }

      public void requestNext() {
         try {
            if(this.completed) {
               this.subscriber.onError(new IllegalStateException("Request should not be called after closing"));
               return;
            }

            Unfiltered unfiltered = UnfilteredPartitionSerializer.this.unfilteredSerializer.deserialize(this.in, this.sHeader, this.helper, this.builder);
            if(unfiltered == null) {
               this.completed = true;
               this.subscriber.onComplete();
            } else {
               this.subscriber.onNext(unfiltered);
            }
         } catch (Throwable var2) {
            this.subscriber.onError(var2);
         }

      }

      public void close() throws Exception {
         if(!this.completed) {
            Unfiltered unfiltered;
            do {
               unfiltered = UnfilteredPartitionSerializer.this.unfilteredSerializer.deserialize(this.in, this.sHeader, this.helper, this.builder);
            } while(unfiltered != null);

            this.completed = true;
         }
      }
   }
}
