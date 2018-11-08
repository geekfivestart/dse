package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.Histogram;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.github.jamm.Unmetered;

public class BigRowIndexEntry extends RowIndexEntry {
   static final int CACHE_NOT_INDEXED = 0;
   static final int CACHE_INDEXED = 1;
   static final int CACHE_INDEXED_SHALLOW = 2;
   static final Histogram indexEntrySizeHistogram;
   static final Histogram indexInfoCountHistogram;
   static final Histogram indexInfoGetsHistogram;

   public BigRowIndexEntry(long position) {
      super(position);
   }

   public boolean indexOnHeap() {
      return false;
   }

   public static BigRowIndexEntry create(long dataFilePosition, long indexFilePosition, DeletionTime deletionTime, long headerLength, int columnIndexCount, int indexedPartSize, List<IndexInfo> indexSamples, int[] offsets, ISerializer<IndexInfo> idxInfoSerializer) {
      return (BigRowIndexEntry)(indexSamples != null && indexSamples.size() > 1?new BigRowIndexEntry.IndexedEntry(dataFilePosition, deletionTime, headerLength, (IndexInfo[])indexSamples.toArray(new IndexInfo[0]), offsets, indexedPartSize, idxInfoSerializer):(columnIndexCount > 1?new BigRowIndexEntry.ShallowIndexedEntry(dataFilePosition, indexFilePosition, deletionTime, headerLength, columnIndexCount, indexedPartSize, idxInfoSerializer):new BigRowIndexEntry(dataFilePosition)));
   }

   public BigRowIndexEntry.IndexInfoRetriever openWithIndex(FileHandle indexFile, Rebufferer.ReaderConstraint rc) {
      return null;
   }

   private static int serializedSize(DeletionTime deletionTime, long headerLength, int columnIndexCount) {
      return TypeSizes.sizeofUnsignedVInt(headerLength) + (int)DeletionTime.serializer.serializedSize(deletionTime) + TypeSizes.sizeofUnsignedVInt((long)columnIndexCount);
   }

   public void serialize(DataOutputPlus out, IndexInfo.Serializer idxInfoSerializer, ByteBuffer indexInfo) throws IOException {
      out.writeUnsignedVInt(this.position);
      out.writeUnsignedVInt(0L);
   }

   public void serializeForCache(DataOutputPlus out) throws IOException {
      out.writeUnsignedVInt(this.position);
      out.writeByte(0);
   }

   static {
      MetricNameFactory factory = new DefaultNameFactory("Index", "RowIndexEntry");
      indexEntrySizeHistogram = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("IndexedEntrySize"), false);
      indexInfoCountHistogram = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("IndexInfoCount"), false);
      indexInfoGetsHistogram = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("IndexInfoGets"), false);
   }

   private abstract static class FileIndexInfoRetriever implements BigRowIndexEntry.IndexInfoRetriever {
      final long indexInfoFilePosition;
      final ISerializer<IndexInfo> idxInfoSerializer;
      final FileDataInput indexReader;
      int retrievals;

      FileIndexInfoRetriever(long indexInfoFilePosition, FileDataInput indexReader, ISerializer<IndexInfo> idxInfoSerializer) {
         this.indexInfoFilePosition = indexInfoFilePosition;
         this.idxInfoSerializer = idxInfoSerializer;
         this.indexReader = indexReader;
      }

      public final IndexInfo columnsIndex(int index) throws IOException {
         return this.fetchIndex(index);
      }

      abstract IndexInfo fetchIndex(int var1) throws IOException;

      public void close() throws IOException {
         this.indexReader.close();
         BigRowIndexEntry.indexInfoGetsHistogram.update(this.retrievals);
      }
   }

   public interface IndexInfoRetriever extends AutoCloseable {
      IndexInfo columnsIndex(int var1) throws IOException;

      void close() throws IOException;
   }

   private static final class ShallowInfoRetriever extends BigRowIndexEntry.FileIndexInfoRetriever {
      private final int offsetsOffset;

      private ShallowInfoRetriever(long indexInfoFilePosition, int offsetsOffset, FileDataInput indexReader, ISerializer<IndexInfo> idxInfoSerializer) {
         super(indexInfoFilePosition, indexReader, idxInfoSerializer);
         this.offsetsOffset = offsetsOffset;
      }

      IndexInfo fetchIndex(int index) throws IOException {
         ++this.retrievals;
         this.indexReader.seek(this.indexInfoFilePosition + (long)this.offsetsOffset + (long)(index * TypeSizes.sizeof((int)0)));
         int indexInfoPos = this.indexReader.readInt();
         this.indexReader.seek(this.indexInfoFilePosition + (long)indexInfoPos);
         return (IndexInfo)this.idxInfoSerializer.deserialize(this.indexReader);
      }
   }

   private static final class ShallowIndexedEntry extends BigRowIndexEntry {
      private static final long BASE_SIZE;
      private final long indexFilePosition;
      private final DeletionTime deletionTime;
      private final long headerLength;
      private final int columnsIndexCount;
      private final int indexedPartSize;
      private final int offsetsOffset;
      @Unmetered
      private final ISerializer<IndexInfo> idxInfoSerializer;
      private final int fieldsSerializedSize;

      private ShallowIndexedEntry(long dataFilePosition, long indexFilePosition, DeletionTime deletionTime, long headerLength, int columnIndexCount, int indexedPartSize, ISerializer<IndexInfo> idxInfoSerializer) {
         super(dataFilePosition);

         assert columnIndexCount > 1;

         this.indexFilePosition = indexFilePosition;
         this.headerLength = headerLength;
         this.deletionTime = deletionTime;
         this.columnsIndexCount = columnIndexCount;
         this.indexedPartSize = indexedPartSize;
         this.idxInfoSerializer = idxInfoSerializer;
         this.fieldsSerializedSize = BigRowIndexEntry.serializedSize(deletionTime, headerLength, columnIndexCount);
         this.offsetsOffset = indexedPartSize + this.fieldsSerializedSize - this.columnsIndexCount * TypeSizes.sizeof((int)0);
      }

      private ShallowIndexedEntry(long dataFilePosition, DataInputPlus in, IndexInfo.Serializer idxInfoSerializer) throws IOException {
         super(dataFilePosition);
         this.indexFilePosition = in.readUnsignedVInt();
         this.headerLength = in.readUnsignedVInt();
         this.deletionTime = DeletionTime.serializer.deserialize(in);
         this.columnsIndexCount = (int)in.readUnsignedVInt();
         this.indexedPartSize = (int)in.readUnsignedVInt();
         this.idxInfoSerializer = idxInfoSerializer;
         this.fieldsSerializedSize = BigRowIndexEntry.serializedSize(this.deletionTime, this.headerLength, this.columnsIndexCount);
         this.offsetsOffset = this.indexedPartSize + this.fieldsSerializedSize - this.columnsIndexCount * TypeSizes.sizeof((int)0);
      }

      public int rowIndexCount() {
         return this.columnsIndexCount;
      }

      public DeletionTime deletionTime() {
         return this.deletionTime;
      }

      public BigRowIndexEntry.IndexInfoRetriever openWithIndex(FileHandle indexFile, Rebufferer.ReaderConstraint rc) {
         indexEntrySizeHistogram.update(this.indexedPartSize + this.fieldsSerializedSize);
         indexInfoCountHistogram.update(this.columnsIndexCount);
         return new BigRowIndexEntry.ShallowInfoRetriever(this.indexFilePosition + (long)VIntCoding.computeUnsignedVIntSize(this.position) + (long)VIntCoding.computeUnsignedVIntSize((long)(this.indexedPartSize + this.fieldsSerializedSize)) + (long)this.fieldsSerializedSize, this.offsetsOffset - this.fieldsSerializedSize, indexFile.createReader(rc), this.idxInfoSerializer);
      }

      public long unsharedHeapSize() {
         return BASE_SIZE;
      }

      public void serialize(DataOutputPlus out, IndexInfo.Serializer idxInfoSerializer, ByteBuffer indexInfo) throws IOException {
         out.writeUnsignedVInt(this.position);
         out.writeUnsignedVInt((long)(this.fieldsSerializedSize + indexInfo.limit()));
         out.writeUnsignedVInt(this.headerLength);
         DeletionTime.serializer.serialize(this.deletionTime, out);
         out.writeUnsignedVInt((long)this.columnsIndexCount);
         out.write(indexInfo);
      }

      static long deserializePositionAndSkip(DataInputPlus in) throws IOException {
         long position = in.readUnsignedVInt();
         int size = (int)in.readUnsignedVInt();
         if(size > 0) {
            in.skipBytesFully(size);
         }

         return position;
      }

      public void serializeForCache(DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt(this.position);
         out.writeByte(2);
         out.writeUnsignedVInt(this.indexFilePosition);
         out.writeUnsignedVInt(this.headerLength);
         DeletionTime.serializer.serialize(this.deletionTime, out);
         out.writeUnsignedVInt((long)this.columnsIndexCount);
         out.writeUnsignedVInt((long)this.indexedPartSize);
      }

      static void skipForCache(DataInputPlus in) throws IOException {
         in.readUnsignedVInt();
         in.readUnsignedVInt();
         DeletionTime.serializer.skip(in);
         in.readUnsignedVInt();
         in.readUnsignedVInt();
      }

      static {
         BASE_SIZE = ObjectSizes.measure(new BigRowIndexEntry.ShallowIndexedEntry(0L, 0L, DeletionTime.LIVE, 0L, 10, 0, (ISerializer)null));
      }
   }

   private static final class IndexedEntry extends BigRowIndexEntry {
      private static final long BASE_SIZE;
      private final DeletionTime deletionTime;
      private final long headerLength;
      private final IndexInfo[] columnsIndex;
      private final int[] offsets;
      private final int indexedPartSize;
      @Unmetered
      private final ISerializer<IndexInfo> idxInfoSerializer;

      private IndexedEntry(long dataFilePosition, DeletionTime deletionTime, long headerLength, IndexInfo[] columnsIndex, int[] offsets, int indexedPartSize, ISerializer<IndexInfo> idxInfoSerializer) {
         super(dataFilePosition);
         this.headerLength = headerLength;
         this.deletionTime = deletionTime;
         this.columnsIndex = columnsIndex;
         this.offsets = offsets;
         this.indexedPartSize = indexedPartSize;
         this.idxInfoSerializer = idxInfoSerializer;
      }

      private IndexedEntry(long dataFilePosition, DataInputPlus in, DeletionTime deletionTime, long headerLength, int columnIndexCount, IndexInfo.Serializer idxInfoSerializer, Version version, int indexedPartSize) throws IOException {
         super(dataFilePosition);
         this.headerLength = headerLength;
         this.deletionTime = deletionTime;
         int columnsIndexCount = columnIndexCount;
         this.columnsIndex = new IndexInfo[columnIndexCount];

         int i;
         for(i = 0; i < columnsIndexCount; ++i) {
            this.columnsIndex[i] = idxInfoSerializer.deserialize(in);
         }

         this.offsets = new int[this.columnsIndex.length];

         for(i = 0; i < this.offsets.length; ++i) {
            this.offsets[i] = in.readInt();
         }

         this.indexedPartSize = indexedPartSize;
         this.idxInfoSerializer = idxInfoSerializer;
      }

      private IndexedEntry(long dataFilePosition, DataInputPlus in, IndexInfo.Serializer idxInfoSerializer, Version version) throws IOException {
         super(dataFilePosition);
         this.headerLength = in.readUnsignedVInt();
         this.deletionTime = DeletionTime.serializer.deserialize(in);
         int columnsIndexCount = (int)in.readUnsignedVInt();
         TrackedDataInputPlus trackedIn = new TrackedDataInputPlus(in);
         this.columnsIndex = new IndexInfo[columnsIndexCount];

         for(int i = 0; i < columnsIndexCount; ++i) {
            this.columnsIndex[i] = idxInfoSerializer.deserialize(trackedIn);
         }

         this.offsets = null;
         this.indexedPartSize = (int)trackedIn.getBytesRead();
         this.idxInfoSerializer = idxInfoSerializer;
      }

      public boolean indexOnHeap() {
         return true;
      }

      public int rowIndexCount() {
         return this.columnsIndex.length;
      }

      public DeletionTime deletionTime() {
         return this.deletionTime;
      }

      public BigRowIndexEntry.IndexInfoRetriever openWithIndex(FileHandle indexFile, Rebufferer.ReaderConstraint rc) {
         indexEntrySizeHistogram.update(BigRowIndexEntry.serializedSize(this.deletionTime, this.headerLength, this.columnsIndex.length) + this.indexedPartSize);
         indexInfoCountHistogram.update(this.columnsIndex.length);
         return new BigRowIndexEntry.IndexInfoRetriever() {
            private int retrievals;

            public IndexInfo columnsIndex(int index) {
               ++this.retrievals;
               return IndexedEntry.this.columnsIndex[index];
            }

            public void close() {
               BigRowIndexEntry.indexInfoGetsHistogram.update(this.retrievals);
            }
         };
      }

      public long unsharedHeapSize() {
         long entrySize = 0L;
         IndexInfo[] var3 = this.columnsIndex;
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            IndexInfo idx = var3[var5];
            entrySize += idx.unsharedHeapSize();
         }

         return BASE_SIZE + entrySize + ObjectSizes.sizeOfReferenceArray(this.columnsIndex.length);
      }

      public void serialize(DataOutputPlus out, IndexInfo.Serializer idxInfoSerializer, ByteBuffer indexInfo) throws IOException {
         assert this.indexedPartSize != -2147483648;

         out.writeUnsignedVInt(this.position);
         out.writeUnsignedVInt((long)(BigRowIndexEntry.serializedSize(this.deletionTime, this.headerLength, this.columnsIndex.length) + this.indexedPartSize));
         out.writeUnsignedVInt(this.headerLength);
         DeletionTime.serializer.serialize(this.deletionTime, out);
         out.writeUnsignedVInt((long)this.columnsIndex.length);
         IndexInfo[] var4 = this.columnsIndex;
         int var5 = var4.length;

         int var6;
         for(var6 = 0; var6 < var5; ++var6) {
            IndexInfo info = var4[var6];
            idxInfoSerializer.serialize(info, out);
         }

         int[] var8 = this.offsets;
         var5 = var8.length;

         for(var6 = 0; var6 < var5; ++var6) {
            int offset = var8[var6];
            out.writeInt(offset);
         }

      }

      public void serializeForCache(DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt(this.position);
         out.writeByte(1);
         out.writeUnsignedVInt(this.headerLength);
         DeletionTime.serializer.serialize(this.deletionTime, out);
         out.writeUnsignedVInt((long)this.rowIndexCount());
         IndexInfo[] var2 = this.columnsIndex;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            IndexInfo indexInfo = var2[var4];
            this.idxInfoSerializer.serialize(indexInfo, out);
         }

      }

      static void skipForCache(DataInputPlus in) throws IOException {
         in.readUnsignedVInt();
         DeletionTime.serializer.skip(in);
         in.readUnsignedVInt();
         in.readUnsignedVInt();
      }

      static {
         BASE_SIZE = ObjectSizes.measure(new BigRowIndexEntry.IndexedEntry(0L, DeletionTime.LIVE, 0L, (IndexInfo[])null, (int[])null, 0, (ISerializer)null));
      }
   }

   public static final class Serializer implements BigRowIndexEntry.IndexSerializer {
      private final IndexInfo.Serializer idxInfoSerializer;
      private final Version version;

      public Serializer(Version version, SerializationHeader header) {
         this.idxInfoSerializer = IndexInfo.serializer(version, header);
         this.version = version;
      }

      public IndexInfo.Serializer indexInfoSerializer() {
         return this.idxInfoSerializer;
      }

      public void serialize(BigRowIndexEntry rie, DataOutputPlus out, ByteBuffer indexInfo) throws IOException {
         rie.serialize(out, this.idxInfoSerializer, indexInfo);
      }

      public void serializeForCache(BigRowIndexEntry rie, DataOutputPlus out) throws IOException {
         rie.serializeForCache(out);
      }

      public BigRowIndexEntry deserializeForCache(DataInputPlus in) throws IOException {
         long position = in.readUnsignedVInt();
         switch(in.readByte()) {
         case 0:
            return new BigRowIndexEntry(position);
         case 1:
            return new BigRowIndexEntry.IndexedEntry(position, in, this.idxInfoSerializer, this.version);
         case 2:
            return new BigRowIndexEntry.ShallowIndexedEntry(position, in, this.idxInfoSerializer);
         default:
            throw new AssertionError();
         }
      }

      public static void skipForCache(DataInputPlus in) throws IOException {
         in.readUnsignedVInt();
         switch(in.readByte()) {
         case 0:
            break;
         case 1:
            BigRowIndexEntry.IndexedEntry.skipForCache(in);
            break;
         case 2:
            BigRowIndexEntry.ShallowIndexedEntry.skipForCache(in);
            break;
         default:
            assert false;
         }

      }

      public BigRowIndexEntry deserialize(DataInputPlus in, long indexFilePosition) throws IOException {
         long position = in.readUnsignedVInt();
         int size = (int)in.readUnsignedVInt();
         if(size == 0) {
            return new BigRowIndexEntry(position);
         } else {
            long headerLength = in.readUnsignedVInt();
            DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
            int columnsIndexCount = (int)in.readUnsignedVInt();
            int indexedPartSize = size - BigRowIndexEntry.serializedSize(deletionTime, headerLength, columnsIndexCount);
            if(size <= DatabaseDescriptor.getColumnIndexCacheSize()) {
               return new BigRowIndexEntry.IndexedEntry(position, in, deletionTime, headerLength, columnsIndexCount, this.idxInfoSerializer, this.version, indexedPartSize);
            } else {
               in.skipBytes(indexedPartSize);
               return new BigRowIndexEntry.ShallowIndexedEntry(position, indexFilePosition, deletionTime, headerLength, columnsIndexCount, indexedPartSize, this.idxInfoSerializer);
            }
         }
      }

      public long deserializePositionAndSkip(DataInputPlus in) throws IOException {
         return BigRowIndexEntry.ShallowIndexedEntry.deserializePositionAndSkip(in);
      }

      public static long readPosition(DataInputPlus in) throws IOException {
         return in.readUnsignedVInt();
      }

      public static void skip(DataInputPlus in, Version version) throws IOException {
         readPosition(in);
         skipPromotedIndex(in);
      }

      private static void skipPromotedIndex(DataInputPlus in) throws IOException {
         int size = (int)in.readUnsignedVInt();
         if(size > 0) {
            in.skipBytesFully(size);
         }
      }

      public static void serializeOffsets(DataOutputBuffer out, int[] indexOffsets, int columnIndexCount) throws IOException {
         for(int i = 0; i < columnIndexCount; ++i) {
            out.writeInt(indexOffsets[i]);
         }

      }
   }

   public interface IndexSerializer {
      void serialize(BigRowIndexEntry var1, DataOutputPlus var2, ByteBuffer var3) throws IOException;

      BigRowIndexEntry deserialize(DataInputPlus var1, long var2) throws IOException;

      void serializeForCache(BigRowIndexEntry var1, DataOutputPlus var2) throws IOException;

      BigRowIndexEntry deserializeForCache(DataInputPlus var1) throws IOException;

      long deserializePositionAndSkip(DataInputPlus var1) throws IOException;

      ISerializer<IndexInfo> indexInfoSerializer();
   }
}
