package org.apache.cassandra.io.sstable.format.big;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSummary extends WrappedSharedCloseable {
   private static final Logger logger = LoggerFactory.getLogger(IndexSummary.class);
   public static final IndexSummary.IndexSummarySerializer serializer = new IndexSummary.IndexSummarySerializer();
   private final int minIndexInterval;
   private final IPartitioner partitioner;
   private final int sizeAtFullSampling;
   private final Memory offsets;
   private final int offsetCount;
   private final Memory entries;
   private final long entriesLength;
   private final int samplingLevel;

   public IndexSummary(IPartitioner partitioner, Memory offsets, int offsetCount, Memory entries, long entriesLength, int sizeAtFullSampling, int minIndexInterval, int samplingLevel) {
      super((AutoCloseable[])(new Memory[]{offsets, entries}));

      assert offsets.getInt(0L) == 0;

      this.partitioner = partitioner;
      this.minIndexInterval = minIndexInterval;
      this.offsetCount = offsetCount;
      this.entriesLength = entriesLength;
      this.sizeAtFullSampling = sizeAtFullSampling;
      this.offsets = offsets;
      this.entries = entries;
      this.samplingLevel = samplingLevel;

      assert samplingLevel > 0;

   }

   private IndexSummary(IndexSummary copy) {
      super((WrappedSharedCloseable)copy);
      this.partitioner = copy.partitioner;
      this.minIndexInterval = copy.minIndexInterval;
      this.offsetCount = copy.offsetCount;
      this.entriesLength = copy.entriesLength;
      this.sizeAtFullSampling = copy.sizeAtFullSampling;
      this.offsets = copy.offsets;
      this.entries = copy.entries;
      this.samplingLevel = copy.samplingLevel;
   }

   public int binarySearch(PartitionPosition key) {
      ByteBuffer hollow = UnsafeByteBufferAccess.getHollowDirectByteBuffer().order(ByteOrder.BIG_ENDIAN);
      int low = 0;
      int mid = this.offsetCount;
      int high = mid - 1;
      int result = -1;

      while(low <= high) {
         mid = low + high >> 1;
         this.fillTemporaryKey(mid, hollow);
         result = -DecoratedKey.compareTo(this.partitioner, hollow, key);
         if(result > 0) {
            low = mid + 1;
         } else {
            if(result == 0) {
               return mid;
            }

            high = mid - 1;
         }
      }

      return -mid - (result < 0?1:2);
   }

   public int getPositionInSummary(int index) {
      return this.offsets.getInt((long)(index << 2));
   }

   public byte[] getKey(int index) {
      long start = (long)this.getPositionInSummary(index);
      int keySize = (int)(this.calculateEnd(index) - start - 8L);
      byte[] key = new byte[keySize];
      this.entries.getBytes(start, key, 0, keySize);
      return key;
   }

   private void fillTemporaryKey(int index, ByteBuffer buffer) {
      long start = (long)this.getPositionInSummary(index);
      int keySize = (int)(this.calculateEnd(index) - start - 8L);
      this.entries.setByteBuffer(buffer, start, keySize);
   }

   public void addTo(Ref.IdentityCollection identities) {
      super.addTo(identities);
      identities.add(this.offsets);
      identities.add(this.entries);
   }

   public long getPosition(int index) {
      return this.entries.getLong(this.calculateEnd(index) - 8L);
   }

   public long getEndInSummary(int index) {
      return this.calculateEnd(index);
   }

   private long calculateEnd(int index) {
      return index == this.offsetCount - 1?this.entriesLength:(long)this.getPositionInSummary(index + 1);
   }

   public int getMinIndexInterval() {
      return this.minIndexInterval;
   }

   public double getEffectiveIndexInterval() {
      return 128.0D / (double)this.samplingLevel * (double)this.minIndexInterval;
   }

   public long getEstimatedKeyCount() {
      return ((long)this.getMaxNumberOfEntries() + 1L) * (long)this.minIndexInterval;
   }

   public int size() {
      return this.offsetCount;
   }

   public int getSamplingLevel() {
      return this.samplingLevel;
   }

   public int getMaxNumberOfEntries() {
      return this.sizeAtFullSampling;
   }

   long getEntriesLength() {
      return this.entriesLength;
   }

   Memory getOffsets() {
      return this.offsets;
   }

   Memory getEntries() {
      return this.entries;
   }

   public long getOffHeapSize() {
      return (long)(this.offsetCount * 4) + this.entriesLength;
   }

   public int getEffectiveIndexIntervalAfterIndex(int index) {
      return Downsampling.getEffectiveIndexIntervalAfterIndex(index, this.samplingLevel, this.minIndexInterval);
   }

   public IndexSummary sharedCopy() {
      return new IndexSummary(this);
   }

   public static class IndexSummarySerializer {
      public IndexSummarySerializer() {
      }

      public void serialize(IndexSummary t, DataOutputPlus out) throws IOException {
         out.writeInt(t.minIndexInterval);
         out.writeInt(t.offsetCount);
         out.writeLong(t.getOffHeapSize());
         out.writeInt(t.samplingLevel);
         out.writeInt(t.sizeAtFullSampling);
         int baseOffset = t.offsetCount * 4;

         for(int i = 0; i < t.offsetCount; ++i) {
            int offset = t.offsets.getInt((long)(i * 4)) + baseOffset;
            if(ByteOrder.nativeOrder() != ByteOrder.BIG_ENDIAN) {
               offset = Integer.reverseBytes(offset);
            }

            out.writeInt(offset);
         }

         out.write(t.entries, 0L, t.entriesLength);
      }

      public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner, int expectedMinIndexInterval, int maxIndexInterval) throws IOException {
         int minIndexInterval = in.readInt();
         if(minIndexInterval != expectedMinIndexInterval) {
            throw new IOException(String.format("Cannot read index summary because min_index_interval changed from %d to %d.", new Object[]{Integer.valueOf(minIndexInterval), Integer.valueOf(expectedMinIndexInterval)}));
         } else {
            int offsetCount = in.readInt();
            long offheapSize = in.readLong();
            int samplingLevel = in.readInt();
            int fullSamplingSummarySize = in.readInt();
            int effectiveIndexInterval = (int)Math.ceil(128.0D / (double)samplingLevel * (double)minIndexInterval);
            if(effectiveIndexInterval > maxIndexInterval) {
               throw new IOException(String.format("Rebuilding index summary because the effective index interval (%d) is higher than the current max index interval (%d)", new Object[]{Integer.valueOf(effectiveIndexInterval), Integer.valueOf(maxIndexInterval)}));
            } else {
               Memory offsets = Memory.allocate((long)(offsetCount * 4));
               Memory entries = Memory.allocate(offheapSize - offsets.size());

               try {
                  FBUtilities.copy(in, new MemoryOutputStream(offsets), offsets.size());
                  FBUtilities.copy(in, new MemoryOutputStream(entries), entries.size());
               } catch (IOException var15) {
                  offsets.free();
                  entries.free();
                  throw var15;
               }

               for(int i = 0; (long)i < offsets.size(); i += 4) {
                  offsets.setInt((long)i, (int)((long)offsets.getInt((long)i) - offsets.size()));
               }

               return new IndexSummary(partitioner, offsets, offsetCount, entries, entries.size(), fullSamplingSummarySize, minIndexInterval, samplingLevel);
            }
         }
      }

      public Pair<DecoratedKey, DecoratedKey> deserializeFirstLastKey(DataInputStream in, IPartitioner partitioner) throws IOException {
         in.skipBytes(4);
         int offsetCount = in.readInt();
         long offheapSize = in.readLong();
         in.skipBytes(8);
         in.skip((long)(offsetCount * 4));
         in.skip(offheapSize - (long)(offsetCount * 4));
         DecoratedKey first = partitioner.decorateKey(ByteBufferUtil.readWithLength(in));
         DecoratedKey last = partitioner.decorateKey(ByteBufferUtil.readWithLength(in));
         return Pair.create(first, last);
      }
   }
}
