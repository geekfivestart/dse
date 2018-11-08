package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PartitionIterator implements PartitionIndexIterator {
   private final FileHandle ifile;
   private final RandomAccessReader reader;
   private final IPartitioner partitioner;
   private final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
   private final Version version;
   private final PartitionPosition limit;
   private final int exclusiveLimit;
   private DecoratedKey key;
   private RowIndexEntry entry;
   private long dataPosition;

   public PartitionIterator(FileHandle ifile, IPartitioner partitioner, BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer, Version version) throws IOException {
      this.ifile = ifile.sharedCopy();
      this.reader = this.ifile.createReader();
      this.partitioner = partitioner;
      this.rowIndexEntrySerializer = rowIndexEntrySerializer;
      this.limit = null;
      this.exclusiveLimit = 0;
      this.version = version;
      this.advance();
   }

   public PartitionIterator(BigTableReader sstable) throws IOException {
      this(sstable.ifile, sstable.getPartitioner(), sstable.rowIndexEntrySerializer, sstable.descriptor.version);
   }

   PartitionIterator(BigTableReader sstable, PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight) throws IOException {
      this.limit = right;
      this.exclusiveLimit = exclusiveRight;
      this.ifile = sstable.ifile.sharedCopy();
      this.reader = this.ifile.createReader();
      this.rowIndexEntrySerializer = sstable.rowIndexEntrySerializer;
      this.partitioner = sstable.getPartitioner();
      this.version = sstable.descriptor.version;
      this.seekTo(sstable, left, inclusiveLeft);
   }

   public void close() {
      this.reader.close();
      this.ifile.close();
   }

   private void seekTo(BigTableReader sstable, PartitionPosition left, int inclusiveLeft) throws IOException {
      this.reader.seek(sstable.getIndexScanPosition(left));
      this.entry = null;
      this.dataPosition = -1L;

      while(!this.reader.isEOF()) {
         DecoratedKey indexDecoratedKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.reader));
         if(indexDecoratedKey.compareTo(left) > inclusiveLeft) {
            if(indexDecoratedKey.compareTo(this.limit) <= this.exclusiveLimit) {
               this.key = indexDecoratedKey;
               return;
            }
            break;
         }

         BigRowIndexEntry.Serializer.skip(this.reader, this.version);
      }

      this.key = null;
   }

   public void advance() throws IOException {
      if(this.entry == null && this.key != null && !this.reader.isEOF()) {
         BigRowIndexEntry.Serializer.skip(this.reader, this.version);
      }

      this.entry = null;
      this.dataPosition = -1L;
      if(!this.reader.isEOF()) {
         DecoratedKey indexDecoratedKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.reader));
         if(this.limit == null || indexDecoratedKey.compareTo(this.limit) <= this.exclusiveLimit) {
            this.key = indexDecoratedKey;
            return;
         }
      }

      this.key = null;
      this.entry = null;
   }

   public DecoratedKey key() {
      return this.key;
   }

   public RowIndexEntry entry() throws IOException {
      if(this.entry == null) {
         if(this.key == null) {
            return null;
         }

         assert this.rowIndexEntrySerializer != null : "Cannot use entry() without specifying rowIndexSerializer";

         this.entry = this.rowIndexEntrySerializer.deserialize(this.reader, this.reader.getFilePointer());
         this.dataPosition = this.entry.position;
      }

      return this.entry;
   }

   public long dataPosition() throws IOException {
      if(this.dataPosition == -1L) {
         if(this.key == null) {
            return -1L;
         }

         long pos = this.reader.getFilePointer();
         this.dataPosition = BigRowIndexEntry.Serializer.readPosition(this.reader);
         this.reader.seek(pos);
      }

      return this.dataPosition;
   }
}
