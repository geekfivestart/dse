package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionIndexBuilder implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(PartitionIndexBuilder.class);
   private long dataSyncPosition;
   private long rowIndexSyncPosition;
   private long partitionIndexSyncPosition;
   private IncrementalTrieWriter.PartialTail partialIndexTail;
   private Consumer<PartitionIndex> partialIndexConsumer;
   private long partialIndexDataEnd;
   private long partialIndexRowEnd;
   private long partialIndexPartitionEnd;
   private DecoratedKey partialIndexLastKey;
   private final IncrementalTrieWriter<PartitionIndex.Payload> trieWriter;
   SequentialWriter writer;
   private DecoratedKey firstKey;
   private DecoratedKey lastKey;
   private DecoratedKey lastWrittenKey;
   private PartitionIndex.Payload lastPayload;
   private int lastDiffPoint;
   final FileHandle.Builder fhBuilder;

   public PartitionIndexBuilder(SequentialWriter writer, FileHandle.Builder fhBuilder) {
      this.writer = writer;
      this.trieWriter = IncrementalTrieWriter.construct(PartitionIndex.trieSerializer, writer);
      this.fhBuilder = fhBuilder;
   }

   public void markPartitionIndexSynced(long upToPosition) {
      this.partitionIndexSyncPosition = upToPosition;
      this.refreshReadableBoundary();
   }

   public void markRowIndexSynced(long upToPosition) {
      this.rowIndexSyncPosition = upToPosition;
      this.refreshReadableBoundary();
   }

   public void markDataSynced(long upToPosition) {
      this.dataSyncPosition = upToPosition;
      this.refreshReadableBoundary();
   }

   private void refreshReadableBoundary() {
      if(this.partialIndexConsumer != null) {
         if(this.dataSyncPosition >= this.partialIndexDataEnd) {
            if(this.rowIndexSyncPosition >= this.partialIndexRowEnd) {
               if(this.partitionIndexSyncPosition >= this.partialIndexPartitionEnd) {
                  FileHandle fh = this.fhBuilder.complete();
                  Throwable var2 = null;

                  try {
                     PartitionIndex pi = new PartitionIndexEarly(fh, this.partialIndexTail.root(), this.partialIndexTail.count(), this.firstKey, this.partialIndexLastKey, this.partialIndexTail.cutoff(), this.partialIndexTail.tail());
                     this.partialIndexConsumer.accept(pi);
                     this.partialIndexConsumer = null;
                  } catch (Throwable var11) {
                     var2 = var11;
                     throw var11;
                  } finally {
                     if(fh != null) {
                        if(var2 != null) {
                           try {
                              fh.close();
                           } catch (Throwable var10) {
                              var2.addSuppressed(var10);
                           }
                        } else {
                           fh.close();
                        }
                     }

                  }

               }
            }
         }
      }
   }

   public PartitionIndexBuilder addEntry(DecoratedKey decoratedKey, long position) throws IOException {
      if(this.lastKey == null) {
         this.firstKey = decoratedKey;
         this.lastDiffPoint = 0;
      } else {
         ByteSource current = PartitionIndex.source(decoratedKey);
         ByteSource prev = PartitionIndex.source(this.lastKey);
         int diffPoint = ByteSource.diffPoint(prev, current);
         ByteSource prevPrefix = ByteSource.cut(prev, Math.max(diffPoint, this.lastDiffPoint));
         this.trieWriter.add(prevPrefix, this.lastPayload);
         this.lastWrittenKey = this.lastKey;
         this.lastDiffPoint = diffPoint;
      }

      this.lastKey = decoratedKey;
      this.lastPayload = new PartitionIndex.Payload(position, decoratedKey.filterHashLowerBits());
      return this;
   }

   public long complete() throws IOException {
      this.partialIndexConsumer = null;
      if(this.lastKey != this.lastWrittenKey) {
         ByteSource prev = PartitionIndex.source(this.lastKey);
         ByteSource prevPrefix = ByteSource.cut(prev, this.lastDiffPoint);
         this.trieWriter.add(prevPrefix, this.lastPayload);
      }

      long root = this.trieWriter.complete();
      long firstKeyPos = this.writer.position();
      if(this.firstKey != null) {
         ByteBufferUtil.writeWithShortLength((ByteBuffer)this.firstKey.getKey(), (DataOutputPlus)this.writer);
         ByteBufferUtil.writeWithShortLength((ByteBuffer)this.lastKey.getKey(), (DataOutputPlus)this.writer);
      } else {
         assert this.lastKey == null;

         this.writer.writeShort(0);
         this.writer.writeShort(0);
      }

      this.writer.writeLong(firstKeyPos);
      this.writer.writeLong(this.trieWriter.count());
      this.writer.writeLong(root);
      this.writer.sync();
      return root;
   }

   public boolean buildPartial(Consumer<PartitionIndex> callWhenReady, long rowIndexEnd, long dataEnd) {
      if(this.lastWrittenKey == this.partialIndexLastKey) {
         return false;
      } else if(this.partialIndexConsumer != null) {
         return false;
      } else {
         try {
            this.partialIndexTail = this.trieWriter.makePartialRoot();
            this.partialIndexDataEnd = dataEnd;
            this.partialIndexRowEnd = rowIndexEnd;
            this.partialIndexPartitionEnd = this.writer.position();
            this.partialIndexLastKey = this.lastWrittenKey;
            this.partialIndexConsumer = callWhenReady;
            return true;
         } catch (IOException var7) {
            throw new AssertionError(var7);
         }
      }
   }

   public void close() {
   }
}
