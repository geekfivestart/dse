package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.utils.SyncUtil;

public class CompressedSegment extends FileDirectSegment {
   static final int COMPRESSED_MARKER_SIZE = 12;
   final ICompressor compressor;

   CompressedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      super(commitLog, manager);
      this.compressor = commitLog.configuration.getCompressor();
      manager.getBufferPool().setPreferredReusableBufferType(this.compressor.preferredBufferType());
   }

   ByteBuffer createBuffer(CommitLog commitLog) {
      return this.manager.getBufferPool().createBuffer(commitLog.configuration.getCompressor().preferredBufferType());
   }

   synchronized void write(int startMarker, int nextMarker) {
      int contentStart = startMarker + 8;
      int length = nextMarker - contentStart;

      assert length > 0 || length == 0 && !this.isStillAllocating();

      try {
         int neededBufferSize = this.compressor.initialCompressedBufferLength(length) + 12;
         ByteBuffer compressedBuffer = this.manager.getBufferPool().getThreadLocalReusableBuffer(neededBufferSize);
         ByteBuffer inputBuffer = this.buffer.duplicate();
         inputBuffer.limit(contentStart + length).position(contentStart);
         compressedBuffer.limit(compressedBuffer.capacity()).position(12);
         this.compressor.compress(inputBuffer, compressedBuffer);
         compressedBuffer.flip();
         compressedBuffer.putInt(8, length);
         writeSyncMarker(this.id, compressedBuffer, 0, (int)this.channel.position(), (int)this.channel.position() + compressedBuffer.remaining());
         this.manager.addSize((long)compressedBuffer.limit());
         this.channel.write(compressedBuffer);

         assert this.channel.position() - this.lastWrittenPos == (long)compressedBuffer.limit();

         this.lastWrittenPos = this.channel.position();
         SyncUtil.force(this.channel, true);
      } catch (Exception var8) {
         throw new FSWriteError(var8, this.getPath());
      }
   }

   public long onDiskSize() {
      return this.lastWrittenPos;
   }
}
