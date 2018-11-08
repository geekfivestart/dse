package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.SyncUtil;

public class UncompressedSegment extends FileDirectSegment {
   UncompressedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      super(commitLog, manager);
      manager.getBufferPool().setPreferredReusableBufferType(BufferType.OFF_HEAP);
   }

   ByteBuffer createBuffer(CommitLog commitLog) {
      return this.manager.getBufferPool().createBuffer(BufferType.OFF_HEAP);
   }

   synchronized void write(int startMarker, int nextMarker) {
      int contentStart = startMarker + 8;
      int length = nextMarker - contentStart;

      assert length > 0 || length == 0 && !this.isStillAllocating();

      try {
         writeSyncMarker(this.id, this.buffer, startMarker, startMarker, nextMarker);
         ByteBuffer inputBuffer = this.buffer.duplicate();
         inputBuffer.limit(nextMarker).position(startMarker);
         this.manager.addSize((long)inputBuffer.remaining());
         this.channel.write(inputBuffer);
         this.lastWrittenPos = (long)nextMarker;

         assert this.channel.position() == (long)nextMarker;

         SyncUtil.force(this.channel, true);
      } catch (Exception var6) {
         throw new FSWriteError(var6, this.getPath());
      }
   }

   public long onDiskSize() {
      return this.lastWrittenPos;
   }
}
