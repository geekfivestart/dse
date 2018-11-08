package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.FSWriteError;

public abstract class FileDirectSegment extends CommitLogSegment {
   volatile long lastWrittenPos = 0L;

   FileDirectSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      super(commitLog, manager);
   }

   void writeLogHeader() {
      super.writeLogHeader();

      try {
         this.channel.write((ByteBuffer)this.buffer.duplicate().flip());
         this.manager.addSize(this.lastWrittenPos = (long)this.buffer.position());
      } catch (IOException var2) {
         throw new FSWriteError(var2, this.getPath());
      }
   }

   protected void internalClose() {
      try {
         this.manager.getBufferPool().releaseBuffer(this.buffer);
         super.internalClose();
      } finally {
         this.manager.notifyBufferFreed();
      }

   }
}
