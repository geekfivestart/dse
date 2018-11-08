package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SyncUtil;

public class MemoryMappedSegment extends CommitLogSegment {
   MemoryMappedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      super(commitLog, manager);
      int firstSync = this.buffer.position();
      this.buffer.putInt(firstSync + 0, 0);
      this.buffer.putInt(firstSync + 4, 0);
   }

   ByteBuffer createBuffer(CommitLog commitLog) {
      try {
         MappedByteBuffer mappedFile = this.channel.map(MapMode.READ_WRITE, 0L, (long)DatabaseDescriptor.getCommitLogSegmentSize());
         this.manager.addSize((long)DatabaseDescriptor.getCommitLogSegmentSize());
         return mappedFile;
      } catch (IOException var3) {
         throw new FSWriteError(var3, this.logFile);
      }
   }

   void write(int startMarker, int nextMarker) {
      if(nextMarker <= this.buffer.capacity() - 8) {
         this.buffer.putInt(nextMarker, 0);
         this.buffer.putInt(nextMarker + 4, 0);
      }

      writeSyncMarker(this.id, this.buffer, startMarker, startMarker, nextMarker);

      try {
         SyncUtil.force((MappedByteBuffer)this.buffer);
      } catch (Exception var4) {
         throw new FSWriteError(var4, this.getPath());
      }

      NativeLibrary.trySkipCache(this.fd, (long)startMarker, nextMarker, this.logFile.getAbsolutePath());
   }

   public long onDiskSize() {
      return (long)DatabaseDescriptor.getCommitLogSegmentSize();
   }

   protected void internalClose() {
      if(FileUtils.isCleanerAvailable) {
         FileUtils.clean(this.buffer);
      }

      super.internalClose();
   }
}
