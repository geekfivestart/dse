package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import org.apache.cassandra.io.compress.ICompressor;

public class CompressedHintsWriter extends HintsWriter {
   static final int METADATA_SIZE = 8;
   private final ICompressor compressor;
   private volatile ByteBuffer compressionBuffer = null;

   public CompressedHintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC) {
      super(directory, descriptor, file, channel, fd, globalCRC);
      this.compressor = descriptor.createCompressor();

      assert this.compressor != null;

   }

   protected void writeBuffer(ByteBuffer bb) throws IOException {
      int originalSize = bb.remaining();
      int estimatedSize = this.compressor.initialCompressedBufferLength(originalSize) + 8;
      if(this.compressionBuffer == null || this.compressionBuffer.capacity() < estimatedSize) {
         this.compressionBuffer = this.compressor.preferredBufferType().allocate(estimatedSize);
      }

      this.compressionBuffer.clear();
      this.compressionBuffer.position(8);
      this.compressor.compress(bb, this.compressionBuffer);
      int compressedSize = this.compressionBuffer.position() - 8;
      this.compressionBuffer.rewind();
      this.compressionBuffer.putInt(originalSize);
      this.compressionBuffer.putInt(compressedSize);
      this.compressionBuffer.rewind();
      this.compressionBuffer.limit(compressedSize + 8);
      super.writeBuffer(this.compressionBuffer);
   }

   @VisibleForTesting
   ICompressor getCompressor() {
      return this.compressor;
   }
}
