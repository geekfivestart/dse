package org.apache.cassandra.io.compress;

import java.io.IOException;

public class CorruptBlockException extends IOException {
   public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk) {
      this(filePath, chunk, (Throwable)null);
   }

   public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk, Throwable cause) {
      this(filePath, chunk.offset, chunk.length, cause);
   }

   public CorruptBlockException(String filePath, long offset, int length) {
      this(filePath, offset, length, (Throwable)null);
   }

   public CorruptBlockException(String filePath, long offset, int length, Throwable cause) {
      super(String.format("(%s): corruption detected, chunk at %d of length %d.", new Object[]{filePath, Long.valueOf(offset), Integer.valueOf(length)}), cause);
   }
}
