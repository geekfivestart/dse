package org.apache.cassandra.io.sstable;

import java.io.File;

public class CorruptSSTableException extends RuntimeException {
   public final File path;

   public CorruptSSTableException(Throwable cause, File path) {
      super("Corrupted: " + path, cause);
      this.path = path;
   }

   public CorruptSSTableException(Throwable cause, String path) {
      this(cause, new File(path));
   }
}
