package org.apache.cassandra.io.util;

public class CorruptFileException extends RuntimeException {
   public final String filePath;

   public CorruptFileException(Throwable cause, String filePath) {
      super(cause);
      this.filePath = filePath;
   }
}
