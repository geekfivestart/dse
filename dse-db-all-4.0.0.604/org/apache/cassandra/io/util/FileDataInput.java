package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.IOException;

public interface FileDataInput extends RewindableDataInput, Closeable {
   String getPath();

   boolean isEOF() throws IOException;

   long bytesRemaining() throws IOException;

   void seek(long var1) throws IOException;

   long getFilePointer();
}
