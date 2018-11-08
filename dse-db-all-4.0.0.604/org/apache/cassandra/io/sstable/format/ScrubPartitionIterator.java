package org.apache.cassandra.io.sstable.format;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ScrubPartitionIterator extends Closeable {
   ByteBuffer key();

   long dataPosition();

   void advance() throws IOException;

   void close();
}
