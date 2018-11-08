package org.apache.cassandra.io.util;

public interface ReaderFileProxy extends AutoCloseable {
   void close();

   AsynchronousChannelProxy channel();

   long fileLength();

   double getCrcCheckChance();
}
