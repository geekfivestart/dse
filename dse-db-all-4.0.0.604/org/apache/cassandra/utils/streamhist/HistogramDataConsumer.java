package org.apache.cassandra.utils.streamhist;

public interface HistogramDataConsumer<T extends Exception> {
   void consume(int var1, int var2) throws T;
}
