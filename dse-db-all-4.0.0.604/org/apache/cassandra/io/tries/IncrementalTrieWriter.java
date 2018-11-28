package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteSource;

public interface IncrementalTrieWriter<Value> {
   void add(ByteSource var1, Value var2) throws IOException;

   long count();

   long complete() throws IOException;

   IncrementalTrieWriter.PartialTail makePartialRoot() throws IOException;

   public static <Value> IncrementalTrieWriter<Value> construct(TrieSerializer<Value, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest) {
      return new IncrementalTrieWriterPageAware(trieSerializer, dest);
   }

   public interface PartialTail {
      long root();

      long count();

      long cutoff();

      ByteBuffer tail();
   }
}
