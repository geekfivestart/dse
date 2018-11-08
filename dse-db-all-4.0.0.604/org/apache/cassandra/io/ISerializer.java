package org.apache.cassandra.io;

import java.io.IOException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface ISerializer<T> {
   void serialize(T var1, DataOutputPlus var2) throws IOException;

   T deserialize(DataInputPlus var1) throws IOException;

   long serializedSize(T var1);

   default void skip(DataInputPlus in) throws IOException {
      this.deserialize(in);
   }
}
