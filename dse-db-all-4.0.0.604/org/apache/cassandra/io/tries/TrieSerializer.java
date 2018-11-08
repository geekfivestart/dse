package org.apache.cassandra.io.tries;

import java.io.IOException;

public interface TrieSerializer<Value, Dest> {
   int sizeofNode(SerializationNode<Value> var1, long var2);

   void write(Dest var1, SerializationNode<Value> var2, long var3) throws IOException;
}
