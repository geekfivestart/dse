package org.apache.cassandra.io.tries;

public interface SerializationNode<Value> {
   int childCount();

   Value payload();

   int transition(int var1);

   long serializedPositionDelta(int var1, long var2);

   long maxPositionDelta(long var1);
}
