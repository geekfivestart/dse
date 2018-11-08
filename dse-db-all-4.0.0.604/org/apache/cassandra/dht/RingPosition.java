package org.apache.cassandra.dht;

public interface RingPosition<C extends RingPosition<C>> extends Comparable<C> {
   Token getToken();

   IPartitioner getPartitioner();

   boolean isMinimum();

   C minValue();
}
