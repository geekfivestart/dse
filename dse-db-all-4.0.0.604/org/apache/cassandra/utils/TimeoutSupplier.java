package org.apache.cassandra.utils;

public interface TimeoutSupplier<P> {
   long get(P var1);
}
