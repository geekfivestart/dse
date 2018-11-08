package org.apache.cassandra.metrics;

public interface Reservoir extends com.codahale.metrics.Reservoir {
   long getCount();
}
