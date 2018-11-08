package org.apache.cassandra.concurrent;

public interface ExecutorSupplier<P> {
   TracingAwareExecutor get(P var1);
}
