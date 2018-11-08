package org.apache.cassandra.concurrent;

import java.util.concurrent.ExecutorService;

public interface TracingAwareExecutorService extends ExecutorService, TracingAwareExecutor {
}
