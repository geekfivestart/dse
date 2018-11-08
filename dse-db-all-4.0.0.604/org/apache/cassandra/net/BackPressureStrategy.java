package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface BackPressureStrategy<S extends BackPressureState> {
   CompletableFuture<Void> apply(Set<S> var1, long var2, TimeUnit var4);

   S newState(InetAddress var1);
}
