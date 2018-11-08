package org.apache.cassandra.net;

import java.util.concurrent.CompletableFuture;

public interface VerbHandler<P, Q> {
   CompletableFuture<Response<Q>> handle(Request<P, Q> var1);
}
