package org.apache.cassandra.net.interceptors;

import org.apache.cassandra.net.Message;

public interface Interceptor {
   <M extends Message<?>> void intercept(M var1, InterceptionContext<M> var2);
}
