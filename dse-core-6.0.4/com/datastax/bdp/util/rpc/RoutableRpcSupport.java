package com.datastax.bdp.util.rpc;

import com.datastax.bdp.util.LazyRef;
import java.net.InetAddress;

public interface RoutableRpcSupport {
   InetAddress getEndpoint(String var1, LazyRef<Object[]> var2);
}
