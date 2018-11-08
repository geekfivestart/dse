package org.apache.cassandra.net;

import java.net.InetAddress;

public interface MessageCallback<Q> {
   void onResponse(Response<Q> var1);

   void onFailure(FailureResponse<Q> var1);

   default void onTimeout(InetAddress host) {
   }
}
