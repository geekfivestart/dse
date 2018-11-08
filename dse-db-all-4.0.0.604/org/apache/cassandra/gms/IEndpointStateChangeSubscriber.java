package org.apache.cassandra.gms;

import java.net.InetAddress;

public interface IEndpointStateChangeSubscriber {
   void onJoin(InetAddress var1, EndpointState var2);

   void beforeChange(InetAddress var1, EndpointState var2, ApplicationState var3, VersionedValue var4);

   void onChange(InetAddress var1, ApplicationState var2, VersionedValue var3);

   void onAlive(InetAddress var1, EndpointState var2);

   void onDead(InetAddress var1, EndpointState var2);

   void onRemove(InetAddress var1);

   default void afterRemove(InetAddress endpoint) {
   }

   void onRestart(InetAddress var1, EndpointState var2);

   default void onStarted(InetAddress endpoint, boolean isNew, EndpointState state) {
   }
}
