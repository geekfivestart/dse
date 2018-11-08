package org.apache.cassandra.service;

import java.net.InetAddress;

public interface IEndpointLifecycleSubscriber {
   void onJoinCluster(InetAddress var1);

   void onLeaveCluster(InetAddress var1);

   void onUp(InetAddress var1);

   void onDown(InetAddress var1);

   void onMove(InetAddress var1);
}
