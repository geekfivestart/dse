package org.apache.cassandra.gms;

import java.net.InetAddress;

public interface IFailureDetector {
   boolean isAlive(InetAddress var1);

   void interpret(InetAddress var1);

   void report(InetAddress var1);

   void remove(InetAddress var1);

   void forceConviction(InetAddress var1);

   void registerFailureDetectionEventListener(IFailureDetectionEventListener var1);

   void unregisterFailureDetectionEventListener(IFailureDetectionEventListener var1);
}
