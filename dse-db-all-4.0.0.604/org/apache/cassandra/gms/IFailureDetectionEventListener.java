package org.apache.cassandra.gms;

import java.net.InetAddress;

public interface IFailureDetectionEventListener {
   void convict(InetAddress var1, double var2);
}
