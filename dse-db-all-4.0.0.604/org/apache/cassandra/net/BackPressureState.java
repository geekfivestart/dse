package org.apache.cassandra.net;

import java.net.InetAddress;

public interface BackPressureState {
   void onRequestSent(Request<?, ?> var1);

   void onResponseReceived();

   void onResponseTimeout();

   double getBackPressureRateLimit();

   InetAddress getHost();
}
