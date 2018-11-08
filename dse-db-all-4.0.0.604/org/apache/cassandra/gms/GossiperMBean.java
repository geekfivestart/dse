package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.List;

public interface GossiperMBean {
   long getEndpointDowntime(String var1) throws UnknownHostException;

   int getCurrentGenerationNumber(String var1) throws UnknownHostException;

   void unsafeAssassinateEndpoint(String var1) throws UnknownHostException;

   void assassinateEndpoint(String var1) throws UnknownHostException;

   void reviveEndpoint(String var1) throws UnknownHostException;

   void unsafeSetEndpointState(String var1, String var2) throws UnknownHostException;

   double getSeedGossipProbability();

   void setSeedGossipProbability(double var1);

   List<String> reloadSeeds();

   List<String> getSeeds();
}
