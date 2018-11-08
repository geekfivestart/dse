package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public interface DynamicEndpointSnitchMBean {
   Map<InetAddress, Double> getScores();

   int getUpdateInterval();

   int getResetInterval();

   double getBadnessThreshold();

   String getSubsnitchClassName();

   List<Double> dumpTimings(String var1) throws UnknownHostException;

   void setSeverity(double var1);

   double getSeverity();
}
