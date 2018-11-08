package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.Map;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface FailureDetectorMBean {
   void dumpInterArrivalTimes();

   void setPhiConvictThreshold(double var1);

   double getPhiConvictThreshold();

   String getAllEndpointStates();

   String getEndpointState(String var1) throws UnknownHostException;

   Map<String, String> getSimpleStates();

   int getDownEndpointCount();

   int getUpEndpointCount();

   TabularData getPhiValues() throws OpenDataException;
}
