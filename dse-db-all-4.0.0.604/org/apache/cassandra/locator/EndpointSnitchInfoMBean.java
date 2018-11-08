package org.apache.cassandra.locator;

import java.net.UnknownHostException;

public interface EndpointSnitchInfoMBean {
   String getRack(String var1) throws UnknownHostException;

   String getDatacenter(String var1) throws UnknownHostException;

   String getRack();

   String getDatacenter();

   String getSnitchName();

   String getDisplayName();

   boolean isDynamicSnitch();
}
