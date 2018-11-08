package com.datastax.bdp.leasemanager;

import com.datastax.bdp.plugin.IPlugin;
import java.util.Map;
import java.util.Set;

public interface LeaseMXBean {
   Map<String, Boolean> getLeaseStatus(String var1, String var2) throws IPlugin.PluginNotActiveException;

   Map<LeaseMonitorCore.LeaseId, Map<String, Boolean>> getAllLeasesStatus() throws Exception;

   Set<LeaseMonitorCore.LeaseRow> getAllLeases() throws Exception;

   Set<LeaseMonitorCore.LeaseId> getLeasesOfNonexistentDatacenters() throws Exception;

   Set<LeaseMonitorCore.LeaseId> getSparkMastersOfNonAnalyticsDcs() throws Exception;

   boolean cleanupDeadLease(String var1, String var2) throws Exception;

   LeaseMonitor.ClientPingResult clientPing(String var1, String var2) throws Exception;

   boolean createLease(String var1, String var2, int var3) throws Exception;

   boolean disableLease(String var1, String var2) throws Exception;

   boolean deleteLease(String var1, String var2) throws Exception;

   int getLeaseDuration(String var1, String var2) throws Exception;
}
