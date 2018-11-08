package com.datastax.bdp.leasemanager;

import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@Singleton
@DsePlugin(
   dependsOn = {LeasePlugin.class}
)
public class SmallExclusiveTasksPlugin extends AbstractPlugin {
   public static final String LEASE_KEY = "dse_tasks";
   public static final int LEASE_DURATION_MS = 180000;
   @Inject
   LeasePlugin leasePlugin;
   InternalLeaseLeader leader;

   public SmallExclusiveTasksPlugin() {
   }

   public void onActivate() {
      this.leader = this.leasePlugin.getLeader("dse_tasks", EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress()), 180000, true);
   }

   public Future executeIfLeader(Runnable runnable, boolean abortOnLeaseExpiration, boolean testMode) {
      return this.leader.executeIfLeader(runnable, 18000L, abortOnLeaseExpiration, testMode);
   }

   public <T> Future<T> executeIfLeader(Callable<T> runnable, boolean abortOnLeaseExpiration, boolean testMode) {
      return this.leader.executeIfLeader(runnable, 18000L, abortOnLeaseExpiration, testMode);
   }
}
