package com.datastax.bdp.leasemanager;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.bdp.reporting.CqlWritable;
import com.datastax.bdp.util.Addresses;
import java.net.InetAddress;
import java.util.Date;

public class LeaseMetrics implements CqlWritable {
   public final String name;
   public final String dc;
   public final InetAddress monitor;
   public final boolean up;
   public final Date upOrDownSince;
   public final long renewAverageLatencyMs;
   public final long renewLatency99ms;
   public final long renewMaxLatencyMs;
   public final double renewRate15;
   public final long acquireAverageLatencyMs;
   public final long acquireLatency99ms;
   public final long acquireMaxLatencyMs;
   public final double acquireRate15;
   public final long resolveAverageLatencyMs;
   public final long resolveLatency99ms;
   public final long resolveMaxLatencyMs;
   public final double resolveRate15;
   public final long disableAverageLatencyMs;
   public final long disableLatency99ms;
   public final long disableMaxLatencyMs;
   public final double disableRate15;

   LeaseMetrics(LeaseMonitor monitor) {
      Snapshot renewSnapshot = ((Timer)monitor.opLatency.get("Renew")).getSnapshot();
      Snapshot acquireSnapshot = ((Timer)monitor.opLatency.get("Acquire")).getSnapshot();
      Snapshot resolveSnapshot = ((Timer)monitor.opLatency.get("Resolve")).getSnapshot();
      Snapshot disableSnapshot = ((Timer)monitor.opLatency.get("Disable")).getSnapshot();
      this.name = monitor.name;
      this.dc = monitor.dc;
      this.monitor = Addresses.Internode.getBroadcastAddress();
      this.upOrDownSince = new Date(monitor.upOrDownSince);
      this.up = monitor.isHeld(monitor.belief.stopTime());
      this.renewAverageLatencyMs = Math.round(renewSnapshot.getMean() / 1000000.0D);
      this.renewMaxLatencyMs = Math.round((double)renewSnapshot.getMax() / 1000000.0D);
      this.renewLatency99ms = Math.round(renewSnapshot.get99thPercentile() / 1000000.0D);
      this.renewRate15 = (double)Math.round(((Timer)monitor.opLatency.get("Renew")).getFifteenMinuteRate());
      this.acquireAverageLatencyMs = Math.round(acquireSnapshot.getMean() / 1000000.0D);
      this.acquireMaxLatencyMs = Math.round((double)acquireSnapshot.getMax() / 1000000.0D);
      this.acquireLatency99ms = Math.round(acquireSnapshot.get99thPercentile() / 1000000.0D);
      this.acquireRate15 = (double)Math.round(((Timer)monitor.opLatency.get("Acquire")).getFifteenMinuteRate());
      this.resolveAverageLatencyMs = Math.round(resolveSnapshot.getMean() / 1000000.0D);
      this.resolveMaxLatencyMs = Math.round((double)resolveSnapshot.getMax() / 1000000.0D);
      this.resolveLatency99ms = Math.round(resolveSnapshot.get99thPercentile() / 1000000.0D);
      this.resolveRate15 = (double)Math.round(((Timer)monitor.opLatency.get("Resolve")).getFifteenMinuteRate());
      this.disableAverageLatencyMs = Math.round(disableSnapshot.getMean() / 1000000.0D);
      this.disableMaxLatencyMs = Math.round((double)disableSnapshot.getMax() / 1000000.0D);
      this.disableLatency99ms = Math.round(disableSnapshot.get99thPercentile() / 1000000.0D);
      this.disableRate15 = (double)Math.round(((Timer)monitor.opLatency.get("Disable")).getFifteenMinuteRate());
   }

   public String toString() {
      return String.format("%s.%s.%s: up: %s since: %s", new Object[]{this.name, this.dc, this.monitor.getHostAddress(), this.up?"YES":"NO", this.upOrDownSince.toString()});
   }
}
