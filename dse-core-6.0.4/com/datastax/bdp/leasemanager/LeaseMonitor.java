package com.datastax.bdp.leasemanager;

import com.codahale.metrics.Timer;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.LambdaMayThrow;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.beans.ConstructorProperties;
import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseMonitor implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(LeaseMonitor.class);
   private static final NoSpamLogger noSpamLogger;
   public static final long FIRST_EPOCH = 0L;
   public static final long DISABLED_EPOCH = -1L;
   public static final long NONEXISTANT_EPOCH = -2L;
   public static final String OP_RENEW = "Renew";
   public static final String OP_ACQUIRE = "Acquire";
   public static final String OP_RESOLVE = "Resolve";
   public static final String OP_DISABLE = "Disable";
   public final LeaseMonitorCore core;
   public final String name;
   public final String dc;
   public final int duration_ms;
   protected Supplier<Long> clock;
   protected final BiFunction<LeaseMonitor.BeliefState, Long, LeaseMonitor.BeliefState> onDeletion;
   protected volatile LeaseMonitor.BeliefState belief;
   private LeaseMonitorCore.LeaseRow lastRow;
   private ScheduledFuture task;
   protected long upOrDownSince;
   protected ImmutableMap<String, Timer> opLatency;

   LeaseMonitor(String name, String dc, int duration_ms, LeaseMonitorCore core, Supplier<Long> clock, Consumer<LeaseMonitor> onDeletion) {
      long now = ((Long)clock.get()).longValue();
      this.core = core;
      this.name = name;
      this.dc = dc;
      this.duration_ms = duration_ms;
      this.belief = new LeaseMonitor.BeliefState(0L, (InetAddress)null, now - 1L, now + (long)duration_ms, now - (long)duration_ms, now - 100000L);
      this.clock = clock;
      this.task = null;
      this.upOrDownSince = System.currentTimeMillis();
      this.opLatency = ImmutableMap.copyOf((Map)Lists.newArrayList(new String[]{"Renew", "Acquire", "Resolve", "Disable"}).stream().collect(Collectors.toMap(Function.identity(), (opName) -> {
         return CassandraMetricsRegistry.Metrics.timer(name + "." + dc + "." + opName);
      })));
      this.onDeletion = (b, stopTime) -> {
         onDeletion.accept(this);
         return new LeaseMonitor.BeliefState(-2L, (InetAddress)null, b.earliest, b.latest, b.lastSuccess, b.lastTimeout);
      };
   }

   public boolean isDisabled() {
      return this.belief.isDisabled();
   }

   public boolean exists() {
      return this.belief.exists();
   }

   public boolean isOpen(long time) {
      return this.belief.isOpen(time);
   }

   public boolean isHeld(long time) {
      return this.belief.isHeld(time);
   }

   public InetAddress getLeaseHolder() {
      return this.belief.holder;
   }

   public long getEpoch() {
      return this.belief.epoch;
   }

   public LeaseMonitor.ClientPingResult clientPing(InetAddress client, boolean takeIfOpen) {
      long startTime = ((Long)this.clock.get()).longValue();
      LeaseMonitor.BeliefState currentBelief = this.belief;
      if(currentBelief.isHeld(startTime) || currentBelief.holder != null && Objects.equals(client, currentBelief.holder)) {
         if(client.equals(currentBelief.holder) && (double)(currentBelief.earliest - startTime) < 0.95D * (double)this.duration_ms) {
            currentBelief = this.renew(startTime, currentBelief.epoch, client);
         }
      } else if(currentBelief.isOpen(startTime) && !currentBelief.isDisabled() && takeIfOpen) {
         currentBelief = this.acquire(startTime, currentBelief.epoch, client);
      }

      LeaseMonitor.ClientPingResult res = currentBelief.isHeld(startTime)?new LeaseMonitor.ClientPingResult(currentBelief.holder, currentBelief.earliest - startTime):new LeaseMonitor.ClientPingResult(Math.min(currentBelief.epoch, 0L));
      logger.debug("client ping of {}.{} yields {}", new Object[]{this.name, this.dc, res});
      return res;
   }

   public void run() {
      this.resolve();
   }

   public synchronized void start(ScheduledExecutorService executor, double rate) {
      if(this.task != null) {
         this.stop();
      }

      this.task = executor.scheduleAtFixedRate(this, 0L, Math.round(rate * (double)this.duration_ms), TimeUnit.MILLISECONDS);
   }

   public synchronized void stop() {
      if(this.task != null) {
         this.task.cancel(false);
         this.task = null;
      }

   }

   private synchronized LeaseMonitor.BeliefState coreOp(long startTime, Predicate<LeaseMonitor.BeliefState> check, LambdaMayThrow.SupplierMayThrow<Boolean> op, BiFunction<LeaseMonitor.BeliefState, Long, LeaseMonitor.BeliefState> onSuccess, BiFunction<LeaseMonitor.BeliefState, Long, LeaseMonitor.BeliefState> onFailure, String opType) {
      LeaseMonitor.BeliefState currentBelief = this.belief;
      if(check.test(currentBelief) && startTime - currentBelief.lastTimeout >= 1000L) {
         LeaseMonitor.BeliefState nextBelief;
         long stopTime;
         try {
            if(((Boolean)op.get()).booleanValue()) {
               nextBelief = (LeaseMonitor.BeliefState)onSuccess.apply(currentBelief, Long.valueOf(stopTime = ((Long)this.clock.get()).longValue()));
            } else {
               nextBelief = (LeaseMonitor.BeliefState)onFailure.apply(currentBelief, Long.valueOf(stopTime = ((Long)this.clock.get()).longValue()));
            }
         } catch (Exception var16) {
            stopTime = ((Long)this.clock.get()).longValue();
            nextBelief = new LeaseMonitor.BeliefState(currentBelief.epoch, currentBelief.holder, currentBelief.earliest, stopTime + (long)this.duration_ms, currentBelief.lastSuccess, stopTime);
            if(var16 instanceof UnavailableException && this.core.getLocalDcRf() == 0) {
               noSpamLogger.info("Suppressing UnavailableException for {}.{} as our replication factor has been changed to 0; is the datacenter being decommissioned?", new Object[]{this.name, this.dc});
               logger.trace("Suppressed exception", var16);
            } else if(EndpointStateTracker.instance.isActive(Addresses.Internode.getBroadcastAddress())) {
               noSpamLogger.warn(String.format("Lease LWT query failed; lease %s.%s will be unavailable until it cansucceed. Most likely this will be fixed by increasing the replication factor for the dse_leases keyspace in this DC (%s) to 3 and repairing the dse_leases keyspace. If not, something else is causing the queries to fail, such as nodes being overloaded or network connectivity issues. You may also see this warning briefly after increasing the replication factor, and before repair of the keyspace finishes.", new Object[]{this.name, this.dc, this.dc}), new Object[]{var16});
            }
         }

         long lastOperationTime = Math.max(currentBelief.lastSuccess, currentBelief.lastTimeout);

         try {
            if(currentBelief.isHeld(lastOperationTime) != nextBelief.isHeld(stopTime)) {
               this.upOrDownSince = System.currentTimeMillis();
               this.core.logMonitorBeliefChange(this.name, this.dc, Addresses.Internode.getBroadcastAddress(), this.upOrDownSince, currentBelief.isHeld(lastOperationTime)?currentBelief.holder:null, nextBelief.isHeld(stopTime)?nextBelief.holder:null);
            }
         } catch (Exception var15) {
            noSpamLogger.warn("Couldn't log lease operation", new Object[]{var15});
         }

         ((Timer)this.opLatency.get(opType)).update(stopTime - startTime, TimeUnit.MILLISECONDS);
         return this.belief = nextBelief;
      } else {
         return currentBelief;
      }
   }

   protected LeaseMonitor.BeliefState acquire(long startTime, long epoch, InetAddress client) {
      return this.coreOp(startTime, (b) -> {
         return b.epoch == epoch;
      }, () -> {
         return Boolean.valueOf(this.core.acquireLease(this.name, this.dc, client, epoch));
      }, (b, stopTime) -> {
         return new LeaseMonitor.BeliefState(epoch + 1L, client, startTime + (long)this.duration_ms, stopTime.longValue() + (long)this.duration_ms, startTime, b.lastTimeout);
      }, (b, stopTime) -> {
         return this.resolve();
      }, "Acquire");
   }

   protected LeaseMonitor.BeliefState renew(long startTime, long epoch, InetAddress client) {
      return this.coreOp(startTime, (b) -> {
         return b.epoch == epoch;
      }, () -> {
         return Boolean.valueOf(this.core.renewLease(this.name, this.dc, epoch));
      }, (b, stopTime) -> {
         return new LeaseMonitor.BeliefState(epoch + 1L, client, startTime + (long)this.duration_ms, stopTime.longValue() + (long)this.duration_ms, startTime, b.lastTimeout);
      }, (b, stopTime) -> {
         return this.resolve();
      }, "Renew");
   }

   protected LeaseMonitor.BeliefState resolve() {
      long startTime = ((Long)this.clock.get()).longValue();
      return this.coreOp(startTime, (b) -> {
         return true;
      }, () -> {
         return Boolean.valueOf((this.lastRow = this.core.readLease(this.name, this.dc)) != null);
      }, (b, stopTime) -> {
         return new LeaseMonitor.BeliefState(this.lastRow.epoch, this.lastRow.holder, b.epoch != this.lastRow.epoch?b.lastSuccess + (long)this.duration_ms:b.earliest, b.epoch != this.lastRow.epoch?stopTime.longValue() + (long)this.duration_ms:b.latest, startTime, b.lastTimeout);
      }, this.onDeletion, "Resolve");
   }

   protected boolean disable() {
      long startTime = ((Long)this.clock.get()).longValue();
      return this.coreOp(startTime, (b) -> {
         return true;
      }, () -> {
         return Boolean.valueOf(this.core.disableLease(this.name, this.dc));
      }, (b, stopTime) -> {
         return new LeaseMonitor.BeliefState(-1L, (InetAddress)null, 0L, b.latest, startTime, b.lastTimeout);
      }, this.onDeletion, "Disable").isDisabled();
   }

   public synchronized LeaseMetrics getMetrics() {
      return new LeaseMetrics(this);
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 3600000L, TimeUnit.MILLISECONDS);
   }

   public static class ClientPingResult {
      public final InetAddress holder;
      public final long leaseTimeRemaining;

      public ClientPingResult(long code) {
         this((InetAddress)null, code);

         assert code <= 0L;

      }

      public ClientPingResult(InetAddress holder, long leaseTimeRemaining) {
         this.holder = holder;
         this.leaseTimeRemaining = holder == null && leaseTimeRemaining > 0L?0L:leaseTimeRemaining;
      }

      @ConstructorProperties({"holder", "leaseTimeRemaining"})
      public ClientPingResult(String holder, long leaseTimeRemaining) throws Exception {
         this(holder == null?null:InetAddress.getByName(holder), leaseTimeRemaining);
      }

      public boolean isHeld() {
         return this.leaseTimeRemaining > 0L;
      }

      public boolean isOpen() {
         return this.holder == null && this.leaseTimeRemaining == 0L;
      }

      public boolean isDisabled() {
         return this.leaseTimeRemaining == -1L;
      }

      public boolean isDeleted() {
         return this.leaseTimeRemaining == -2L;
      }

      public boolean equals(Object o) {
         if(o != null && o instanceof LeaseMonitor.ClientPingResult) {
            LeaseMonitor.ClientPingResult other = (LeaseMonitor.ClientPingResult)o;
            return Objects.equals(this.holder, other.holder) && this.leaseTimeRemaining == other.leaseTimeRemaining;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.holder, Long.valueOf(this.leaseTimeRemaining)});
      }

      public String toString() {
         String prefix = "";
         if(this.isDeleted()) {
            prefix = "<Deleted> ";
         } else if(this.isDisabled()) {
            prefix = "<Disabled> ";
         } else if(this.isOpen()) {
            prefix = "<Open> ";
         }

         return String.format("%s%s for %dms", new Object[]{prefix, this.holder != null?this.holder.toString():null, Long.valueOf(this.leaseTimeRemaining)});
      }

      public String getHolder() {
         return this.holder == null?null:this.holder.getHostAddress();
      }

      public long getLeaseTimeRemaining() {
         return this.leaseTimeRemaining;
      }
   }

   protected static class BeliefState {
      final long epoch;
      final long earliest;
      final long latest;
      final long lastSuccess;
      final long lastTimeout;
      final InetAddress holder;

      BeliefState(long epoch, InetAddress holder, long earliest, long latest, long lastSuccess, long lastTimeout) {
         this.epoch = epoch;
         this.earliest = earliest;
         this.latest = latest;
         this.holder = holder;
         this.lastSuccess = lastSuccess;
         this.lastTimeout = lastTimeout;
      }

      public long stopTime() {
         return Math.max(this.lastSuccess, this.lastTimeout);
      }

      public boolean isDisabled() {
         return this.epoch == -1L;
      }

      public boolean exists() {
         return this.epoch != -2L;
      }

      public boolean isHeld(long time) {
         return time < this.earliest && this.holder != null && this.epoch >= 0L;
      }

      public boolean isOpen(long time) {
         return !this.exists()?false:(this.isDisabled()?time >= this.latest:this.holder == null || time >= this.latest);
      }

      public String toString() {
         return String.format("BeliefState(epoch=%d, addr=%s, earliest=%d, latest=%d, lastSuccess=%d, lastTimeout=%d", new Object[]{Long.valueOf(this.epoch), LeaseMonitorCore.formatInetAddress(this.holder), Long.valueOf(this.earliest), Long.valueOf(this.latest), Long.valueOf(this.lastSuccess), Long.valueOf(this.lastTimeout)});
      }
   }
}
