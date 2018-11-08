package com.datastax.bdp.leasemanager;

import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.LambdaMayThrow;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseManager implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(LeaseMonitor.class);
   private static final NoSpamLogger noSpamLogger;
   public final LeaseMonitorCore core;
   private final ScheduledExecutorService executor;
   protected Supplier<Long> clock;
   protected final ConcurrentHashMap<LeaseMonitorCore.LeaseId, LeaseMonitor> monitors;
   private ScheduledFuture task;

   protected LeaseManager(String keyspace, String table, String logTable, ScheduledExecutorService executor, Supplier<Long> clock) throws Exception {
      this.monitors = new ConcurrentHashMap();
      this.core = new LeaseMonitorCore(keyspace, table, logTable);
      this.executor = executor;
      this.clock = clock;
      this.task = null;
   }

   public LeaseManager(String keyspace, String table, String logTable, ScheduledExecutorService executor) throws Exception {
      this(keyspace, table, logTable, executor, () -> {
         return Long.valueOf(LeasePlugin.clock());
      });
   }

   public LeaseManager(ScheduledExecutorService executor) throws Exception {
      this("dse_leases", "leases", "logs", executor);
   }

   public synchronized boolean createLease(String name, String dc, int duration_ms) {
      checkNameAndDc(name, dc);
      LeaseMonitorCore.LeaseId key = new LeaseMonitorCore.LeaseId(name, dc);
      LeaseMonitor monitor = (LeaseMonitor)this.monitors.get(key);
      if(monitor != null) {
         boolean matches = !monitor.isDisabled() && monitor.exists() && monitor.duration_ms == duration_ms;
         logger.debug("Lease {}.{} already exists (matches: {})", new Object[]{name, dc, Boolean.valueOf(matches)});
         return matches;
      } else {
         try {
            if(this.core.createLease(name, dc, duration_ms)) {
               this.addMonitor(key, duration_ms);
               return true;
            } else {
               logger.debug("Failed to insert lease row for {}.{} with duration {}", new Object[]{name, dc, Integer.valueOf(duration_ms)});
               LeaseMonitorCore.LeaseRow row = this.core.readLease(name, dc);
               return row != null && row.duration_ms == duration_ms;
            }
         } catch (Exception var7) {
            noSpamLogger.warn("Caught exception while trying to create lease", new Object[]{var7});
            return false;
         }
      }
   }

   public Integer getLeaseDuration(String name, String dc) {
      LeaseMonitor monitor = (LeaseMonitor)this.monitors.get(new LeaseMonitorCore.LeaseId(name, dc));
      return monitor == null?null:Integer.valueOf(monitor.duration_ms);
   }

   public boolean disableLease(String name, String dc) {
      checkNameAndDc(name, dc);
      LeaseMonitor monitor = (LeaseMonitor)this.monitors.get(new LeaseMonitorCore.LeaseId(name, dc));
      return monitor != null && monitor.exists() && !monitor.isDisabled()?monitor.disable():true;
   }

   public boolean deleteLease(String name, String dc) {
      checkNameAndDc(name, dc);
      LeaseMonitorCore.LeaseId key = new LeaseMonitorCore.LeaseId(name, dc);
      LeaseMonitor monitor = (LeaseMonitor)this.monitors.get(key);
      if(monitor != null && monitor.exists()) {
         if(monitor.isDisabled() && monitor.isOpen(((Long)this.clock.get()).longValue())) {
            synchronized(monitor) {
               try {
                  this.core.deleteLease(name, dc);
               } catch (Exception var10) {
                  noSpamLogger.warn("Caught exception while trying to delete lease", new Object[]{var10});
                  return false;
               }
            }

            synchronized(this) {
               monitor.stop();
               this.monitors.remove(key);
               return true;
            }
         } else {
            return false;
         }
      } else {
         return true;
      }
   }

   public LeaseMonitor.ClientPingResult clientPing(String name, String dc, InetAddress client, boolean takeIfOpen) {
      checkNameAndDc(name, dc);
      if(client == null && takeIfOpen) {
         throw new IllegalArgumentException("Client cannot be null if it can take the lease.");
      } else {
         LeaseMonitor monitor = (LeaseMonitor)this.monitors.get(new LeaseMonitorCore.LeaseId(name, dc));
         if(monitor == null) {
            try {
               LeaseMonitorCore.LeaseRow row = this.core.readLease(name, dc);
               if(row != null) {
                  this.addMonitor(new LeaseMonitorCore.LeaseId(name, dc), row.duration_ms);
                  monitor = (LeaseMonitor)this.monitors.get(new LeaseMonitorCore.LeaseId(name, dc));
               }
            } catch (Exception var7) {
               noSpamLogger.warn("Caught exception while checking for unknown lease", new Object[0]);
            }
         }

         return monitor == null?new LeaseMonitor.ClientPingResult(-2L):monitor.clientPing(client, takeIfOpen);
      }
   }

   public void run() {
      this.rescheduleMonitors();
   }

   public synchronized LeaseManager start() {
      if(this.task != null) {
         this.stop();
      }

      this.task = this.executor.scheduleAtFixedRate(this, 0L, 5000L, TimeUnit.MILLISECONDS);
      return this;
   }

   public synchronized void stop() {
      if(this.task != null) {
         this.task.cancel(false);
         this.task = null;
      }

      this.monitors.keySet().stream().forEach((id) -> {
         this.removeMonitor(id);
      });
   }

   protected synchronized void rescheduleMonitors() {
      try {
         String dc = EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress());
         Set<LeaseMonitorCore.LeaseId> localLeases = this.core.getLocalDcRf() > 0?this.core.readLocalLeases(dc):Collections.emptySet();
         localLeases.stream().filter((id) -> {
            return !this.monitors.containsKey(id);
         }).map(LambdaMayThrow.logAndReturnNullOnException(noSpamLogger, (id) -> {
            return this.core.readLocalLease(id.name, id.dc);
         })).filter((row) -> {
            return row != null;
         }).forEach((row) -> {
            this.addMonitor(new LeaseMonitorCore.LeaseId(row.name, row.dc), row.duration_ms);
         });
         this.monitors.keySet().stream().filter((id) -> {
            return !localLeases.contains(id);
         }).forEach((deletedLease) -> {
            this.removeMonitor(deletedLease);
         });
      } catch (Exception var3) {
         noSpamLogger.warn("Couldn't read local lease table.", new Object[0]);
      }

   }

   protected synchronized void addMonitor(LeaseMonitorCore.LeaseId key, int duration_ms) {
      if(!this.monitors.containsKey(key)) {
         logger.debug("adding monitor for key {}", key);
         LeaseMonitor monitor = new LeaseMonitor(key.name, key.dc, duration_ms, this.core, () -> {
            return (Long)this.clock.get();
         }, (lm) -> {
            this.removeMonitor(new LeaseMonitorCore.LeaseId(lm.name, lm.dc));
         });
         this.startMonitor(monitor);
         this.monitors.put(key, monitor);
      }

   }

   protected synchronized void removeMonitor(LeaseMonitorCore.LeaseId key) {
      if(this.monitors.containsKey(key)) {
         logger.debug("removing monitor for key {}", key);
         ((LeaseMonitor)this.monitors.get(key)).stop();
         this.monitors.remove(key);
      }

   }

   protected void startMonitor(LeaseMonitor monitor) {
      monitor.start(this.executor, 0.1D);
   }

   private static void checkNameAndDc(String name, String dc) {
      if(name != null && !name.isEmpty()) {
         if(dc == null || dc.isEmpty()) {
            throw new IllegalArgumentException("DC cannot be empty or null");
         }
      } else {
         throw new IllegalArgumentException("Name cannot be empty or null");
      }
   }

   List<LeaseMetrics> getMetrics() {
      return (List)this.monitors.values().stream().map((monitor) -> {
         return monitor.getMetrics();
      }).collect(Collectors.toList());
   }

   public boolean isRunning() {
      return this.task != null;
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 3600000L, TimeUnit.MILLISECONDS);
   }
}
