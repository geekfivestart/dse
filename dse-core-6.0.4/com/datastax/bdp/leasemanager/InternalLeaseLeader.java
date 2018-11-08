package com.datastax.bdp.leasemanager;

import com.datastax.bdp.plugin.IPlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.google.common.util.concurrent.Futures;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalLeaseLeader implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(InternalLeaseLeader.class);
   private static final NoSpamLogger noSpamLogger;
   public final String name;
   public final String dc;
   public final int duration_ms;
   public final boolean takeIfOpen;
   private InetAddress holder;
   private long expires;
   private final List<InternalLeaseLeader.Listener> onChange;
   private final LeasePlugin plugin;
   private final ThreadPoolPlugin threadPool;
   private volatile boolean cancelled;
   private volatile boolean created;

   InternalLeaseLeader(LeasePlugin plugin, ThreadPoolPlugin threadPool, String name, String dc, int duration_ms, boolean takeIfOpen) {
      this.plugin = plugin;
      this.threadPool = threadPool;
      this.name = name;
      this.dc = dc;
      this.duration_ms = duration_ms;
      this.onChange = new ArrayList();
      this.takeIfOpen = takeIfOpen;
      this.cancelled = false;
      this.created = false;
      this.expires = 0L;
      this.run();
   }

   public InetAddress getHolder() {
      return this.isHeld()?this.holder:null;
   }

   public boolean isHeld() {
      return this.expires > System.currentTimeMillis() && this.holder != null;
   }

   public boolean isHeldByMeForAtLeast(long left) {
      return this.expires - left > System.currentTimeMillis() && Objects.equals(this.holder, Addresses.Internode.getBroadcastAddress());
   }

   public boolean isHeldByMe() {
      return this.isHeldByMeForAtLeast(0L);
   }

   public void cancel() {
      logger.info("Cancelling internal lease polling for {}.{}", this.name, this.dc);
      this.maybeUpdateHolder((InetAddress)null);
      this.cancelled = true;
   }

   public synchronized void addListener(InternalLeaseLeader.Listener listener) {
      this.onChange.add(listener);
      if(this.isHeld()) {
         listener.onLeaseHolderChange((InetAddress)null, this.holder);
      }

   }

   public synchronized void removeListener(InternalLeaseLeader.Listener listener) {
      this.onChange.remove(listener);
   }

   private boolean createLease() {
      try {
         this.created = this.plugin.createLease(this.name, this.dc, this.duration_ms);
         if(this.created) {
            logger.debug("Created lease {}.{}", this.name, this.dc);
         } else {
            this.maybeWarnOfCreateLeaseFailure(new Exception("Couldn't create lease"));
         }
      } catch (IllegalStateException var2) {
         logger.debug("Plugin not active yet; will try again in a few seconds.", var2);
      } catch (Exception var3) {
         this.maybeWarnOfCreateLeaseFailure(var3);
      }

      return this.created;
   }

   private void maybeWarnOfCreateLeaseFailure(Exception ex) {
      if(EndpointStateTracker.instance.isActive(Addresses.Internode.getBroadcastAddress())) {
         noSpamLogger.warn(String.format("Lease LWT query failed; lease %s.%s will be unavailable until it can succeed. Most likely this will be fixed by increasing the replication factor for the dse_leases keyspace in this DC (%s) to 3 and repairing the dse_leases keyspace. If not, something else is causing the queries to fail, such as nodes being overloaded or network connectivity issues. You may also see this warning briefly after increasing the replication factor, and before repair of the keyspace finishes.", new Object[]{this.name, this.dc, this.dc}), new Object[]{ex});
      }

   }

   public void run() {
      try {
         if(!this.cancelled && (this.created || this.createLease())) {
            LeaseMonitor.ClientPingResult r = this.plugin.internalClientPing(this.name, this.dc, this.takeIfOpen);
            synchronized(this) {
               if(this.isHeld() && !r.isHeld()) {
                  logger.debug("Ignoring update {} because we still have {}ms remaining on our current timer.", r, Long.valueOf(System.currentTimeMillis() - this.expires));
               } else {
                  this.expires = r.leaseTimeRemaining;
                  this.maybeUpdateHolder(r.holder);
               }
            }

            if(r.leaseTimeRemaining < 0L && !SystemInfo.isSystemLease(this.name)) {
               logger.info("Lease {}.{} has been {}.", new Object[]{this.name, this.dc, r.leaseTimeRemaining == -2L?"deleted":"disabled"});
               this.cancel();
            }
         }
      } catch (Exception var6) {
         Exception e = var6;

         try {
            if(this.created && this.plugin.getReplicationFactor() == 0) {
               logger.info("Suppressing '{}:{}' and ceasing to poll {}.{} because it seems this datacenter is being decommissioned (the lease keyspace is no longer present here). If this is unintentional, increase the replication factor of the lease keyspace and restart DSE.", new Object[]{e.getClass().getSimpleName(), e.getMessage(), this.name, this.dc});
               logger.debug("Suppressed exception: ", e);
               this.cancel();
            } else {
               noSpamLogger.warn("Exception while polling lease {}.{}", new Object[]{this.name, this.dc, e});
            }

            if(!this.isHeld()) {
               this.maybeUpdateHolder((InetAddress)null);
            }
         } catch (IPlugin.PluginNotActiveException var4) {
            logger.info("Chained exceptions {} and {}; are we shutting down?", var6, var4);
            this.cancel();
         }
      }

      if(!this.cancelled) {
         this.threadPool.schedule((Runnable)this, (long)(this.duration_ms / 10), TimeUnit.MILLISECONDS);
      }

   }

   private synchronized void maybeUpdateHolder(InetAddress newHolder) {
      if(!Objects.equals(this.holder, newHolder)) {
         logger.debug("notifying {} listeners that lease {}.{} is now held by {} not {}", new Object[]{Integer.valueOf(this.onChange.size()), this.name, this.dc, newHolder, this.holder});
         InetAddress oldHolder = this.holder;
         this.holder = newHolder;
         this.onChange.stream().forEach((listener) -> {
            listener.onLeaseHolderChange(oldHolder, this.holder);
         });
      }

   }

   public String toString() {
      return String.format("InternalLeaseLeader(name=%s, dc=%s, duration_ms=%d, takeIfOpen=%s, holder=%s, expires=%d, %d listeners.", new Object[]{this.name, this.dc, Integer.valueOf(this.duration_ms), this.takeIfOpen + "", LeaseMonitorCore.formatInetAddress(this.holder), Long.valueOf(this.expires), Integer.valueOf(this.onChange.size())});
   }

   public <T> Future<T> executeIfLeader(Callable<T> task, long margin, boolean abortOnLeaseExpiration, boolean testMode) {
      if(this.isHeldByMeForAtLeast(margin)) {
         if(testMode) {
            try {
               logger.debug("Executing {} as leader of {}.{} immediately", new Object[]{task, this.name, this.dc});
               return Futures.immediateFuture(task.call());
            } catch (Exception var7) {
               return Futures.immediateFailedFuture(var7);
            }
         } else {
            logger.debug("Executing {} as leader of {}.{}", new Object[]{task, this.name, this.dc});
            Future future = this.threadPool.submit(task);
            this.threadPool.submit((Runnable)(new InternalLeaseLeader.TaskMonitor(future, margin, abortOnLeaseExpiration)));
            return future;
         }
      } else {
         logger.debug("Not executing {} as we are not the leader of {}.{}!", new Object[]{task, this.name, this.dc});
         return null;
      }
   }

   public Future executeIfLeader(Runnable task, long margin, boolean abortOnLeaseExpiration, boolean testMode) {
      return this.executeIfLeader(() -> {
         task.run();
         return null;
      }, margin, abortOnLeaseExpiration, testMode);
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 3600000L, TimeUnit.MILLISECONDS);
   }

   public class TaskMonitor implements Runnable {
      public final Future task;
      public final long safetyMargin;
      public final boolean abortOnLeaseExpiration;

      public TaskMonitor(Future task, long safetyMargin, boolean abortOnLeaseExpiration) {
         this.task = task;
         this.safetyMargin = safetyMargin;
         this.abortOnLeaseExpiration = abortOnLeaseExpiration;
      }

      public void run() {
         if(!this.task.isDone() && !this.task.isCancelled()) {
            if(InternalLeaseLeader.this.isHeldByMeForAtLeast(this.safetyMargin)) {
               InternalLeaseLeader.this.threadPool.schedule((Runnable)this, InternalLeaseLeader.this.expires - System.currentTimeMillis() - this.safetyMargin, TimeUnit.MILLISECONDS);
            } else {
               InternalLeaseLeader.logger.debug("Cancelling task {} because we lost the lease {}.{}", new Object[]{this.task, InternalLeaseLeader.this.name, InternalLeaseLeader.this.dc});
               this.task.cancel(this.abortOnLeaseExpiration);
            }
         }

      }
   }

   public interface Listener {
      void onLeaseHolderChange(InetAddress var1, InetAddress var2);
   }
}
