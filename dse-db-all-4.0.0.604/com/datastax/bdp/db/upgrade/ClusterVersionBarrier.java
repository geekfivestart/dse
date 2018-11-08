package com.datastax.bdp.db.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterVersionBarrier {
   private static final Logger logger = LoggerFactory.getLogger(ClusterVersionBarrier.class);
   public static final CassandraVersion ownOssVersion = new CassandraVersion(FBUtilities.getReleaseVersionString());
   private volatile ClusterVersionBarrier.ClusterVersionInfo current = new ClusterVersionBarrier.ClusterVersionInfo(new CassandraVersion("0.0.0"), new CassandraVersion("0.0.0"), UUID.randomUUID(), true);
   private final CopyOnWriteArrayList<ClusterVersionBarrier.ClusterVersionListener> listeners = new CopyOnWriteArrayList();
   private volatile int scheduled;
   private static final AtomicIntegerFieldUpdater<ClusterVersionBarrier> scheduledUpdater = AtomicIntegerFieldUpdater.newUpdater(ClusterVersionBarrier.class, "scheduled");
   private boolean ready;
   private final Supplier<Iterable<InetAddress>> endpointsSupplier;
   private final Function<InetAddress, ClusterVersionBarrier.EndpointInfo> endpointInfoFunction;

   public ClusterVersionBarrier(Supplier<Iterable<InetAddress>> endpointsSupplier, Function<InetAddress, ClusterVersionBarrier.EndpointInfo> endpointInfoFunction) {
      this.endpointsSupplier = endpointsSupplier;
      this.endpointInfoFunction = endpointInfoFunction;
   }

   public void scheduleUpdateVersions() {
      if(scheduledUpdater.compareAndSet(this, 0, 1)) {
         ScheduledExecutors.nonPeriodicTasks.execute(() -> {
            scheduledUpdater.set(this, 0);
            this.updateVersionsBlocking();
         });
      }

   }

   public ClusterVersionBarrier.ClusterVersionInfo currentClusterVersionInfo() {
      return this.current;
   }

   @VisibleForTesting
   boolean hasScheduledUpdate() {
      return scheduledUpdater.get(this) != 0;
   }

   public synchronized void updateVersionsBlocking() {
      ClusterVersionBarrier.ClusterVersionInfo versions = this.computeClusterVersionInfo();
      if(!this.current.equals(versions) && this.ready) {
         this.current = versions;
         logger.trace("updateVersionsBlocking - calling listeners");
         Iterator var2 = this.listeners.iterator();

         while(var2.hasNext()) {
            ClusterVersionBarrier.ClusterVersionListener listener = (ClusterVersionBarrier.ClusterVersionListener)var2.next();
            listener.clusterVersionUpdated(versions);
         }
      }

   }

   private ClusterVersionBarrier.ClusterVersionInfo computeClusterVersionInfo() {
      logger.trace("computeClusterVersionInfo - start computing");
      CassandraVersion minOss = null;
      CassandraVersion maxOss = null;
      UUID schema = null;
      boolean schemaAgreement = true;
      Iterator var5 = ((Iterable)this.endpointsSupplier.get()).iterator();

      while(true) {
         ClusterVersionBarrier.EndpointInfo epInfo;
         do {
            if(!var5.hasNext()) {
               if(minOss == null) {
                  minOss = ownOssVersion;
               }

               if(maxOss == null) {
                  maxOss = ownOssVersion;
               }

               ClusterVersionBarrier.ClusterVersionInfo clusterVersionInfo = new ClusterVersionBarrier.ClusterVersionInfo(minOss, maxOss, schemaAgreement?schema:null, schemaAgreement);
               logger.trace("computeClusterVersionInfo - result={}", clusterVersionInfo);
               return clusterVersionInfo;
            }

            InetAddress ep = (InetAddress)var5.next();
            epInfo = (ClusterVersionBarrier.EndpointInfo)this.endpointInfoFunction.apply(ep);
            logger.trace("computeClusterVersionInfo - endpoint {} : {}", ep, epInfo);
         } while(epInfo == null);

         CassandraVersion epVer = epInfo.version;
         if(epVer != null) {
            if(minOss == null || epVer.compareTo(minOss) < 0) {
               minOss = epVer;
            }

            if(maxOss == null || epVer.compareTo(maxOss) > 0) {
               maxOss = epVer;
            }
         }

         UUID schemaVer = epInfo.schemaVersion;
         if(schemaVer != null) {
            if(schema == null) {
               schema = schemaVer;
            } else if(!schema.equals(schemaVer)) {
               schemaAgreement = false;
            }
         } else {
            schemaAgreement = false;
         }
      }
   }

   public synchronized void onLocalNodeReady() {
      if(!this.ready) {
         this.ready = true;
         this.updateVersionsBlocking();
      }

   }

   public synchronized void register(ClusterVersionBarrier.ClusterVersionListener listener) {
      if(this.ready) {
         listener.clusterVersionUpdated(this.current);
      }

      this.listeners.add(listener);
   }

   @VisibleForTesting
   public synchronized void removeAllListeners() {
      this.listeners.clear();
   }

   public interface ClusterVersionListener {
      void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo var1);
   }

   public static final class ClusterVersionInfo {
      public final CassandraVersion minOss;
      public final CassandraVersion maxOss;
      public final UUID schemaVersion;
      public final boolean schemaAgreement;

      public ClusterVersionInfo(CassandraVersion minOss, CassandraVersion maxOss, UUID schemaVersion, boolean schemaAgreement) {
         this.minOss = minOss;
         this.maxOss = maxOss;
         this.schemaVersion = schemaVersion;
         this.schemaAgreement = schemaAgreement;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ClusterVersionBarrier.ClusterVersionInfo that = (ClusterVersionBarrier.ClusterVersionInfo)o;
            return Objects.equals(this.minOss, that.minOss) && Objects.equals(this.maxOss, that.maxOss) && Objects.equals(this.schemaVersion, that.schemaVersion) && this.schemaAgreement == that.schemaAgreement;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.minOss, this.maxOss, this.schemaVersion, Boolean.valueOf(this.schemaAgreement)});
      }

      public String toString() {
         return "ClusterVersionInfo{minOss=" + this.minOss + ", maxOss=" + this.maxOss + ", schemaVersion=" + this.schemaVersion + ", schemaAgreement=" + this.schemaAgreement + '}';
      }
   }

   public static final class EndpointInfo {
      public final CassandraVersion version;
      public final UUID schemaVersion;

      public EndpointInfo(CassandraVersion version, UUID schemaVersion) {
         this.version = version;
         this.schemaVersion = schemaVersion;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ClusterVersionBarrier.EndpointInfo that = (ClusterVersionBarrier.EndpointInfo)o;
            return Objects.equals(this.version, that.version) && Objects.equals(this.schemaVersion, that.schemaVersion);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.version, this.schemaVersion});
      }

      public String toString() {
         return "EndpointInfo{version=" + this.version + ", schemaVersion=" + this.schemaVersion + '}';
      }
   }
}
