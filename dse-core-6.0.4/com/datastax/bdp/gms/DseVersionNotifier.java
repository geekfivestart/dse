package com.datastax.bdp.gms;

import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseVersionNotifier {
   private static final Logger logger = LoggerFactory.getLogger(DseVersionNotifier.class);
   public static final CassandraVersion MIN_VERSION = new CassandraVersion("0.0.0");
   public static final CassandraVersion VERSION_48 = new CassandraVersion("4.8.0");
   public static final CassandraVersion VERSION_50 = new CassandraVersion("5.0.0");
   public static final CassandraVersion VERSION_51 = new CassandraVersion("5.1.0");
   public static final CassandraVersion VERSION_60 = new CassandraVersion("6.0.0");
   private final Multimap<CassandraVersion, DseVersionNotifier.Observer> observers = LinkedHashMultimap.create();
   private final EndpointStateTracker endpointStateTracker;
   private volatile CassandraVersion minVersion;

   public DseVersionNotifier(EndpointStateTracker endpointStateTracker) {
      this.minVersion = MIN_VERSION;
      this.endpointStateTracker = endpointStateTracker;
   }

   protected Collection<InetAddress> getAllEndpoints() {
      return StorageService.instance.getTokenMetadata().getAllEndpoints();
   }

   protected CassandraVersion getEndpointVersion(InetAddress endpoint) {
      CassandraVersion version = this.endpointStateTracker.getDseVersion(endpoint);
      if(version != null) {
         return version;
      } else {
         Set<Workload> workloads = this.endpointStateTracker.getWorkloads(endpoint);
         return workloads != null && !workloads.isEmpty()?VERSION_48:DseState.localVersion;
      }
   }

   public CompletableFuture<Void> maybeUpdateVersion(CassandraVersion version) {
      return CompletableFuture.runAsync(() -> {
         this.maybeUpdateMinVersion(version);
      });
   }

   public synchronized void maybeUpdateMinVersion(CassandraVersion version) {
      if(version == null || version.compareTo(this.minVersion) > 0) {
         Optional<CassandraVersion> newMinVersion = this.getAllEndpoints().stream().map((endpoint) -> {
            return this.getEndpointVersion(endpoint);
         }).min((v1, v2) -> {
            return v1.compareTo(v2);
         });
         if(newMinVersion.isPresent() && ((CassandraVersion)newMinVersion.get()).compareTo(this.minVersion) > 0) {
            logger.debug("Min Cluster DSE Version {} -> {}", this.minVersion, newMinVersion.get());
            this.minVersion = (CassandraVersion)newMinVersion.get();
            Set<CassandraVersion> observedVersions = new HashSet(this.observers.keySet());
            Iterator var4 = observedVersions.iterator();

            while(true) {
               CassandraVersion observedVersion;
               do {
                  if(!var4.hasNext()) {
                     return;
                  }

                  observedVersion = (CassandraVersion)var4.next();
               } while(this.minVersion.compareTo(observedVersion) < 0);

               logger.trace("Min version met {}, endpoint statuses: {}", this.minVersion, Gossiper.instance.getEndpointStates().stream().map((entry) -> {
                  return entry.getKey() + ":" + ((EndpointState)entry.getValue()).getStatus();
               }).collect(Collectors.joining(", ")));
               Iterator var6 = this.observers.get(observedVersion).iterator();

               while(var6.hasNext()) {
                  DseVersionNotifier.Observer observer = (DseVersionNotifier.Observer)var6.next();

                  try {
                     observer.onMinVersionMet(this.minVersion);
                  } catch (Exception var9) {
                     logger.error("A component couldn't finish upgrading: ", var9);
                  }
               }

               this.observers.removeAll(observedVersion);
            }
         }
      }

   }

   public void runAtVersion(CassandraVersion minVersion, String standByMessage, String readyMessage, Runnable runnable) {
      if(this.minVersionMet(minVersion)) {
         runnable.run();
      } else {
         logger.info("Cluster hasn't been completely upgraded to DSE version {} or higher yet. {}", minVersion, standByMessage);
         this.addObserver((version) -> {
            runnable.run();
            logger.info("Cluster is now at DSE version {} or higher. {}", version, readyMessage);
         }, minVersion);
      }

   }

   public boolean isReady() {
      return !this.isMinVersion(this.minVersion);
   }

   public synchronized void addObserver(DseVersionNotifier.Observer observer, CassandraVersion version) {
      if(this.minVersionMet(version)) {
         logger.trace("Min version met {}, endpoint statuses: {}", this.minVersion, Gossiper.instance.getEndpointStates().stream().map((entry) -> {
            return entry.getKey() + ":" + ((EndpointState)entry.getValue()).getStatus();
         }).collect(Collectors.joining(", ")));
         observer.onMinVersionMet(this.minVersion);
      } else {
         this.observers.put(version, observer);
      }
   }

   public boolean minVersionMet(CassandraVersion version) {
      return this.minVersion.compareTo(version) >= 0;
   }

   public CassandraVersion getMinVersion() {
      return this.minVersion;
   }

   public boolean isMinVersion(CassandraVersion version) {
      return version.equals(MIN_VERSION);
   }

   @VisibleForTesting
   void setMinVersion(CassandraVersion minVersion) {
      this.minVersion = minVersion;
   }

   @VisibleForTesting
   int pendingVersions() {
      return this.observers.keySet().size();
   }

   public interface Observer {
      void onMinVersionMet(CassandraVersion var1);
   }
}
