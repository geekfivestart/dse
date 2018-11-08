package com.datastax.bdp.db.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VersionDependentFeature<I extends VersionDependentFeature.VersionDependent> {
   private static final Logger logger = LoggerFactory.getLogger(VersionDependentFeature.class);
   private static final long postDdlChangeCheckMillis = Long.getLong("dse.versionDependentFeature.postDdlChangeCheckMillis", 1000L).longValue();
   private VersionDependentFeature.FeatureStatus status;
   private final I currentImplementation;
   private final I legacyImplementation;
   private final String name;
   private final CassandraVersion minimumVersion;
   private boolean currentInitialized;
   private boolean legacyInitialized;
   private Future<?> ddlFuture;
   private ClusterVersionBarrier clusterVersionBarrier;

   public VersionDependentFeature(String name, CassandraVersion minimumVersion, I legacyImplementation, I currentImplementation) {
      this.status = VersionDependentFeature.FeatureStatus.UNKNOWN;
      this.name = name;
      this.minimumVersion = minimumVersion;
      this.legacyImplementation = legacyImplementation;
      this.currentImplementation = currentImplementation;
   }

   public static <I extends VersionDependentFeature.VersionDependent> VersionDependentFeature<I> createForSchemaUpgrade(String name, CassandraVersion minimumVersion, I legacyImplementation, I currentImplementation, final SchemaUpgrade schemaUpgrade, final Logger logger, final String messageActivating, final String messageActivated, final String messageDeactivated) {
      return new VersionDependentFeature<I>(name, minimumVersion, legacyImplementation, currentImplementation) {
         protected boolean ddlChangeRequired() {
            return schemaUpgrade.ddlChangeRequired();
         }

         protected void executeDDL() {
            schemaUpgrade.executeDDL();
         }

         public void onFeatureActivating() {
            logger.info(messageActivating);
         }

         public void onFeatureActivated() {
            logger.info(messageActivated);
         }

         public void onFeatureDeactivated() {
            logger.info(messageDeactivated);
         }
      };
   }

   public void setup(ClusterVersionBarrier clusterVersionBarrier) {
      clusterVersionBarrier.register(this::clusterVersionUpdated);
      this.clusterVersionBarrier = clusterVersionBarrier;
   }

   @VisibleForTesting
   public synchronized void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo) {
      logger.trace("clusterVersionUpdated for {}/{}: {}", new Object[]{this.name, this.status, versionInfo});
      boolean minVersionOK = this.minimumVersion.compareTo(versionInfo.minOss) <= 0;
      switch(null.$SwitchMap$com$datastax$bdp$db$upgrade$VersionDependentFeature$FeatureStatus[this.status.ordinal()]) {
      case 1:
      case 2:
         if(minVersionOK && !this.ddlChangeRequired()) {
            this.updateStatus(VersionDependentFeature.FeatureStatus.ACTIVATED);
         } else if(minVersionOK) {
            this.updateStatus(VersionDependentFeature.FeatureStatus.DEACTIVATED);
            this.updateStatus(VersionDependentFeature.FeatureStatus.ACTIVATING);
            this.scheduleCallback();
         } else {
            this.updateStatus(VersionDependentFeature.FeatureStatus.DEACTIVATED);
         }
         break;
      case 3:
         if(this.ddlChangeRequired()) {
            if(versionInfo.schemaAgreement) {
               this.maybeScheduleDDL();
            }
         } else if(versionInfo.schemaAgreement) {
            this.updateStatus(VersionDependentFeature.FeatureStatus.ACTIVATED);
         }
         break;
      case 4:
         if(!minVersionOK) {
            this.updateStatus(VersionDependentFeature.FeatureStatus.DEACTIVATED);
         }
      }

   }

   private synchronized void maybeScheduleDDL() {
      if(this.ddlFuture == null) {
         logger.info("Scheduling DDL change for '{}'", this.name);
         this.ddlFuture = ScheduledExecutors.nonPeriodicTasks.submit(() -> {
            try {
               this.executeDDL();
            } finally {
               this.ddlFinished();
               this.scheduleCallback();
            }

         });
      }

   }

   private synchronized void scheduleCallback() {
      ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
         this.clusterVersionUpdated(this.clusterVersionBarrier.currentClusterVersionInfo());
      }, postDdlChangeCheckMillis, TimeUnit.MILLISECONDS);
   }

   private synchronized void ddlFinished() {
      logger.debug("DDL change for '{}' finished", this.name);
      this.ddlFuture = null;
   }

   private synchronized void updateStatus(VersionDependentFeature.FeatureStatus newStatus) {
      if(this.status != newStatus) {
         this.status = newStatus;
         logger.debug("New status for '{}': {}", this.name, newStatus);
         switch(null.$SwitchMap$com$datastax$bdp$db$upgrade$VersionDependentFeature$FeatureStatus[newStatus.ordinal()]) {
         case 2:
            if(!this.legacyInitialized) {
               this.legacyImplementation.initialize();
               this.legacyInitialized = true;
            }

            this.onFeatureDeactivated();
            break;
         case 3:
            this.onFeatureActivating();
            break;
         case 4:
            if(!this.currentInitialized) {
               this.currentImplementation.initialize();
               this.currentInitialized = true;
            }

            this.onFeatureActivated();
            break;
         default:
            throw new RuntimeException("Unknown new status " + newStatus);
         }
      }

   }

   public I implementation() {
      return this.status == VersionDependentFeature.FeatureStatus.ACTIVATED?this.currentImplementation:this.legacyImplementation;
   }

   public VersionDependentFeature.FeatureStatus getStatus() {
      return this.status;
   }

   protected abstract boolean ddlChangeRequired();

   protected abstract void executeDDL();

   public abstract void onFeatureDeactivated();

   public abstract void onFeatureActivating();

   public abstract void onFeatureActivated();

   public String toString() {
      return "VersionDependentFeature{name='" + this.name + '\'' + ", minimumVersion=" + this.minimumVersion + ", status=" + this.status + '}';
   }

   public interface VersionDependent {
      void initialize();
   }

   public static enum FeatureStatus {
      UNKNOWN,
      DEACTIVATED,
      ACTIVATING,
      ACTIVATED;

      private FeatureStatus() {
      }
   }
}
