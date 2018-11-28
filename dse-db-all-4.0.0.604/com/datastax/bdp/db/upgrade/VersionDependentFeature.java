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
      switch (this.status) {
         case UNKNOWN:
         case DEACTIVATED: {
            if (minVersionOK && !this.ddlChangeRequired()) {
               this.updateStatus(FeatureStatus.ACTIVATED);
               break;
            }
            if (minVersionOK) {
               this.updateStatus(FeatureStatus.DEACTIVATED);
               this.updateStatus(FeatureStatus.ACTIVATING);
               this.scheduleCallback();
               break;
            }
            this.updateStatus(FeatureStatus.DEACTIVATED);
            break;
         }
         case ACTIVATING: {
            if (this.ddlChangeRequired()) {
               if (!versionInfo.schemaAgreement) break;
               this.maybeScheduleDDL();
               break;
            }
            if (!versionInfo.schemaAgreement) break;
            this.updateStatus(FeatureStatus.ACTIVATED);
            break;
         }
         case ACTIVATED: {
            if (minVersionOK) break;
            this.updateStatus(FeatureStatus.DEACTIVATED);
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

   private synchronized void updateStatus(FeatureStatus newStatus) {
      if (this.status != newStatus) {
         this.status = newStatus;
         logger.debug("New status for '{}': {}", (Object)this.name, (Object)newStatus);
         switch (newStatus) {
            case ACTIVATED: {
               if (!this.currentInitialized) {
                  this.currentImplementation.initialize();
                  this.currentInitialized = true;
               }
               this.onFeatureActivated();
               break;
            }
            case ACTIVATING: {
               this.onFeatureActivating();
               break;
            }
            case DEACTIVATED: {
               if (!this.legacyInitialized) {
                  this.legacyImplementation.initialize();
                  this.legacyInitialized = true;
               }
               this.onFeatureDeactivated();
               break;
            }
            default: {
               throw new RuntimeException("Unknown new status " + (Object)((Object)newStatus));
            }
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
