package com.datastax.bdp.gms;

import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.snitch.EndpointStateTracker;
import java.util.function.Supplier;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionBarrier implements DseVersionNotifier.Observer {
   public static final Logger logger = LoggerFactory.getLogger(VersionBarrier.class);
   public VersionBarrier.Unlocker onUnlock;
   public Supplier<String> onAccessBeforeUnlock;
   private volatile boolean unlocked;

   public VersionBarrier(CassandraVersion min, Runnable ifUpgrading, VersionBarrier.Unlocker onUnlock, Supplier<String> onAccessBeforeUnlock) {
      this.onUnlock = null;
      this.onAccessBeforeUnlock = null;
      if(!DseDaemon.isSystemSchemaSetup() && !SystemInfo.isTestMode()) {
         throw new AssertionError("VersionBarrier depends on EndpointStateTracker which depends on the extra DSE fields in the system schema.  Probably you want to intialize it in the 'setupSchema' portion of your plugin.");
      } else {
         if(SystemInfo.isTestMode()) {
            this.unlocked = true;
            onUnlock.unlock(min, false);
         } else if(EndpointStateTracker.instance.getVersionNotifier().minVersionMet(min)) {
            this.unlocked = true;
            onUnlock.unlock(EndpointStateTracker.instance.getVersionNotifier().getMinVersion(), false);
         } else {
            this.unlocked = false;
            this.onUnlock = onUnlock;
            this.onAccessBeforeUnlock = onAccessBeforeUnlock;
            ifUpgrading.run();
            EndpointStateTracker.instance.getVersionNotifier().addObserver(this, min);
         }

      }
   }

   public VersionBarrier(CassandraVersion min, String ifUpgrading, String onUnlock, String onAccessBeforeUnlock) {
      this(min, () -> {
         logger.info(ifUpgrading);
      }, (version, wasUpgrade) -> {
         if(!wasUpgrade) {
            logger.info(onUnlock);
         }

      }, () -> {
         return onAccessBeforeUnlock;
      });
   }

   public synchronized void onMinVersionMet(CassandraVersion version) {
      if(!this.unlocked) {
         this.onUnlock.unlock(version, true);
         this.unlocked = true;
         this.onUnlock = null;
      }

   }

   public void check() {
      if(!this.unlocked) {
         VersionBarrier.UpgradingException e = new VersionBarrier.UpgradingException((String)this.onAccessBeforeUnlock.get());
         logger.debug("Early use of new features", e);
         throw e;
      }
   }

   public boolean isUnlocked() {
      return this.unlocked;
   }

   public static class UpgradingException extends RuntimeException {
      public UpgradingException(String message) {
         super(message);
      }
   }

   public interface Unlocker {
      void unlock(CassandraVersion var1, boolean var2);
   }
}
