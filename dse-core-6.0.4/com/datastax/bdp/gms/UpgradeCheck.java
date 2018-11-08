package com.datastax.bdp.gms;

import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeCheck {
   private static final Logger logger = LoggerFactory.getLogger(UpgradeCheck.class);
   private static final Comparator<CassandraVersion> MAJOR_MINOR_COMP = (one, two) -> {
      return ComparisonChain.start().compare(one.major, two.major).compare(one.minor, two.minor).result();
   };

   public UpgradeCheck() {
   }

   public static void failIfRequiredUpgradeIsSkipped(CassandraVersion currentVersion, CassandraVersion lastKnownDseVersion) {
      failIfMajorVersionIsSkippedDuringUpgrade(currentVersion, lastKnownDseVersion, DseVersionNotifier.VERSION_50);
   }

   public static void failIfMajorVersionIsSkippedDuringUpgrade(CassandraVersion currentVersion, CassandraVersion lastKnownDseVersion, CassandraVersion majorVersionNotAllowedToBeSkipped) {
      if(null != currentVersion && null != lastKnownDseVersion && null != majorVersionNotAllowedToBeSkipped) {
         if(currentVersion.compareTo(lastKnownDseVersion) > 0) {
            logger.info(String.format("Upgrading from DSE %s to DSE %s", new Object[]{lastKnownDseVersion.toString(), currentVersion.toString()}));
         } else if(currentVersion.compareTo(lastKnownDseVersion) < 0) {
            logger.info(String.format("Downgrading from DSE %s to DSE %s", new Object[]{lastKnownDseVersion.toString(), currentVersion.toString()}));
         }

         if(MAJOR_MINOR_COMP.compare(currentVersion, majorVersionNotAllowedToBeSkipped) > 0 && lastKnownDseVersion.compareTo(majorVersionNotAllowedToBeSkipped) < 0) {
            throw new VersionBarrier.UpgradingException(String.format("Major version skipped during upgrade. You are trying to upgrade from DSE %s to DSE %s without upgrading to the latest version of DSE %s.%s.x first. Uninstall DSE %s and upgrade to DSE %s.%s.x. See http://docs.datastax.com/en/latest-upgrade/ for more details.", new Object[]{lastKnownDseVersion.toString(), currentVersion.toString(), Integer.valueOf(majorVersionNotAllowedToBeSkipped.major), Integer.valueOf(majorVersionNotAllowedToBeSkipped.minor), currentVersion.toString(), Integer.valueOf(majorVersionNotAllowedToBeSkipped.major), Integer.valueOf(majorVersionNotAllowedToBeSkipped.minor)}));
         }
      }
   }
}
