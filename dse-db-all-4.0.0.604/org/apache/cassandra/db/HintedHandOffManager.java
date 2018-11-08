package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.hints.HintsService;

/** @deprecated */
@Deprecated
public final class HintedHandOffManager implements HintedHandOffManagerMBean {
   public static final HintedHandOffManager instance = new HintedHandOffManager();
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";

   private HintedHandOffManager() {
   }

   public void registerMBean() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=HintedHandoffManager"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public void deleteHintsForEndpoint(String host) {
      HintsService.instance.deleteAllHintsForEndpoint(host);
   }

   public void truncateAllHints() {
      HintsService.instance.deleteAllHints();
   }

   public List<String> listEndpointsPendingHints() {
      throw new UnsupportedOperationException();
   }

   public void scheduleHintDelivery(String host) {
      throw new UnsupportedOperationException();
   }

   public void pauseHintsDelivery(boolean doPause) {
      if(doPause) {
         HintsService.instance.pauseDispatch();
      } else {
         HintsService.instance.resumeDispatch();
      }

   }
}
