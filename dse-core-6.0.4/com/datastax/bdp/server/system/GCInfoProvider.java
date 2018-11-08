package com.datastax.bdp.server.system;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface GCInfoProvider {
   long getCmsCollectionCount();

   long getCmsCollectionTime();

   long getParNewCollectionCount();

   long getParNewCollectionTime();

   public static class JmxGCInfoProvider implements GCInfoProvider {
      private static final Logger logger = LoggerFactory.getLogger(GCInfoProvider.JmxGCInfoProvider.class);
      private GarbageCollectorMXBean cms = this.getBean("ConcurrentMarkSweep");
      private GarbageCollectorMXBean parNew = this.getBean("ParNew");

      public JmxGCInfoProvider() {
      }

      public long getCmsCollectionCount() {
         try {
            return this.cms.getCollectionCount();
         } catch (Exception var2) {
            logger.debug("ConcurrentMarkSweep statistics not available");
            this.cms = this.getBean("ConcurrentMarkSweep");
            return 0L;
         }
      }

      public long getCmsCollectionTime() {
         try {
            return this.cms.getCollectionTime();
         } catch (Exception var2) {
            logger.debug("ConcurrentMarkSweep statistics not available");
            this.cms = this.getBean("ConcurrentMarkSweep");
            return 0L;
         }
      }

      public long getParNewCollectionCount() {
         try {
            return this.parNew.getCollectionCount();
         } catch (Exception var2) {
            logger.debug("ParNew statistics not available");
            this.parNew = this.getBean("ParNew");
            return 0L;
         }
      }

      public long getParNewCollectionTime() {
         try {
            return this.parNew.getCollectionTime();
         } catch (Exception var2) {
            logger.debug("ParNew statistics not available");
            this.parNew = this.getBean("ParNew");
            return 0L;
         }
      }

      private GarbageCollectorMXBean getBean(String type) {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            return (GarbageCollectorMXBean)JMX.newMXBeanProxy(server, ObjectName.getInstance(String.format("java.lang:type=GarbageCollector,name=%s", new Object[]{type})), GarbageCollectorMXBean.class);
         } catch (MalformedObjectNameException var4) {
            throw new RuntimeException(var4);
         }
      }
   }
}
