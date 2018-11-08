package com.datastax.bdp.server.system;

import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Iterator;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SystemResourcesInfoProvider {
   long getTotalPhysicalMemory();

   double getProcessCpuLoad();

   long getTotalPhysicalDiskSpace();

   long getFreeDiskSpaceRemaining();

   long getTotalTableDataSize();

   long getTotalIndexDataSize();

   public static class DefaultProvider implements SystemResourcesInfoProvider {
      private static final Logger logger = LoggerFactory.getLogger(SystemResourcesInfoProvider.DefaultProvider.class);
      private OperatingSystemMXBean os;

      public DefaultProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.os = (OperatingSystemMXBean)JMX.newMXBeanProxy(server, ObjectName.getInstance("java.lang:type=OperatingSystem"), OperatingSystemMXBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public long getTotalPhysicalMemory() {
         try {
            return this.os.getTotalPhysicalMemorySize();
         } catch (Exception var2) {
            logger.debug("Unable to query total physical memory", var2.getMessage());
            return 0L;
         }
      }

      public double getProcessCpuLoad() {
         try {
            return this.os.getProcessCpuLoad();
         } catch (Exception var2) {
            logger.debug("Unable to query total physical memory", var2.getMessage());
            return 0.0D;
         }
      }

      public long getTotalPhysicalDiskSpace() {
         long total = 0L;
         FileSystem fileSystem = FileSystems.getDefault();
         Iterator var4 = fileSystem.getFileStores().iterator();

         while(var4.hasNext()) {
            FileStore fileStore = (FileStore)var4.next();

            try {
               total += fileStore.getTotalSpace();
            } catch (IOException var7) {
               logger.debug("Error getting total space for filestore {}", fileStore.name());
            }
         }

         return total;
      }

      public long getFreeDiskSpaceRemaining() {
         long total = 0L;
         FileSystem fileSystem = FileSystems.getDefault();
         Iterator var4 = fileSystem.getFileStores().iterator();

         while(var4.hasNext()) {
            FileStore fileStore = (FileStore)var4.next();
            if(!fileStore.isReadOnly()) {
               try {
                  total += fileStore.getUsableSpace();
               } catch (IOException var7) {
                  logger.debug("Error getting usable space for filestore {}", fileStore.name());
               }
            }
         }

         return total;
      }

      public long getTotalTableDataSize() {
         return this.getTotalCFSSize(false);
      }

      public long getTotalIndexDataSize() {
         return this.getTotalCFSSize(true);
      }

      private long getTotalCFSSize(boolean indexes) {
         long total = 0L;
         Iterator var4 = ColumnFamilyStore.all().iterator();

         while(var4.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
            if(cfs.isIndex() == indexes) {
               total += cfs.metric.totalDiskSpaceUsed.getCount();
            }
         }

         return total;
      }
   }
}
