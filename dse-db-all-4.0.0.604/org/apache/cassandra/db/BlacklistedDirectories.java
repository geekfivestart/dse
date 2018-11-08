package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlacklistedDirectories implements BlacklistedDirectoriesMBean {
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BlacklistedDirectories";
   private static final Logger logger = LoggerFactory.getLogger(BlacklistedDirectories.class);
   private static final BlacklistedDirectories instance = new BlacklistedDirectories();
   private final Set<File> unreadableDirectories = new CopyOnWriteArraySet();
   private final Set<File> unwritableDirectories = new CopyOnWriteArraySet();
   private static final AtomicInteger directoriesVersion = new AtomicInteger();

   private BlacklistedDirectories() {
      try {
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=BlacklistedDirectories"));
      } catch (Exception var2) {
         JVMStabilityInspector.inspectThrowable(var2);
         logger.error("error registering MBean {}", "org.apache.cassandra.db:type=BlacklistedDirectories", var2);
      }

   }

   public Set<File> getUnreadableDirectories() {
      return Collections.unmodifiableSet(this.unreadableDirectories);
   }

   public Set<File> getUnwritableDirectories() {
      return Collections.unmodifiableSet(this.unwritableDirectories);
   }

   public void markUnreadable(String path) {
      maybeMarkUnreadable(new File(path));
   }

   public void markUnwritable(String path) {
      maybeMarkUnwritable(new File(path));
   }

   public static File maybeMarkUnreadable(File path) {
      File directory = getDirectory(path);
      if(instance.unreadableDirectories.add(directory)) {
         directoriesVersion.incrementAndGet();
         logger.warn("Blacklisting {} for reads", directory);
         return directory;
      } else {
         return null;
      }
   }

   public static File maybeMarkUnwritable(File path) {
      File directory = getDirectory(path);
      if(instance.unwritableDirectories.add(directory)) {
         directoriesVersion.incrementAndGet();
         logger.warn("Blacklisting {} for writes", directory);
         return directory;
      } else {
         return null;
      }
   }

   public static int getDirectoriesVersion() {
      return directoriesVersion.get();
   }

   @VisibleForTesting
   public static void clearUnwritableUnsafe() {
      instance.unwritableDirectories.clear();
   }

   public static boolean isUnreadable(File directory) {
      return instance.unreadableDirectories.contains(directory);
   }

   public static boolean isUnwritable(File directory) {
      return instance.unwritableDirectories.contains(directory);
   }

   private static File getDirectory(File file) {
      return file.isDirectory()?file:(file.isFile()?file.getParentFile():(file.getPath().endsWith(".db")?file.getParentFile():file));
   }
}
