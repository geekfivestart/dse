package org.apache.cassandra.io.sstable;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotDeletingTask implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(SnapshotDeletingTask.class);
   public final File path;
   private static final Queue<Runnable> failedTasks = new ConcurrentLinkedQueue();

   public static void addFailedSnapshot(File path) {
      logger.warn("Failed to delete snapshot [{}]. Will retry after further sstable deletions. Folder will be deleted on JVM shutdown or next node restart on crash.", path);
      WindowsFailedSnapshotTracker.handleFailedSnapshot(path);
      failedTasks.add(new SnapshotDeletingTask(path));
   }

   private SnapshotDeletingTask(File path) {
      this.path = path;
   }

   public void run() {
      try {
         FileUtils.deleteRecursive(this.path);
         logger.info("Successfully deleted snapshot {}.", this.path);
      } catch (FSWriteError var2) {
         failedTasks.add(this);
      }

   }

   public static void rescheduleFailedTasks() {
      Runnable task;
      while(null != (task = (Runnable)failedTasks.poll())) {
         ScheduledExecutors.nonPeriodicTasks.submit(task);
      }

   }

   @VisibleForTesting
   public static int pendingDeletionCount() {
      return failedTasks.size();
   }
}
