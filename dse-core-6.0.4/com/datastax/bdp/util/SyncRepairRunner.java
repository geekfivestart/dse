package com.datastax.bdp.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncRepairRunner {
   private static final Logger logger = LoggerFactory.getLogger(SyncRepairRunner.class);

   public SyncRepairRunner() {
   }

   public static void repairPrimaryRange(String keyspace, String... columnFamilies) throws IOException {
      boolean success = runRepair(StorageService.instance, true, new SyncRepairRunner.RepairRunner(StorageService.instance, new SimpleCondition(), keyspace, columnFamilies));
      if(!success) {
         throw new IOException(String.format("Primary range repair task for %s [%s] failed", new Object[]{keyspace, Joiner.on(",").join(columnFamilies)}));
      }
   }

   public static void repair(String keyspace, String... columnFamilies) throws IOException {
      boolean success = runRepair(StorageService.instance, false, new SyncRepairRunner.RepairRunner(StorageService.instance, new SimpleCondition(), keyspace, columnFamilies));
      if(!success) {
         throw new IOException(String.format("Repair task for %s [%s] failed", new Object[]{keyspace, Joiner.on(",").join(columnFamilies)}));
      }
   }

   private static boolean runRepair(StorageService storage, boolean primaryRange, SyncRepairRunner.RepairRunner runner) throws IOException {
      storage.addNotificationListener(runner, (NotificationFilter)null, (Object)null);

      boolean var3;
      try {
         var3 = runner.runRepair(primaryRange);
      } catch (Exception var12) {
         logger.warn("Caught exception waiting for repair task to complete", var12);
         throw new IOException(var12);
      } finally {
         try {
            storage.removeNotificationListener(runner);
         } catch (ListenerNotFoundException var11) {
            logger.debug("Repair listener not registered, cannot remove it", var11);
         }

      }

      return var3;
   }

   @VisibleForTesting
   protected static boolean runRepairUnsafe(StorageService storage, boolean primaryRange, SyncRepairRunner.RepairRunner runner) throws IOException {
      return runRepair(storage, primaryRange, runner);
   }

   @VisibleForTesting
   static class RepairRunner implements NotificationListener {
      private final StorageService storage;
      private final Condition condition;
      private final String keyspace;
      private final String[] columnFamilies;
      private int cmd;
      private volatile boolean failureReported = false;
      private volatile boolean successReported = false;
      private String sourceTag;

      RepairRunner(StorageService storage, Condition condition, String keyspace, String... columnFamilies) {
         this.storage = storage;
         this.condition = condition;
         this.keyspace = keyspace;
         this.columnFamilies = columnFamilies;
      }

      public boolean runRepair(boolean primaryRange) throws InterruptedException {
         RepairOption options = new RepairOption(RepairParallelism.PARALLEL, primaryRange, false, false, 1, Collections.emptyList(), false, false, PreviewKind.NONE);
         if(primaryRange) {
            options.getRanges().addAll(this.storage.getPrimaryRanges(this.keyspace));
         } else {
            options.getRanges().addAll(this.storage.getLocalRanges(this.keyspace));
         }

         if(this.columnFamilies != null) {
            String[] var3 = this.columnFamilies;
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               String columnFamily = var3[var5];
               options.getColumnFamilies().add(columnFamily);
            }
         }

         this.cmd = this.storage.repairAsync(this.keyspace, options.asMap());
         if(this.cmd <= 0) {
            SyncRepairRunner.logger.debug("Repair task was a no-op, returning success");
            return true;
         } else {
            SyncRepairRunner.logger.debug("Repair task {} submitted, waiting for status notifications", Integer.valueOf(this.cmd));
            this.sourceTag = "repair:" + this.cmd;
            this.condition.await();
            return this.successReported && !this.failureReported;
         }
      }

      public void handleNotification(Notification notification, Object handback) {
         if(notification.getSource().equals(this.sourceTag)) {
            SyncRepairRunner.logger.debug("Received notification {}, {}", notification.getType(), notification.getUserData());
            if(notification.getType().equals("progress")) {
               Map<String, Integer> progress = (Map)notification.getUserData();
               ProgressEventType event = ProgressEventType.values()[((Integer)progress.get("type")).intValue()];
               if(event == ProgressEventType.SUCCESS) {
                  this.successReported = true;
               } else if(event == ProgressEventType.ERROR) {
                  this.failureReported = true;
               } else if(event == ProgressEventType.COMPLETE) {
                  this.condition.signalAll();
               }
            }
         }

      }
   }
}
