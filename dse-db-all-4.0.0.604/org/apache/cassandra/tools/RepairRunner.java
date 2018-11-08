package org.apache.cassandra.tools;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;

public class RepairRunner extends JMXNotificationProgressListener {
   private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
   private final PrintStream out;
   private final StorageServiceMBean ssProxy;
   private final String keyspace;
   private final Map<String, String> options;
   private final Condition condition = new SimpleCondition();
   private volatile Integer cmd = null;
   private volatile Exception error;

   public RepairRunner(PrintStream out, StorageServiceMBean ssProxy, String keyspace, Map<String, String> options) {
      this.out = out;
      this.ssProxy = ssProxy;
      this.keyspace = keyspace;
      this.options = options;
   }

   public void run() throws Exception {
      this.cmd = Integer.valueOf(this.ssProxy.repairAsync(this.keyspace, this.options));
      if(this.cmd.intValue() <= 0) {
         String message = String.format("[%s] Replication factor is 1. No repair is needed for keyspace '%s'", new Object[]{this.format.format(Long.valueOf(System.currentTimeMillis())), this.keyspace});
         this.out.println(message);
      } else {
         while(true) {
            if(this.condition.await(NodeProbe.JMX_NOTIFICATION_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
               if(this.error != null) {
                  throw this.error;
               }
               break;
            }

            this.queryForCompletedRepair(String.format("After waiting for poll interval of %s seconds", new Object[]{Long.valueOf(NodeProbe.JMX_NOTIFICATION_POLL_INTERVAL_SECONDS)}));
         }
      }

   }

   public boolean isInterestedIn(String tag) {
      int var2 = 0;

      while(this.cmd == null) {
         if(var2++ == 600) {
            this.out.println("Did not get repair command ID after 1 minute - repair notifications may be lost.");
            break;
         }

         Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
      }

      return tag.equals("repair:" + this.cmd);
   }

   public void handleNotificationLost(long timestamp, String message) {
      if(this.cmd.intValue() > 0) {
         this.queryForCompletedRepair("After receiving lost notification");
      }

   }

   public void handleConnectionClosed(long timestamp, String message) {
      this.handleConnectionFailed(timestamp, message);
   }

   public void handleConnectionFailed(long timestamp, String message) {
      this.error = new IOException(String.format("[%s] JMX connection closed. You should check server log for repair status of keyspace %s(Subsequent keyspaces are not going to be repaired).", new Object[]{this.format.format(Long.valueOf(timestamp)), this.keyspace}));
      this.condition.signalAll();
   }

   public void progress(String tag, ProgressEvent event) {
      ProgressEventType type = event.getType();
      String message = String.format("[%s] %s", new Object[]{this.format.format(Long.valueOf(System.currentTimeMillis())), event.getMessage()});
      if(type == ProgressEventType.PROGRESS) {
         message = message + " (progress: " + (int)event.getProgressPercentage() + "%)";
      }

      this.out.println(message);
      if(type == ProgressEventType.ERROR) {
         this.error = new RuntimeException("Repair job has failed with the error message: " + message);
      }

      if(type == ProgressEventType.COMPLETE) {
         this.condition.signalAll();
      }

   }

   private void queryForCompletedRepair(String triggeringCondition) {
      List<String> status = this.ssProxy.getParentRepairStatus(this.cmd.intValue());
      String queriedString = "queried for parent session status and";
      if(status == null) {
         String message = String.format("[%s] %s %s couldn't find repair status for cmd: %s", new Object[]{triggeringCondition, queriedString, this.format.format(Long.valueOf(System.currentTimeMillis())), this.cmd});
         this.out.println(message);
      } else {
         ActiveRepairService.ParentRepairStatus parentRepairStatus = ActiveRepairService.ParentRepairStatus.valueOf((String)status.get(0));
         List<String> messages = status.subList(1, status.size());
         switch(null.$SwitchMap$org$apache$cassandra$service$ActiveRepairService$ParentRepairStatus[parentRepairStatus.ordinal()]) {
         case 1:
         case 2:
            this.out.println(String.format("[%s] %s %s discovered repair %s.", new Object[]{this.format.format(Long.valueOf(System.currentTimeMillis())), triggeringCondition, queriedString, parentRepairStatus.name().toLowerCase()}));
            if(parentRepairStatus == ActiveRepairService.ParentRepairStatus.FAILED) {
               this.error = new IOException((String)messages.get(0));
            }

            this.printMessages(messages);
            this.condition.signalAll();
         case 3:
            break;
         default:
            this.out.println(String.format("[%s] WARNING Encountered unexpected RepairRunnable.ParentRepairStatus: %s", new Object[]{Long.valueOf(System.currentTimeMillis()), parentRepairStatus}));
            this.printMessages(messages);
         }
      }

   }

   private void printMessages(List<String> messages) {
      Iterator var2 = messages.iterator();

      while(var2.hasNext()) {
         String message = (String)var2.next();
         this.out.println(String.format("[%s] %s", new Object[]{this.format.format(Long.valueOf(System.currentTimeMillis())), message}));
      }

   }
}
