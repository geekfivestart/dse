package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "netstats",
   description = "Print network information on provided host (connecting node by default)"
)
public class NetStats extends NodeTool.NodeToolCmd {
   @Option(
      title = "human_readable",
      name = {"-H", "--human-readable"},
      description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"
   )
   private boolean humanReadable = false;

   public NetStats() {
   }

   public void execute(final NodeProbe probe) {
      System.out.printf("Mode: %s%n", probe.getOperationMode());
      final Set<StreamState> statuses = probe.getStreamStatus();
      if (statuses.isEmpty()) {
         System.out.println("Not sending any streams.");
      }
      for (final StreamState status : statuses) {
         System.out.printf("%s %s%n", status.streamOperation.getDescription(), status.planId.toString());
         for (final SessionInfo info : status.sessions) {
            System.out.printf("    %s", info.peer.toString());
            if (!info.peer.equals(info.connecting)) {
               System.out.printf(" (using %s)", info.connecting.toString());
            }
            System.out.printf("%n", new Object[0]);
            if (!info.receivingSummaries.isEmpty()) {
               if (this.humanReadable) {
                  System.out.printf("        Receiving %d files, %s total. Already received %d files, %s total%n", info.getTotalFilesToReceive(), FileUtils.stringifyFileSize(info.getTotalSizeToReceive()), info.getTotalFilesReceived(), FileUtils.stringifyFileSize(info.getTotalSizeReceived()));
               }
               else {
                  System.out.printf("        Receiving %d files, %d bytes total. Already received %d files, %d bytes total%n", info.getTotalFilesToReceive(), info.getTotalSizeToReceive(), info.getTotalFilesReceived(), info.getTotalSizeReceived());
               }
               for (final ProgressInfo progress : info.getReceivingFiles()) {
                  System.out.printf("            %s%n", progress.toString());
               }
            }
            if (!info.sendingSummaries.isEmpty()) {
               if (this.humanReadable) {
                  System.out.printf("        Sending %d files, %s total. Already sent %d files, %s total%n", info.getTotalFilesToSend(), FileUtils.stringifyFileSize(info.getTotalSizeToSend()), info.getTotalFilesSent(), FileUtils.stringifyFileSize(info.getTotalSizeSent()));
               }
               else {
                  System.out.printf("        Sending %d files, %d bytes total. Already sent %d files, %d bytes total%n", info.getTotalFilesToSend(), info.getTotalSizeToSend(), info.getTotalFilesSent(), info.getTotalSizeSent());
               }
               for (final ProgressInfo progress : info.getSendingFiles()) {
                  System.out.printf("            %s%n", progress.toString());
               }
            }
         }
      }
      if (!probe.isStarting()) {
         System.out.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());
         final MessagingServiceMBean ms = probe.msProxy;
         System.out.printf("%-25s", "Pool Name");
         System.out.printf("%10s", "Active");
         System.out.printf("%10s", "Pending");
         System.out.printf("%15s", "Completed");
         System.out.printf("%10s%n", "Dropped");
         int pending = 0;
         for (final int n : ms.getLargeMessagePendingTasks().values()) {
            pending += n;
         }
         long completed = 0L;
         for (final long n2 : ms.getLargeMessageCompletedTasks().values()) {
            completed += n2;
         }
         long dropped = 0L;
         for (final long n2 : ms.getLargeMessageDroppedTasks().values()) {
            dropped += n2;
         }
         System.out.printf("%-25s%10s%10s%15s%10s%n", "Large messages", "n/a", pending, completed, dropped);
         pending = 0;
         for (final int n : ms.getSmallMessagePendingTasks().values()) {
            pending += n;
         }
         completed = 0L;
         for (final long n2 : ms.getSmallMessageCompletedTasks().values()) {
            completed += n2;
         }
         dropped = 0L;
         for (final long n2 : ms.getSmallMessageDroppedTasks().values()) {
            dropped += n2;
         }
         System.out.printf("%-25s%10s%10s%15s%10s%n", "Small messages", "n/a", pending, completed, dropped);
         pending = 0;
         for (final int n : ms.getGossipMessagePendingTasks().values()) {
            pending += n;
         }
         completed = 0L;
         for (final long n2 : ms.getGossipMessageCompletedTasks().values()) {
            completed += n2;
         }
         dropped = 0L;
         for (final long n2 : ms.getGossipMessageDroppedTasks().values()) {
            dropped += n2;
         }
         System.out.printf("%-25s%10s%10s%15s%10s%n", "Gossip messages", "n/a", pending, completed, dropped);
      }
   }

}
