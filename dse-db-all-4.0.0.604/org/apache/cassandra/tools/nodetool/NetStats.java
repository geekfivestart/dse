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

   public void execute(NodeProbe probe) {
      System.out.printf("Mode: %s%n", new Object[]{probe.getOperationMode()});
      Set<StreamState> statuses = probe.getStreamStatus();
      if(statuses.isEmpty()) {
         System.out.println("Not sending any streams.");
      }

      Iterator var3 = statuses.iterator();

      label146:
      while(var3.hasNext()) {
         StreamState status = (StreamState)var3.next();
         System.out.printf("%s %s%n", new Object[]{status.streamOperation.getDescription(), status.planId.toString()});
         Iterator var5 = status.sessions.iterator();

         while(true) {
            SessionInfo info;
            Iterator var7;
            ProgressInfo progress;
            do {
               if(!var5.hasNext()) {
                  continue label146;
               }

               info = (SessionInfo)var5.next();
               System.out.printf("    %s", new Object[]{info.peer.toString()});
               if(!info.peer.equals(info.connecting)) {
                  System.out.printf(" (using %s)", new Object[]{info.connecting.toString()});
               }

               System.out.printf("%n", new Object[0]);
               if(!info.receivingSummaries.isEmpty()) {
                  if(this.humanReadable) {
                     System.out.printf("        Receiving %d files, %s total. Already received %d files, %s total%n", new Object[]{Long.valueOf(info.getTotalFilesToReceive()), FileUtils.stringifyFileSize((double)info.getTotalSizeToReceive()), Long.valueOf(info.getTotalFilesReceived()), FileUtils.stringifyFileSize((double)info.getTotalSizeReceived())});
                  } else {
                     System.out.printf("        Receiving %d files, %d bytes total. Already received %d files, %d bytes total%n", new Object[]{Long.valueOf(info.getTotalFilesToReceive()), Long.valueOf(info.getTotalSizeToReceive()), Long.valueOf(info.getTotalFilesReceived()), Long.valueOf(info.getTotalSizeReceived())});
                  }

                  var7 = info.getReceivingFiles().iterator();

                  while(var7.hasNext()) {
                     progress = (ProgressInfo)var7.next();
                     System.out.printf("            %s%n", new Object[]{progress.toString()});
                  }
               }
            } while(info.sendingSummaries.isEmpty());

            if(this.humanReadable) {
               System.out.printf("        Sending %d files, %s total. Already sent %d files, %s total%n", new Object[]{Long.valueOf(info.getTotalFilesToSend()), FileUtils.stringifyFileSize((double)info.getTotalSizeToSend()), Long.valueOf(info.getTotalFilesSent()), FileUtils.stringifyFileSize((double)info.getTotalSizeSent())});
            } else {
               System.out.printf("        Sending %d files, %d bytes total. Already sent %d files, %d bytes total%n", new Object[]{Long.valueOf(info.getTotalFilesToSend()), Long.valueOf(info.getTotalSizeToSend()), Long.valueOf(info.getTotalFilesSent()), Long.valueOf(info.getTotalSizeSent())});
            }

            var7 = info.getSendingFiles().iterator();

            while(var7.hasNext()) {
               progress = (ProgressInfo)var7.next();
               System.out.printf("            %s%n", new Object[]{progress.toString()});
            }
         }
      }

      if(!probe.isStarting()) {
         System.out.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", new Object[]{Long.valueOf(probe.getReadRepairAttempted()), Long.valueOf(probe.getReadRepairRepairedBlocking()), Long.valueOf(probe.getReadRepairRepairedBackground())});
         MessagingServiceMBean ms = probe.msProxy;
         System.out.printf("%-25s", new Object[]{"Pool Name"});
         System.out.printf("%10s", new Object[]{"Active"});
         System.out.printf("%10s", new Object[]{"Pending"});
         System.out.printf("%15s", new Object[]{"Completed"});
         System.out.printf("%10s%n", new Object[]{"Dropped"});
         int pending = 0;

         Iterator var9;
         int n;
         for(var9 = ms.getLargeMessagePendingTasks().values().iterator(); var9.hasNext(); pending += n) {
            n = ((Integer)var9.next()).intValue();
         }

         long completed = 0L;

         long n;
         for(var9 = ms.getLargeMessageCompletedTasks().values().iterator(); var9.hasNext(); completed += n) {
            n = ((Long)var9.next()).longValue();
         }

         long dropped = 0L;

         for(var9 = ms.getLargeMessageDroppedTasks().values().iterator(); var9.hasNext(); dropped += n) {
            n = ((Long)var9.next()).longValue();
         }

         System.out.printf("%-25s%10s%10s%15s%10s%n", new Object[]{"Large messages", "n/a", Integer.valueOf(pending), Long.valueOf(completed), Long.valueOf(dropped)});
         pending = 0;

         for(var9 = ms.getSmallMessagePendingTasks().values().iterator(); var9.hasNext(); pending += n) {
            n = ((Integer)var9.next()).intValue();
         }

         completed = 0L;

         for(var9 = ms.getSmallMessageCompletedTasks().values().iterator(); var9.hasNext(); completed += n) {
            n = ((Long)var9.next()).longValue();
         }

         dropped = 0L;

         for(var9 = ms.getSmallMessageDroppedTasks().values().iterator(); var9.hasNext(); dropped += n) {
            n = ((Long)var9.next()).longValue();
         }

         System.out.printf("%-25s%10s%10s%15s%10s%n", new Object[]{"Small messages", "n/a", Integer.valueOf(pending), Long.valueOf(completed), Long.valueOf(dropped)});
         pending = 0;

         for(var9 = ms.getGossipMessagePendingTasks().values().iterator(); var9.hasNext(); pending += n) {
            n = ((Integer)var9.next()).intValue();
         }

         completed = 0L;

         for(var9 = ms.getGossipMessageCompletedTasks().values().iterator(); var9.hasNext(); completed += n) {
            n = ((Long)var9.next()).longValue();
         }

         dropped = 0L;

         for(var9 = ms.getGossipMessageDroppedTasks().values().iterator(); var9.hasNext(); dropped += n) {
            n = ((Long)var9.next()).longValue();
         }

         System.out.printf("%-25s%10s%10s%15s%10s%n", new Object[]{"Gossip messages", "n/a", Integer.valueOf(pending), Long.valueOf(completed), Long.valueOf(dropped)});
      }

   }
}
