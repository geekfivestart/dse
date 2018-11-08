package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.FBUtilities;

@Command(
   name = "repair_admin",
   description = "list and fail incremental repair sessions"
)
public class RepairAdmin extends NodeTool.NodeToolCmd {
   @Option(
      title = "list",
      name = {"-l", "--list"},
      description = "list repair sessions (default behavior)"
   )
   private boolean list = false;
   @Option(
      title = "all",
      name = {"-a", "--all"},
      description = "include completed and failed sessions"
   )
   private boolean all = false;
   @Option(
      title = "cancel",
      name = {"-x", "--cancel"},
      description = "cancel an incremental repair session"
   )
   private String cancel = null;
   @Option(
      title = "force",
      name = {"-f", "--force"},
      description = "cancel repair session from a node other than the repair coordinator. Attempting to cancel FINALIZED or FAILED sessions is an error."
   )
   private boolean force = false;
   private static final List<String> header = Lists.newArrayList(new String[]{"id", "state", "last activity", "coordinator", "participants"});

   public RepairAdmin() {
   }

   private List<String> sessionValues(Map<String, String> session, int now) {
      int updated = Integer.parseInt((String)session.get("LAST_UPDATE"));
      return Lists.newArrayList(new String[]{(String)session.get("SESSION_ID"), (String)session.get("STATE"), Integer.toString(now - updated) + " (s)", (String)session.get("COORDINATOR"), (String)session.get("PARTICIPANTS")});
   }

   private void listSessions(ActiveRepairServiceMBean repairServiceProxy) {
      Preconditions.checkArgument(this.cancel == null);
      Preconditions.checkArgument(!this.force, "-f/--force only valid for session cancel");
      List<Map<String, String>> sessions = repairServiceProxy.getSessions(this.all);
      if(sessions.isEmpty()) {
         System.out.println("no sessions");
      } else {
         List<List<String>> rows = new ArrayList();
         rows.add(header);
         int now = FBUtilities.nowInSeconds();
         Iterator var5 = sessions.iterator();

         while(var5.hasNext()) {
            Map<String, String> session = (Map)var5.next();
            rows.add(this.sessionValues(session, now));
         }

         int[] widths = new int[header.size()];
         Iterator var12 = rows.iterator();

         while(var12.hasNext()) {
            List<String> row = (List)var12.next();

            assert row.size() == widths.length;

            for(int i = 0; i < widths.length; ++i) {
               widths[i] = Math.max(widths[i], ((String)row.get(i)).length());
            }
         }

         List<String> fmts = new ArrayList(widths.length);

         for(int i = 0; i < widths.length; ++i) {
            fmts.add("%-" + Integer.toString(widths[i]) + "s");
         }

         Iterator var15 = rows.iterator();

         while(var15.hasNext()) {
            List<String> row = (List)var15.next();
            List<String> formatted = new ArrayList(row.size());

            for(int i = 0; i < widths.length; ++i) {
               formatted.add(String.format((String)fmts.get(i), new Object[]{row.get(i)}));
            }

            System.out.println(Joiner.on(" | ").join(formatted));
         }
      }

   }

   private void cancelSession(ActiveRepairServiceMBean repairServiceProxy) {
      Preconditions.checkArgument(!this.list);
      Preconditions.checkArgument(!this.all, "-a/--all only valid for session list");
      repairServiceProxy.failSession(this.cancel, this.force);
   }

   protected void execute(NodeProbe probe) {
      if(this.list && this.cancel != null) {
         throw new RuntimeException("Can either list, or cancel sessions, not both");
      } else {
         if(this.cancel != null) {
            this.cancelSession(probe.getRepairServiceProxy());
         } else {
            this.listSessions(probe.getRepairServiceProxy());
         }

      }
   }
}
