package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(
   name = "viewbuildstatus",
   description = "Show progress of a materialized view build"
)
public class ViewBuildStatus extends NodeTool.NodeToolCmd {
   private static final String SUCCESS = "SUCCESS";
   @Arguments(
      usage = "<keyspace> <view> | <keyspace.view>",
      description = "The keyspace and view name"
   )
   private List<String> args = new ArrayList();

   public ViewBuildStatus() {
   }

   protected void execute(NodeProbe probe) {
      if(this.args.isEmpty()) {
         Iterator var2 = probe.getKeyspacesAndViews().entrySet().iterator();

         while(var2.hasNext()) {
            Entry<String, List<String>> ksViews = (Entry)var2.next();
            Iterator var4 = ((List)ksViews.getValue()).iterator();

            while(var4.hasNext()) {
               String table = (String)var4.next();
               this.viewBuildStatus(probe, (String)ksViews.getKey(), table);
            }
         }
      } else {
         String keyspace = null;
         String view = null;
         if(this.args.size() == 2) {
            keyspace = (String)this.args.get(0);
            view = (String)this.args.get(1);
         } else if(this.args.size() == 1) {
            String[] input = ((String)this.args.get(0)).split("\\.");
            Preconditions.checkArgument(input.length == 2, "viewbuildstatus requires keyspace and view name arguments");
            keyspace = input[0];
            view = input[1];
         } else {
            Preconditions.checkArgument(false, "viewbuildstatus requires keyspace and view name arguments");
         }

         System.exit(this.viewBuildStatus(probe, keyspace, view)?0:1);
      }

   }

   private boolean viewBuildStatus(NodeProbe probe, String keyspace, String view) {
      Map<String, String> buildStatus = probe.getViewBuildStatuses(keyspace, view);
      boolean failed = false;
      TableBuilder builder = new TableBuilder();
      builder.add(new String[]{"Host", "Info"});

      Entry status;
      for(Iterator var7 = buildStatus.entrySet().iterator(); var7.hasNext(); builder.add(new String[]{(String)status.getKey(), (String)status.getValue()})) {
         status = (Entry)var7.next();
         if(!((String)status.getValue()).equals("SUCCESS")) {
            failed = true;
         }
      }

      if(failed) {
         System.out.println(String.format("%s.%s has not finished building; node status is below.", new Object[]{keyspace, view}));
         System.out.println();
         builder.printTo(System.out);
         return false;
      } else {
         System.out.println(String.format("%s.%s has finished building", new Object[]{keyspace, view}));
         return true;
      }
   }
}
