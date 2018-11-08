package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import javax.management.openmbean.TabularData;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(
   name = "listsnapshots",
   description = "Lists all the snapshots along with the size on disk and true size."
)
public class ListSnapshots extends NodeTool.NodeToolCmd {
   public ListSnapshots() {
   }

   public void execute(NodeProbe probe) {
      try {
         System.out.println("Snapshot Details: ");
         Map<String, TabularData> snapshotDetails = probe.getSnapshotDetails();
         if(snapshotDetails.isEmpty()) {
            System.out.println("There are no snapshots");
         } else {
            long trueSnapshotsSize = probe.trueSnapshotsSize();
            TableBuilder table = new TableBuilder();
            List<String> indexNames = ((TabularData)((Entry)snapshotDetails.entrySet().iterator().next()).getValue()).getTabularType().getIndexNames();
            table.add((String[])indexNames.toArray(new String[0]));
            Iterator var7 = snapshotDetails.entrySet().iterator();

            while(var7.hasNext()) {
               Entry<String, TabularData> snapshotDetail = (Entry)var7.next();
               Set<?> values = ((TabularData)snapshotDetail.getValue()).keySet();
               Iterator var10 = values.iterator();

               while(var10.hasNext()) {
                  Object eachValue = var10.next();
                  List<?> value = (List)eachValue;
                  table.add((String[])value.toArray(new String[0]));
               }
            }

            table.printTo(System.out);
            System.out.println("\nTotal TrueDiskSpaceUsed: " + FileUtils.stringifyFileSize((double)trueSnapshotsSize) + "\n");
         }
      } catch (Exception var13) {
         throw new RuntimeException("Error during list snapshot", var13);
      }
   }
}
