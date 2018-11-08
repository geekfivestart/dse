package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import java.util.List;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "failuredetector",
   description = "Shows the failure detector information for the cluster"
)
public class FailureDetectorInfo extends NodeTool.NodeToolCmd {
   public FailureDetectorInfo() {
   }

   public void execute(NodeProbe probe) {
      TabularData data = probe.getFailureDetectorPhilValues();
      System.out.printf("%10s,%16s%n", new Object[]{"Endpoint", "Phi"});
      Iterator var3 = data.keySet().iterator();

      while(var3.hasNext()) {
         Object o = var3.next();
         CompositeData datum = data.get(((List)o).toArray());
         System.out.printf("%10s,%16.8f%n", new Object[]{datum.get("Endpoint"), datum.get("PHI")});
      }

   }
}
