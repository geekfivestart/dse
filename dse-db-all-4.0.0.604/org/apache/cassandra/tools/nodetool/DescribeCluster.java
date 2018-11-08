package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "describecluster",
   description = "Print the name, snitch, partitioner and schema version of a cluster"
)
public class DescribeCluster extends NodeTool.NodeToolCmd {
   public DescribeCluster() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Cluster Information:");
      System.out.println("\tName: " + probe.getClusterName());
      String snitch = probe.getEndpointSnitchInfoProxy().getDisplayName();
      boolean dynamicSnitchEnabled = probe.getEndpointSnitchInfoProxy().isDynamicSnitch();
      System.out.println("\tSnitch: " + snitch);
      System.out.println("\tDynamicEndPointSnitch: " + (dynamicSnitchEnabled?"enabled":"disabled"));
      System.out.println("\tPartitioner: " + probe.getPartitioner());
      System.out.println("\tSchema versions:");
      Map<String, List<String>> schemaVersions = probe.getSpProxy().getSchemaVersions();
      Iterator var5 = schemaVersions.keySet().iterator();

      while(var5.hasNext()) {
         String version = (String)var5.next();
         System.out.println(String.format("\t\t%s: %s%n", new Object[]{version, schemaVersions.get(version)}));
      }

   }
}
