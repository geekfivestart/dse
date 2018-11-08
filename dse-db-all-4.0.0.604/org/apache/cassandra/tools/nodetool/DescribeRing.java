package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "describering",
   description = "Shows the token ranges info of a given keyspace"
)
public class DescribeRing extends NodeTool.NodeToolCmd {
   @Arguments(
      description = "The keyspace name(s)"
   )
   List<String> keyspace = new ArrayList();

   public DescribeRing() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Schema Version:" + probe.getSchemaVersion());

      try {
         if(this.keyspace.isEmpty()) {
            this.keyspace.addAll(probe.getNonLocalStrategyKeyspaces());
         }

         Iterator var2 = this.keyspace.iterator();

         while(var2.hasNext()) {
            String ks = (String)var2.next();
            this.forKeyspace(probe, ks);
         }

      } catch (IOException var4) {
         throw new RuntimeException(var4);
      }
   }

   private void forKeyspace(NodeProbe probe, String ksName) throws IOException {
      System.out.println("Keyspace: " + ksName);
      System.out.println("TokenRange: ");
      Iterator var3 = probe.describeRing(ksName).iterator();

      while(var3.hasNext()) {
         String tokenRangeString = (String)var3.next();
         System.out.println("\t" + tokenRangeString);
      }

   }
}
