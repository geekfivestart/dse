package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "verify",
   description = "Verify (check data checksum for) one or more tables"
)
public class Verify extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "extended_verify",
      name = {"-e", "--extended-verify"},
      description = "Verify each cell data, beyond simply checking sstable checksums"
   )
   private boolean extendedVerify = false;

   public Verify() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();

         try {
            probe.verify(System.out, this.extendedVerify, keyspace, tableNames);
         } catch (Exception var7) {
            throw new RuntimeException("Error occurred during verifying", var7);
         }
      }

   }
}
