package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getsstables",
   description = "Print the sstable filenames that own the key"
)
public class GetSSTables extends NodeTool.NodeToolCmd {
   @Option(
      title = "hex_format",
      name = {"-hf", "--hex-format"},
      description = "Specify the key in hexadecimal string format"
   )
   private boolean hexFormat = false;
   @Arguments(
      usage = "<keyspace> <cfname> <key>",
      description = "The keyspace, the column family, and the key"
   )
   private List<String> args = new ArrayList();

   public GetSSTables() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 3, "getsstables requires ks, cf and key args");
      String ks = (String)this.args.get(0);
      String cf = (String)this.args.get(1);
      String key = (String)this.args.get(2);
      List<String> sstables = probe.getSSTables(ks, cf, key, this.hexFormat);
      Iterator var6 = sstables.iterator();

      while(var6.hasNext()) {
         String sstable = (String)var6.next();
         System.out.println(sstable);
      }

   }
}
