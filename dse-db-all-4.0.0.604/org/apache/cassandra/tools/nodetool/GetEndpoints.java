package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getendpoints",
   description = "Print the end points that owns the key"
)
public class GetEndpoints extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table> <key>",
      description = "The keyspace, the table, and the partition key for which we need to find the endpoint"
   )
   private List<String> args = new ArrayList();

   public GetEndpoints() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 3, "getendpoints requires keyspace, table and partition key arguments");
      String ks = (String)this.args.get(0);
      String table = (String)this.args.get(1);
      String key = (String)this.args.get(2);
      List<InetAddress> endpoints = probe.getEndpoints(ks, table, key);
      Iterator var6 = endpoints.iterator();

      while(var6.hasNext()) {
         InetAddress endpoint = (InetAddress)var6.next();
         System.out.println(endpoint.getHostAddress());
      }

   }
}
