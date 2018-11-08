package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getcompactionthreshold",
   description = "Print min and max compaction thresholds for a given table"
)
public class GetCompactionThreshold extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table>",
      description = "The keyspace with a table"
   )
   private List<String> args = new ArrayList();

   public GetCompactionThreshold() {
   }

   public void execute(NodeProbe probe) {
      if(this.args.isEmpty()) {
         Iterator cfProxyIter = probe.getColumnFamilyStoreMBeanProxies();

         while(cfProxyIter.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = (Entry)cfProxyIter.next();
            String ks = (String)entry.getKey();
            ColumnFamilyStoreMBean cfProxy = (ColumnFamilyStoreMBean)entry.getValue();
            String table = cfProxy.getTableName();
            this.printInfo(ks, table, cfProxy);
         }
      } else {
         Preconditions.checkArgument(this.args.size() == 2, "getcompactionthreshold requires ks and cf args");
         String ks = (String)this.args.get(0);
         String cf = (String)this.args.get(1);
         ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
         this.printInfo(ks, cf, cfsProxy);
      }

   }

   private void printInfo(String ks, String cf, ColumnFamilyStoreMBean cfsProxy) {
      System.out.println("Current compaction thresholds for " + ks + '/' + cf + ": \n min = " + cfsProxy.getMinimumCompactionThreshold() + ",  max = " + cfsProxy.getMaximumCompactionThreshold());
   }
}
