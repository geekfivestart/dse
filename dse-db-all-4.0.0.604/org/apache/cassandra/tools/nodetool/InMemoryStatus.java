package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "inmemorystatus",
   description = "Returns a list of the in-memory tables for this node and the amount of memory each table is using, or information about a single table if the keyspace and columnfamily are given."
)
public class InMemoryStatus extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <table>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();

   public InMemoryStatus() {
   }

   protected void execute(NodeProbe probe) {
      if(this.args.size() == 0) {
         this.printInmemoryInfo(probe.getMemoryOnlyStatusProxy());
      } else {
         if(this.args.size() != 2) {
            throw new IllegalArgumentException("inmemorystatus can be called with either no arguments (print all tables) or a keyspace/columnfamily pair.");
         }

         this.printInmemoryInfo(probe.getMemoryOnlyStatusProxy(), (String)this.args.get(0), (String)this.args.get(1));
      }

   }

   private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy) {
      this.printInmemoryInfo(proxy.getMemoryOnlyTableInformation(), proxy.getMemoryOnlyTotals());
   }

   private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy, String ks, String cf) {
      this.printInmemoryInfo(Collections.singletonList(proxy.getMemoryOnlyTableInformation(ks, cf)), proxy.getMemoryOnlyTotals());
   }

   private void printInmemoryInfo(List<MemoryOnlyStatusMXBean.TableInfo> infos, MemoryOnlyStatusMXBean.TotalInfo totals) {
      System.out.format("Max Memory to Lock:                    %10dMB\n", new Object[]{Long.valueOf(totals.getMaxMemoryToLock() / 1048576L)});
      System.out.format("Current Total Memory Locked:           %10dMB\n", new Object[]{Long.valueOf(totals.getUsed() / 1048576L)});
      System.out.format("Current Total Memory Not Able To Lock: %10dMB\n", new Object[]{Long.valueOf(totals.getNotAbleToLock() / 1048576L)});
      if(infos.size() > 0) {
         System.out.format("%-30s %-30s %12s %17s %7s\n", new Object[]{"Keyspace", "ColumnFamily", "Size", "Couldn't Lock", "Usage"});
         Iterator var3 = infos.iterator();

         while(var3.hasNext()) {
            MemoryOnlyStatusMXBean.TableInfo mi = (MemoryOnlyStatusMXBean.TableInfo)var3.next();
            System.out.format("%-30s %-30s %10dMB %15dMB %6.0f%%\n", new Object[]{mi.getKs(), mi.getCf(), Long.valueOf(mi.getUsed() / 1048576L), Long.valueOf(mi.getNotAbleToLock() / 1048576L), Double.valueOf(100.0D * (double)mi.getUsed() / (double)mi.getMaxMemoryToLock())});
         }
      } else {
         System.out.format("No MemoryOnlyStrategy tables found.\n", new Object[0]);
      }

   }
}
