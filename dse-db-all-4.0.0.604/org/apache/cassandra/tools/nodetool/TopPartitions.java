package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.commons.lang3.StringUtils;

@Command(
   name = "toppartitions",
   description = "Sample and print the most active partitions for a given column family"
)
public class TopPartitions extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <cfname> <duration>",
      description = "The keyspace, column family name, and duration in milliseconds"
   )
   private List<String> args = new ArrayList();
   @Option(
      name = {"-s"},
      description = "Capacity of stream summary, closer to the actual cardinality of partitions will yield more accurate results (Default: 256)"
   )
   private int size = 256;
   @Option(
      name = {"-k"},
      description = "Number of the top partitions to list (Default: 10)"
   )
   private int topCount = 10;
   @Option(
      name = {"-a"},
      description = "Comma separated list of samplers to use (Default: all)"
   )
   private String samplers = StringUtils.join(TableMetrics.Sampler.values(), ',');

   public TopPartitions() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.topCount < this.size, "TopK count (-k) option must be smaller then the summary capacity (-s)");
      if(this.args.size() == 1) {
         this.topParittionsForAllTables(probe);
      } else {
         Preconditions.checkArgument(this.args.size() == 3, "toppartitions requires keyspace, column family name, and duration");
         String keyspace = (String)this.args.get(0);
         String cfname = (String)this.args.get(1);
         Integer duration = Integer.valueOf((String)this.args.get(2));
         ColumnFamilyStoreMBean cfProxy = probe.getCfsProxy(keyspace, cfname);
         System.out.print(this.topPartitions(probe, cfProxy, keyspace, cfname, duration));
      }
   }

   private void topParittionsForAllTables(NodeProbe probe) {
      Integer duration = Integer.valueOf((String)this.args.get(0));
      ExecutorService executorService = Executors.newCachedThreadPool();

      try {
         List<Future<String>> futures = new ArrayList();
         Iterator cfProxyIter = probe.getColumnFamilyStoreMBeanProxies();

         while(cfProxyIter.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = (Entry)cfProxyIter.next();
            String ks = (String)entry.getKey();
            ColumnFamilyStoreMBean cfProxy = (ColumnFamilyStoreMBean)entry.getValue();
            String table = cfProxy.getTableName();
            futures.add(executorService.submit(() -> {
               return this.topPartitions(probe, cfProxy, ks, table, duration);
            }));
         }

         cfProxyIter = futures.iterator();

         while(cfProxyIter.hasNext()) {
            Future f = (Future)cfProxyIter.next();

            try {
               System.out.print((String)f.get());
            } catch (InterruptedException var14) {
               Thread.currentThread().interrupt();
               break;
            } catch (ExecutionException var15) {
               throw new RuntimeException(var15.getCause());
            }
         }
      } finally {
         executorService.shutdown();
      }

   }

   private String topPartitions(NodeProbe probe, ColumnFamilyStoreMBean cfProxy, String keyspace, String cfname, Integer duration) {
      List<TableMetrics.Sampler> targets = Lists.newArrayList();
      String[] var7 = this.samplers.split(",");
      int var8 = var7.length;

      for(int var9 = 0; var9 < var8; ++var9) {
         String s = var7[var9];

         try {
            targets.add(TableMetrics.Sampler.valueOf(s.toUpperCase()));
         } catch (Exception var29) {
            throw new IllegalArgumentException(s + " is not a valid sampler, choose one of: " + StringUtils.join(TableMetrics.Sampler.values(), ", "));
         }
      }

      Map results;
      try {
         results = probe.getPartitionSample(keyspace, cfname, cfProxy, this.size, duration.intValue(), this.topCount, targets);
      } catch (OpenDataException var28) {
         throw new RuntimeException(var28);
      }

      boolean first = true;
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      Throwable var11 = null;

      try {
         pw.println("\nKeyspace/table: " + keyspace + '/' + cfname);

         for(Iterator var12 = results.entrySet().iterator(); var12.hasNext(); first = false) {
            Entry<TableMetrics.Sampler, CompositeData> result = (Entry)var12.next();
            CompositeData sampling = (CompositeData)result.getValue();
            List<CompositeData> topk = (List)Lists.newArrayList(((TabularDataSupport)sampling.get("partitions")).values());
            topk.sort(new Ordering<CompositeData>() {
               public int compare(CompositeData left, CompositeData right) {
                  return Long.compare(((Long)right.get("count")).longValue(), ((Long)left.get("count")).longValue());
               }
            });
            if(!first) {
               pw.println();
            }

            pw.println(((TableMetrics.Sampler)result.getKey()).toString() + " Sampler:");
            pw.printf("  Cardinality: ~%d (%d capacity)%n", new Object[]{sampling.get("cardinality"), Integer.valueOf(this.size)});
            pw.printf("  Top %d partitions:%n", new Object[]{Integer.valueOf(this.topCount)});
            if(topk.size() == 0) {
               pw.println("\tNothing recorded during sampling period...");
            } else {
               int offset = 0;

               Iterator var17;
               CompositeData entry;
               for(var17 = topk.iterator(); var17.hasNext(); offset = Math.max(offset, entry.get("string").toString().length())) {
                  entry = (CompositeData)var17.next();
               }

               pw.printf("\t%-" + offset + "s%10s%10s%n", new Object[]{"Partition", "Count", "+/-"});
               var17 = topk.iterator();

               while(var17.hasNext()) {
                  entry = (CompositeData)var17.next();
                  pw.printf("\t%-" + offset + "s%10d%10d%n", new Object[]{entry.get("string").toString(), entry.get("count"), entry.get("error")});
               }
            }
         }
      } catch (Throwable var30) {
         var11 = var30;
         throw var30;
      } finally {
         if(pw != null) {
            if(var11 != null) {
               try {
                  pw.close();
               } catch (Throwable var27) {
                  var11.addSuppressed(var27);
               }
            } else {
               pw.close();
            }
         }

      }

      return sw.toString();
   }
}
