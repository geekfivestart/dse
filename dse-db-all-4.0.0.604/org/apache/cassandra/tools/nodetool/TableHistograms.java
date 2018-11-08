package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang3.ArrayUtils;

@Command(
   name = "tablehistograms",
   description = "Print statistic histograms for a given table"
)
public class TableHistograms extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table> | <keyspace.table>",
      description = "The keyspace and table name"
   )
   private List<String> args = new ArrayList();

   public TableHistograms() {
   }

   public void execute(NodeProbe probe) {
      String keyspace;
      String table;
      if(this.args.size() == 2) {
         keyspace = (String)this.args.get(0);
         table = (String)this.args.get(1);
         this.tableHistograms(probe, keyspace, table);
      } else if(this.args.size() == 1) {
         String[] input = ((String)this.args.get(0)).split("\\.");
         Preconditions.checkArgument(input.length == 2, "tablehistograms requires keyspace and table name arguments");
         keyspace = input[0];
         table = input[1];
         this.tableHistograms(probe, keyspace, table);
      } else {
         Iterator cfIter = probe.getColumnFamilyStoreMBeanProxies();

         while(cfIter.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = (Entry)cfIter.next();
            String ks = (String)entry.getKey();
            String tab = ((ColumnFamilyStoreMBean)entry.getValue()).getTableName();
            this.tableHistograms(probe, ks, tab);
         }
      }

   }

   private void tableHistograms(NodeProbe probe, String keyspace, String table) {
      long[] estimatedPartitionSize = (long[])((long[])probe.getColumnFamilyMetric(keyspace, table, "EstimatedPartitionSizeHistogram"));
      long[] estimatedColumnCount = (long[])((long[])probe.getColumnFamilyMetric(keyspace, table, "EstimatedColumnCountHistogram"));
      double[] estimatedRowSizePercentiles = new double[7];
      double[] estimatedColumnCountPercentiles = new double[7];
      double[] offsetPercentiles = new double[]{0.5D, 0.75D, 0.95D, 0.98D, 0.99D};
      if(!ArrayUtils.isEmpty(estimatedPartitionSize) && !ArrayUtils.isEmpty(estimatedColumnCount)) {
         EstimatedHistogram partitionSizeHist = new EstimatedHistogram(estimatedPartitionSize);
         EstimatedHistogram columnCountHist = new EstimatedHistogram(estimatedColumnCount);
         int i;
         if(partitionSizeHist.isOverflowed()) {
            System.err.println(String.format("Row sizes are larger than %s, unable to calculate percentiles", new Object[]{Long.valueOf(partitionSizeHist.getLargestBucketOffset())}));

            for(i = 0; i < offsetPercentiles.length; ++i) {
               estimatedRowSizePercentiles[i] = 0.0D / 0.0;
            }
         } else {
            for(i = 0; i < offsetPercentiles.length; ++i) {
               estimatedRowSizePercentiles[i] = (double)partitionSizeHist.percentile(offsetPercentiles[i]);
            }
         }

         if(columnCountHist.isOverflowed()) {
            System.err.println(String.format("Column counts are larger than %s, unable to calculate percentiles", new Object[]{Long.valueOf(columnCountHist.getLargestBucketOffset())}));

            for(i = 0; i < estimatedColumnCountPercentiles.length; ++i) {
               estimatedColumnCountPercentiles[i] = 0.0D / 0.0;
            }
         } else {
            for(i = 0; i < offsetPercentiles.length; ++i) {
               estimatedColumnCountPercentiles[i] = (double)columnCountHist.percentile(offsetPercentiles[i]);
            }
         }

         estimatedRowSizePercentiles[5] = (double)partitionSizeHist.min();
         estimatedColumnCountPercentiles[5] = (double)columnCountHist.min();
         estimatedRowSizePercentiles[6] = (double)partitionSizeHist.max();
         estimatedColumnCountPercentiles[6] = (double)columnCountHist.max();
      } else {
         System.err.println("No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles");

         for(int i = 0; i < 7; ++i) {
            estimatedRowSizePercentiles[i] = 0.0D / 0.0;
            estimatedColumnCountPercentiles[i] = 0.0D / 0.0;
         }
      }

      String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
      double[] readLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspace, table, "ReadLatency"));
      double[] writeLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspace, table, "WriteLatency"));
      double[] sstablesPerRead = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxHistogramMBean)probe.getColumnFamilyMetric(keyspace, table, "SSTablesPerReadHistogram"));
      System.out.println(String.format("%s/%s histograms", new Object[]{keyspace, table}));
      System.out.println(String.format("%-10s%10s%18s%18s%18s%18s", new Object[]{"Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"}));
      System.out.println(String.format("%-10s%10s%18s%18s%18s%18s", new Object[]{"", "", "(micros)", "(micros)", "(bytes)", ""}));

      for(int i = 0; i < percentiles.length; ++i) {
         System.out.println(String.format("%-10s%10.2f%18.2f%18.2f%18.0f%18.0f", new Object[]{percentiles[i], Double.valueOf(sstablesPerRead[i]), Double.valueOf(writeLatency[i]), Double.valueOf(readLatency[i]), Double.valueOf(estimatedRowSizePercentiles[i]), Double.valueOf(estimatedColumnCountPercentiles[i])}));
      }

      System.out.println();
   }
}
