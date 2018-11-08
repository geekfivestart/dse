package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.utils.FBUtilities;

public class TableStatsPrinter {
   public TableStatsPrinter() {
   }

   public static StatsPrinter from(String format) {
      byte var2 = -1;
      switch(format.hashCode()) {
      case 3271912:
         if(format.equals("json")) {
            var2 = 0;
         }
         break;
      case 3701415:
         if(format.equals("yaml")) {
            var2 = 1;
         }
      }

      switch(var2) {
      case 0:
         return new StatsPrinter.JsonPrinter();
      case 1:
         return new StatsPrinter.YamlPrinter();
      default:
         return new TableStatsPrinter.DefaultPrinter();
      }
   }

   private static class DefaultPrinter implements StatsPrinter<TableStatsHolder> {
      private DefaultPrinter() {
      }

      public void print(TableStatsHolder data, PrintStream out) {
         out.println("Total number of tables: " + data.numberOfTables);
         out.println("----------------");
         List<StatsKeyspace> keyspaces = data.keyspaces;
         Iterator var4 = keyspaces.iterator();

         while(var4.hasNext()) {
            StatsKeyspace keyspace = (StatsKeyspace)var4.next();
            out.println("Keyspace : " + keyspace.name);
            out.println("\tRead Count: " + keyspace.readCount);
            out.println("\tRead Latency: " + keyspace.readLatency() + " ms");
            out.println("\tWrite Count: " + keyspace.writeCount);
            out.println("\tWrite Latency: " + keyspace.writeLatency() + " ms");
            out.println("\tPending Flushes: " + keyspace.pendingFlushes);
            List<StatsTable> tables = keyspace.tables;
            Iterator var7 = tables.iterator();

            while(var7.hasNext()) {
               StatsTable table = (StatsTable)var7.next();
               out.println("\t\tTable" + (table.isIndex?" (index): " + table.name:": ") + table.name);
               out.println("\t\tSSTable count: " + table.sstableCount);
               if(table.isLeveledSstable) {
                  out.println("\t\tSSTables in each level: [" + String.join(", ", table.sstablesInEachLevel) + "]");
               }

               out.println("\t\tSpace used (live): " + table.spaceUsedLive);
               out.println("\t\tSpace used (total): " + table.spaceUsedTotal);
               out.println("\t\tSpace used by snapshots (total): " + table.spaceUsedBySnapshotsTotal);
               if(table.offHeapUsed) {
                  out.println("\t\tOff heap memory used (total): " + table.offHeapMemoryUsedTotal);
               }

               out.println("\t\tSSTable Compression Ratio: " + table.sstableCompressionRatio);
               out.println("\t\tNumber of partitions (estimate): " + table.numberOfPartitionsEstimate);
               out.println("\t\tMemtable cell count: " + table.memtableCellCount);
               out.println("\t\tMemtable data size: " + table.memtableDataSize);
               if(table.memtableOffHeapUsed) {
                  out.println("\t\tMemtable off heap memory used: " + table.memtableOffHeapMemoryUsed);
               }

               out.println("\t\tMemtable switch count: " + table.memtableSwitchCount);
               out.println("\t\tLocal read count: " + table.localReadCount);
               out.printf("\t\tLocal read latency: %01.3f ms%n", new Object[]{Double.valueOf(table.localReadLatencyMs)});
               out.println("\t\tLocal write count: " + table.localWriteCount);
               out.printf("\t\tLocal write latency: %01.3f ms%n", new Object[]{Double.valueOf(table.localWriteLatencyMs)});
               out.println("\t\tPending flushes: " + table.pendingFlushes);
               out.println("\t\tPercent repaired: " + table.percentRepaired);
               out.println("\t\tBytes repaired: " + FBUtilities.prettyPrintMemory(table.bytesRepaired));
               out.println("\t\tBytes unrepaired: " + FBUtilities.prettyPrintMemory(table.bytesUnrepaired));
               out.println("\t\tBytes pending repair: " + FBUtilities.prettyPrintMemory(table.bytesPendingRepair));
               out.println("\t\tBloom filter false positives: " + table.bloomFilterFalsePositives);
               out.printf("\t\tBloom filter false ratio: %01.5f%n", new Object[]{table.bloomFilterFalseRatio});
               out.println("\t\tBloom filter space used: " + table.bloomFilterSpaceUsed);
               if(table.bloomFilterOffHeapUsed) {
                  out.println("\t\tBloom filter off heap memory used: " + table.bloomFilterOffHeapMemoryUsed);
               }

               if(table.indexSummaryOffHeapUsed) {
                  out.println("\t\tIndex summary off heap memory used: " + table.indexSummaryOffHeapMemoryUsed);
               }

               if(table.compressionMetadataOffHeapUsed) {
                  out.println("\t\tCompression metadata off heap memory used: " + table.compressionMetadataOffHeapMemoryUsed);
               }

               out.println("\t\tCompacted partition minimum bytes: " + table.compactedPartitionMinimumBytes);
               out.println("\t\tCompacted partition maximum bytes: " + table.compactedPartitionMaximumBytes);
               out.println("\t\tCompacted partition mean bytes: " + table.compactedPartitionMeanBytes);
               out.println("\t\tAverage live cells per slice (last five minutes): " + table.averageLiveCellsPerSliceLastFiveMinutes);
               out.println("\t\tMaximum live cells per slice (last five minutes): " + table.maximumLiveCellsPerSliceLastFiveMinutes);
               out.println("\t\tAverage tombstones per slice (last five minutes): " + table.averageTombstonesPerSliceLastFiveMinutes);
               out.println("\t\tMaximum tombstones per slice (last five minutes): " + table.maximumTombstonesPerSliceLastFiveMinutes);
               out.println("\t\tDropped Mutations: " + table.droppedMutations);
               out.println("\t\tFailed Replication Count: " + table.failedReplicationCount);
               out.println("");
            }

            out.println("----------------");
         }

      }
   }
}
