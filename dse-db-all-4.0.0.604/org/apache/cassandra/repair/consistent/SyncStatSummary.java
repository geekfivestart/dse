package org.apache.cassandra.repair.consistent;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class SyncStatSummary {
   private Map<Pair<String, String>, SyncStatSummary.Table> summaries = new HashMap();
   private final boolean isEstimate;
   private int files = -1;
   private long bytes = -1L;
   private int ranges = -1;
   private boolean totalsCalculated = false;

   public SyncStatSummary(boolean isEstimate) {
      this.isEstimate = isEstimate;
   }

   public void consumeRepairResult(RepairResult result) {
      Pair<String, String> cf = Pair.create(result.desc.keyspace, result.desc.columnFamily);
      if(!this.summaries.containsKey(cf)) {
         this.summaries.put(cf, new SyncStatSummary.Table((String)cf.left, (String)cf.right));
      }

      ((SyncStatSummary.Table)this.summaries.get(cf)).consumeStats(result.stats);
   }

   public void consumeSessionResults(List<RepairSessionResult> results) {
      if(results != null) {
         Iterables.filter(results, Objects::nonNull).forEach((r) -> {
            Iterables.filter(r.repairJobResults, Objects::nonNull).forEach(this::consumeRepairResult);
         });
      }

   }

   public boolean isEmpty() {
      this.calculateTotals();
      return this.files == 0 && this.bytes == 0L && this.ranges == 0;
   }

   private void calculateTotals() {
      this.files = 0;
      this.bytes = 0L;
      this.ranges = 0;
      this.summaries.values().forEach(SyncStatSummary.Table::calculateTotals);

      SyncStatSummary.Table table;
      for(Iterator var1 = this.summaries.values().iterator(); var1.hasNext(); this.ranges += table.ranges) {
         table = (SyncStatSummary.Table)var1.next();
         table.calculateTotals();
         this.files += table.files;
         this.bytes += table.bytes;
      }

      this.totalsCalculated = true;
   }

   public String toString() {
      List<Pair<String, String>> tables = Lists.newArrayList(this.summaries.keySet());
      tables.sort((o1, o2) -> {
         int ks = ((String)o1.left).compareTo((String)o2.left);
         return ks != 0?ks:((String)o1.right).compareTo((String)o2.right);
      });
      this.calculateTotals();
      StringBuilder output = new StringBuilder();
      if(this.isEstimate) {
         output.append(String.format("Total estimated streaming: %s ranges, %s sstables, %s bytes\n", new Object[]{Integer.valueOf(this.ranges), Integer.valueOf(this.files), FBUtilities.prettyPrintMemory(this.bytes)}));
      } else {
         output.append(String.format("Total streaming: %s ranges, %s sstables, %s bytes\n", new Object[]{Integer.valueOf(this.ranges), Integer.valueOf(this.files), FBUtilities.prettyPrintMemory(this.bytes)}));
      }

      Iterator var3 = tables.iterator();

      while(var3.hasNext()) {
         Pair<String, String> tableName = (Pair)var3.next();
         SyncStatSummary.Table table = (SyncStatSummary.Table)this.summaries.get(tableName);
         output.append(table.toString()).append('\n');
      }

      return output.toString();
   }

   private static class Table {
      final String keyspace;
      final String table;
      int files = -1;
      long bytes = -1L;
      int ranges = -1;
      boolean totalsCalculated = false;
      final Map<Pair<InetAddress, InetAddress>, SyncStatSummary.Session> sessions = new HashMap();

      Table(String keyspace, String table) {
         this.keyspace = keyspace;
         this.table = table;
      }

      SyncStatSummary.Session getOrCreate(InetAddress from, InetAddress to) {
         Pair<InetAddress, InetAddress> k = Pair.create(from, to);
         if(!this.sessions.containsKey(k)) {
            this.sessions.put(k, new SyncStatSummary.Session(from, to));
         }

         return (SyncStatSummary.Session)this.sessions.get(k);
      }

      void consumeStat(SyncStat stat) {
         Iterator var2 = stat.summaries.iterator();

         while(var2.hasNext()) {
            SessionSummary summary = (SessionSummary)var2.next();
            this.getOrCreate(summary.coordinator, summary.peer).consumeSummaries(summary.sendingSummaries, stat.numberOfDifferences);
            this.getOrCreate(summary.peer, summary.coordinator).consumeSummaries(summary.receivingSummaries, stat.numberOfDifferences);
         }

      }

      void consumeStats(List<SyncStat> stats) {
         Iterables.filter(stats, (s) -> {
            return s.summaries != null;
         }).forEach(this::consumeStat);
      }

      void calculateTotals() {
         this.files = 0;
         this.bytes = 0L;
         this.ranges = 0;

         SyncStatSummary.Session session;
         for(Iterator var1 = this.sessions.values().iterator(); var1.hasNext(); this.ranges = (int)((long)this.ranges + session.ranges)) {
            session = (SyncStatSummary.Session)var1.next();
            this.files += session.files;
            this.bytes += session.bytes;
         }

         this.totalsCalculated = true;
      }

      public String toString() {
         if(!this.totalsCalculated) {
            this.calculateTotals();
         }

         StringBuilder output = new StringBuilder();
         output.append(String.format("%s.%s - %s ranges, %s sstables, %s bytes\n", new Object[]{this.keyspace, this.table, Integer.valueOf(this.ranges), Integer.valueOf(this.files), FBUtilities.prettyPrintMemory(this.bytes)}));
         Iterator var2 = this.sessions.values().iterator();

         while(var2.hasNext()) {
            SyncStatSummary.Session session = (SyncStatSummary.Session)var2.next();
            output.append("    ").append(session.toString()).append('\n');
         }

         return output.toString();
      }
   }

   private static class Session {
      final InetAddress src;
      final InetAddress dst;
      int files = 0;
      long bytes = 0L;
      long ranges = 0L;

      Session(InetAddress src, InetAddress dst) {
         this.src = src;
         this.dst = dst;
      }

      void consumeSummary(StreamSummary summary) {
         this.files += summary.files;
         this.bytes += summary.totalSize;
      }

      void consumeSummaries(Collection<StreamSummary> summaries, long numRanges) {
         summaries.forEach(this::consumeSummary);
         this.ranges += numRanges;
      }

      public String toString() {
         return String.format("%s -> %s: %s ranges, %s sstables, %s bytes", new Object[]{this.src, this.dst, Long.valueOf(this.ranges), Integer.valueOf(this.files), FBUtilities.prettyPrintMemory(this.bytes)});
      }
   }
}
