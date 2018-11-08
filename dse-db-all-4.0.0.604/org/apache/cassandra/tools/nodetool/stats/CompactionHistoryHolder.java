package org.apache.cassandra.tools.nodetool.stats;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.TabularData;
import org.apache.cassandra.tools.NodeProbe;

public class CompactionHistoryHolder implements StatsHolder {
   public final NodeProbe probe;
   public List<String> indexNames;

   public CompactionHistoryHolder(NodeProbe probe) {
      this.probe = probe;
   }

   public Map<String, Object> convert2Map() {
      HashMap<String, Object> result = new HashMap();
      ArrayList<Map<String, Object>> compactions = new ArrayList();
      TabularData tabularData = this.probe.getCompactionHistory();
      this.indexNames = tabularData.getTabularType().getIndexNames();
      if(tabularData.isEmpty()) {
         return result;
      } else {
         List<CompactionHistoryHolder.CompactionHistoryRow> chrList = new ArrayList();
         Set<?> values = tabularData.keySet();
         Iterator var6 = values.iterator();

         while(var6.hasNext()) {
            Object eachValue = var6.next();
            List<?> value = (List)eachValue;
            CompactionHistoryHolder.CompactionHistoryRow chr = new CompactionHistoryHolder.CompactionHistoryRow((String)value.get(0), (String)value.get(1), (String)value.get(2), ((Long)value.get(3)).longValue(), ((Long)value.get(4)).longValue(), ((Long)value.get(5)).longValue(), (String)value.get(6));
            chrList.add(chr);
         }

         Collections.sort(chrList);
         var6 = chrList.iterator();

         while(var6.hasNext()) {
            CompactionHistoryHolder.CompactionHistoryRow chr = (CompactionHistoryHolder.CompactionHistoryRow)var6.next();
            compactions.add(chr.getAllAsMap());
         }

         result.put("CompactionHistory", compactions);
         return result;
      }
   }

   private static class CompactionHistoryRow implements Comparable<CompactionHistoryHolder.CompactionHistoryRow> {
      private final String id;
      private final String ksName;
      private final String cfName;
      private final long compactedAt;
      private final long bytesIn;
      private final long bytesOut;
      private final String rowMerged;

      CompactionHistoryRow(String id, String ksName, String cfName, long compactedAt, long bytesIn, long bytesOut, String rowMerged) {
         this.id = id;
         this.ksName = ksName;
         this.cfName = cfName;
         this.compactedAt = compactedAt;
         this.bytesIn = bytesIn;
         this.bytesOut = bytesOut;
         this.rowMerged = rowMerged;
      }

      public int compareTo(CompactionHistoryHolder.CompactionHistoryRow chr) {
         return Long.signum(chr.compactedAt - this.compactedAt);
      }

      private HashMap<String, Object> getAllAsMap() {
         HashMap<String, Object> compaction = new HashMap();
         compaction.put("id", this.id);
         compaction.put("keyspace_name", this.ksName);
         compaction.put("columnfamily_name", this.cfName);
         Instant instant = Instant.ofEpochMilli(this.compactedAt);
         LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
         compaction.put("compacted_at", ldt.toString());
         compaction.put("bytes_in", Long.valueOf(this.bytesIn));
         compaction.put("bytes_out", Long.valueOf(this.bytesOut));
         compaction.put("rows_merged", this.rowMerged);
         return compaction;
      }
   }
}
