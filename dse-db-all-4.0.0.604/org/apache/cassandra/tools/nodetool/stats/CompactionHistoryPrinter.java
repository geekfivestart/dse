package org.apache.cassandra.tools.nodetool.stats;

import com.google.common.collect.Iterables;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

public class CompactionHistoryPrinter {
   public CompactionHistoryPrinter() {
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
         return new CompactionHistoryPrinter.DefaultPrinter();
      }
   }

   public static class DefaultPrinter implements StatsPrinter<CompactionHistoryHolder> {
      public DefaultPrinter() {
      }

      public void print(CompactionHistoryHolder data, PrintStream out) {
         out.println("Compaction History: ");
         Map<String, Object> convertData = data.convert2Map();
         List<Object> compactionHistories = convertData.get("CompactionHistory") instanceof List?(List)convertData.get("CompactionHistory"):Collections.emptyList();
         List<String> indexNames = data.indexNames;
         if(compactionHistories.size() == 0) {
            out.printf("There is no compaction history", new Object[0]);
         } else {
            TableBuilder table = new TableBuilder();
            table.add((String[])Iterables.toArray(indexNames, String.class));
            Iterator var7 = compactionHistories.iterator();

            while(var7.hasNext()) {
               Object chr = var7.next();
               Map value = chr instanceof Map?(Map)chr:Collections.emptyMap();
               String[] obj = new String[]{(String)value.get("id"), (String)value.get("keyspace_name"), (String)value.get("columnfamily_name"), (String)value.get("compacted_at"), value.get("bytes_in").toString(), value.get("bytes_out").toString(), (String)value.get("rows_merged")};
               table.add(obj);
            }

            table.printTo(out);
         }
      }
   }
}
