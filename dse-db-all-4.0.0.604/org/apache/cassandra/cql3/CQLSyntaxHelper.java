package org.apache.cassandra.cql3;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class CQLSyntaxHelper {
   public CQLSyntaxHelper() {
   }

   public static String toCQLMap(Map<String, String> map) {
      if(map.isEmpty()) {
         return "{}";
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append('{');
         Iterator<Entry<String, String>> iterator = map.entrySet().iterator();
         appendMapEntry(sb, (Entry)iterator.next());

         while(iterator.hasNext()) {
            appendMapEntry(sb.append(", "), (Entry)iterator.next());
         }

         sb.append('}');
         return sb.toString();
      }
   }

   private static void appendMapEntry(StringBuilder sb, Entry<String, String> entry) {
      sb.append(toCQLString((String)entry.getKey())).append(": ").append(toCQLString((String)entry.getValue()));
   }

   public static String toCQLString(String str) {
      return '\'' + str.replaceAll("'", "''") + '\'';
   }
}
