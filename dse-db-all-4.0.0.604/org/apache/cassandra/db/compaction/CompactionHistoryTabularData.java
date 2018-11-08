package org.apache.cassandra.db.compaction;

import com.google.common.base.Throwables;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionHistoryTabularData {
   private static final String[] ITEM_NAMES = new String[]{"id", "keyspace_name", "columnfamily_name", "compacted_at", "bytes_in", "bytes_out", "rows_merged"};
   private static final String[] ITEM_DESCS = new String[]{"time uuid", "keyspace name", "column family name", "compaction finished at", "total bytes in", "total bytes out", "total rows merged"};
   private static final String TYPE_NAME = "CompactionHistory";
   private static final String ROW_DESC = "CompactionHistory";
   private static final OpenType<?>[] ITEM_TYPES;
   private static final CompositeType COMPOSITE_TYPE;
   private static final TabularType TABULAR_TYPE;

   public CompactionHistoryTabularData() {
   }

   public static TabularData from(UntypedResultSet resultSet) throws OpenDataException {
      TabularDataSupport result = new TabularDataSupport(TABULAR_TYPE);
      Iterator var2 = resultSet.iterator();

      while(var2.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
         UUID id = row.getUUID(ITEM_NAMES[0]);
         String ksName = row.getString(ITEM_NAMES[1]);
         String cfName = row.getString(ITEM_NAMES[2]);
         long compactedAt = row.getLong(ITEM_NAMES[3]);
         long bytesIn = row.getLong(ITEM_NAMES[4]);
         long bytesOut = row.getLong(ITEM_NAMES[5]);
         Map<Integer, Long> rowMerged = row.getMap(ITEM_NAMES[6], Int32Type.instance, LongType.instance);
         result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES, new Object[]{id.toString(), ksName, cfName, Long.valueOf(compactedAt), Long.valueOf(bytesIn), Long.valueOf(bytesOut), "{" + FBUtilities.toString(rowMerged) + "}"}));
      }

      return result;
   }

   static {
      try {
         ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING};
         COMPOSITE_TYPE = new CompositeType("CompactionHistory", "CompactionHistory", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
         TABULAR_TYPE = new TabularType("CompactionHistory", "CompactionHistory", COMPOSITE_TYPE, ITEM_NAMES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
