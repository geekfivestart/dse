package org.apache.cassandra.streaming.management;

import com.google.common.base.Throwables;
import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSummary;

public class StreamSummaryCompositeData {
   private static final String[] ITEM_NAMES = new String[]{"tableId", "files", "totalSize"};
   private static final String[] ITEM_DESCS = new String[]{"ColumnFamilu ID", "Number of files", "Total bytes of the files"};
   private static final OpenType<?>[] ITEM_TYPES;
   public static final CompositeType COMPOSITE_TYPE;

   public StreamSummaryCompositeData() {
   }

   public static CompositeData toCompositeData(StreamSummary streamSummary) {
      Map<String, Object> valueMap = new HashMap();
      valueMap.put(ITEM_NAMES[0], streamSummary.tableId.toString());
      valueMap.put(ITEM_NAMES[1], Integer.valueOf(streamSummary.files));
      valueMap.put(ITEM_NAMES[2], Long.valueOf(streamSummary.totalSize));

      try {
         return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
      } catch (OpenDataException var3) {
         throw Throwables.propagate(var3);
      }
   }

   public static StreamSummary fromCompositeData(CompositeData cd) {
      Object[] values = cd.getAll(ITEM_NAMES);
      return new StreamSummary(TableId.fromString((String)values[0]), ((Integer)values[1]).intValue(), ((Long)values[2]).longValue());
   }

   static {
      ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.INTEGER, SimpleType.LONG};

      try {
         COMPOSITE_TYPE = new CompositeType(StreamSummary.class.getName(), "StreamSummary", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
