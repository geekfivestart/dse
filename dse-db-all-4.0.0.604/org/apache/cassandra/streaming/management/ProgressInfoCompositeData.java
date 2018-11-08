package org.apache.cassandra.streaming.management;

import com.google.common.base.Throwables;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.apache.cassandra.streaming.ProgressInfo;

public class ProgressInfoCompositeData {
   private static final String[] ITEM_NAMES = new String[]{"planId", "peer", "sessionIndex", "fileName", "direction", "currentBytes", "totalBytes"};
   private static final String[] ITEM_DESCS = new String[]{"String representation of Plan ID", "Session peer", "Index of session", "Name of the file", "Direction('IN' or 'OUT')", "Current bytes transferred", "Total bytes to transfer"};
   private static final OpenType<?>[] ITEM_TYPES;
   public static final CompositeType COMPOSITE_TYPE;

   public ProgressInfoCompositeData() {
   }

   public static CompositeData toCompositeData(UUID planId, ProgressInfo progressInfo) {
      Map<String, Object> valueMap = new HashMap();
      valueMap.put(ITEM_NAMES[0], planId.toString());
      valueMap.put(ITEM_NAMES[1], progressInfo.peer.getHostAddress());
      valueMap.put(ITEM_NAMES[2], Integer.valueOf(progressInfo.sessionIndex));
      valueMap.put(ITEM_NAMES[3], progressInfo.fileName);
      valueMap.put(ITEM_NAMES[4], progressInfo.direction.name());
      valueMap.put(ITEM_NAMES[5], Long.valueOf(progressInfo.currentBytes));
      valueMap.put(ITEM_NAMES[6], Long.valueOf(progressInfo.totalBytes));

      try {
         return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
      } catch (OpenDataException var4) {
         throw Throwables.propagate(var4);
      }
   }

   public static ProgressInfo fromCompositeData(CompositeData cd) {
      Object[] values = cd.getAll(ITEM_NAMES);

      try {
         return new ProgressInfo(InetAddress.getByName((String)values[1]), ((Integer)values[2]).intValue(), (String)values[3], ProgressInfo.Direction.valueOf((String)values[4]), ((Long)values[5]).longValue(), ((Long)values[6]).longValue());
      } catch (UnknownHostException var3) {
         throw Throwables.propagate(var3);
      }
   }

   static {
      ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, SimpleType.INTEGER, SimpleType.STRING, SimpleType.STRING, SimpleType.LONG, SimpleType.LONG};

      try {
         COMPOSITE_TYPE = new CompositeType(ProgressInfo.class.getName(), "ProgressInfo", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
