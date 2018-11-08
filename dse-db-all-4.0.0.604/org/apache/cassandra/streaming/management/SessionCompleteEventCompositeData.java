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
import org.apache.cassandra.streaming.StreamEvent;

public class SessionCompleteEventCompositeData {
   private static final String[] ITEM_NAMES = new String[]{"planId", "peer", "success"};
   private static final String[] ITEM_DESCS = new String[]{"Plan ID", "Session peer", "Indicates whether session was successful"};
   private static final OpenType<?>[] ITEM_TYPES;
   public static final CompositeType COMPOSITE_TYPE;

   public SessionCompleteEventCompositeData() {
   }

   public static CompositeData toCompositeData(StreamEvent.SessionCompleteEvent event) {
      Map<String, Object> valueMap = new HashMap();
      valueMap.put(ITEM_NAMES[0], event.planId.toString());
      valueMap.put(ITEM_NAMES[1], event.peer.getHostAddress());
      valueMap.put(ITEM_NAMES[2], Boolean.valueOf(event.success));

      try {
         return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
      } catch (OpenDataException var3) {
         throw Throwables.propagate(var3);
      }
   }

   static {
      ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, SimpleType.BOOLEAN};

      try {
         COMPOSITE_TYPE = new CompositeType(StreamEvent.SessionCompleteEvent.class.getName(), "SessionCompleteEvent", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
