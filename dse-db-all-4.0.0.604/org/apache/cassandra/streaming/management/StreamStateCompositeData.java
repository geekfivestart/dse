package org.apache.cassandra.streaming.management;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamState;

public class StreamStateCompositeData {
   private static final String[] ITEM_NAMES = new String[]{"planId", "description", "sessions", "currentRxBytes", "totalRxBytes", "rxPercentage", "currentTxBytes", "totalTxBytes", "txPercentage"};
   private static final String[] ITEM_DESCS = new String[]{"Plan ID of this stream", "Stream plan description", "Active stream sessions", "Number of bytes received across all streams", "Total bytes available to receive across all streams", "Percentage received across all streams", "Number of bytes sent across all streams", "Total bytes available to send across all streams", "Percentage sent across all streams"};
   private static final OpenType<?>[] ITEM_TYPES;
   public static final CompositeType COMPOSITE_TYPE;

   public StreamStateCompositeData() {
   }

   public static CompositeData toCompositeData(final StreamState streamState) {
      Map<String, Object> valueMap = new HashMap();
      valueMap.put(ITEM_NAMES[0], streamState.planId.toString());
      valueMap.put(ITEM_NAMES[1], streamState.streamOperation.getDescription());
      CompositeData[] sessions = new CompositeData[streamState.sessions.size()];
      Lists.newArrayList(Iterables.transform(streamState.sessions, new Function<SessionInfo, CompositeData>() {
         public CompositeData apply(SessionInfo input) {
            return SessionInfoCompositeData.toCompositeData(streamState.planId, input);
         }
      })).toArray(sessions);
      valueMap.put(ITEM_NAMES[2], sessions);
      long currentRxBytes = 0L;
      long totalRxBytes = 0L;
      long currentTxBytes = 0L;
      long totalTxBytes = 0L;

      SessionInfo sessInfo;
      for(Iterator var11 = streamState.sessions.iterator(); var11.hasNext(); totalTxBytes += sessInfo.getTotalSizeToSend()) {
         sessInfo = (SessionInfo)var11.next();
         currentRxBytes += sessInfo.getTotalSizeReceived();
         totalRxBytes += sessInfo.getTotalSizeToReceive();
         currentTxBytes += sessInfo.getTotalSizeSent();
      }

      double rxPercentage = (double)(totalRxBytes == 0L?100L:currentRxBytes * 100L / totalRxBytes);
      double txPercentage = (double)(totalTxBytes == 0L?100L:currentTxBytes * 100L / totalTxBytes);
      valueMap.put(ITEM_NAMES[3], Long.valueOf(currentRxBytes));
      valueMap.put(ITEM_NAMES[4], Long.valueOf(totalRxBytes));
      valueMap.put(ITEM_NAMES[5], Double.valueOf(rxPercentage));
      valueMap.put(ITEM_NAMES[6], Long.valueOf(currentTxBytes));
      valueMap.put(ITEM_NAMES[7], Long.valueOf(totalTxBytes));
      valueMap.put(ITEM_NAMES[8], Double.valueOf(txPercentage));

      try {
         return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
      } catch (OpenDataException var16) {
         throw Throwables.propagate(var16);
      }
   }

   public static StreamState fromCompositeData(CompositeData cd) {
      assert cd.getCompositeType().equals(COMPOSITE_TYPE);

      Object[] values = cd.getAll(ITEM_NAMES);
      UUID planId = UUID.fromString((String)values[0]);
      String typeString = (String)values[1];
      Set<SessionInfo> sessions = Sets.newHashSet(Iterables.transform(Arrays.asList((CompositeData[])((CompositeData[])values[2])), new Function<CompositeData, SessionInfo>() {
         public SessionInfo apply(CompositeData input) {
            return SessionInfoCompositeData.fromCompositeData(input);
         }
      }));
      return new StreamState(planId, StreamOperation.fromString(typeString), sessions);
   }

   static {
      try {
         ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, ArrayType.getArrayType(SessionInfoCompositeData.COMPOSITE_TYPE), SimpleType.LONG, SimpleType.LONG, SimpleType.DOUBLE, SimpleType.LONG, SimpleType.LONG, SimpleType.DOUBLE};
         COMPOSITE_TYPE = new CompositeType(StreamState.class.getName(), "StreamState", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
