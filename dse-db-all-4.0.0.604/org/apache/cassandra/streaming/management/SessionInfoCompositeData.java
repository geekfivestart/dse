package org.apache.cassandra.streaming.management;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;

public class SessionInfoCompositeData {
   private static final String[] ITEM_NAMES = new String[]{"planId", "peer", "connecting", "receivingSummaries", "sendingSummaries", "state", "receivingFiles", "sendingFiles", "sessionIndex"};
   private static final String[] ITEM_DESCS = new String[]{"Plan ID", "Session peer", "Connecting address", "Summaries of receiving data", "Summaries of sending data", "Current session state", "Receiving files", "Sending files", "Session index"};
   private static final OpenType<?>[] ITEM_TYPES;
   public static final CompositeType COMPOSITE_TYPE;

   public SessionInfoCompositeData() {
   }

   public static CompositeData toCompositeData(final UUID planId, SessionInfo sessionInfo) {
      Map<String, Object> valueMap = new HashMap();
      valueMap.put(ITEM_NAMES[0], planId.toString());
      valueMap.put(ITEM_NAMES[1], sessionInfo.peer.getHostAddress());
      valueMap.put(ITEM_NAMES[2], sessionInfo.connecting.getHostAddress());
      Function<StreamSummary, CompositeData> fromStreamSummary = new Function<StreamSummary, CompositeData>() {
         public CompositeData apply(StreamSummary input) {
            return StreamSummaryCompositeData.toCompositeData(input);
         }
      };
      valueMap.put(ITEM_NAMES[3], toArrayOfCompositeData(sessionInfo.receivingSummaries, fromStreamSummary));
      valueMap.put(ITEM_NAMES[4], toArrayOfCompositeData(sessionInfo.sendingSummaries, fromStreamSummary));
      valueMap.put(ITEM_NAMES[5], sessionInfo.state.name());
      Function<ProgressInfo, CompositeData> fromProgressInfo = new Function<ProgressInfo, CompositeData>() {
         public CompositeData apply(ProgressInfo input) {
            return ProgressInfoCompositeData.toCompositeData(planId, input);
         }
      };
      valueMap.put(ITEM_NAMES[6], toArrayOfCompositeData(sessionInfo.getReceivingFiles(), fromProgressInfo));
      valueMap.put(ITEM_NAMES[7], toArrayOfCompositeData(sessionInfo.getSendingFiles(), fromProgressInfo));
      valueMap.put(ITEM_NAMES[8], Integer.valueOf(sessionInfo.sessionIndex));

      try {
         return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
      } catch (OpenDataException var6) {
         throw Throwables.propagate(var6);
      }
   }

   public static SessionInfo fromCompositeData(CompositeData cd) {
      assert cd.getCompositeType().equals(COMPOSITE_TYPE);

      Object[] values = cd.getAll(ITEM_NAMES);

      InetAddress peer;
      InetAddress connecting;
      try {
         peer = InetAddress.getByName((String)values[1]);
         connecting = InetAddress.getByName((String)values[2]);
      } catch (UnknownHostException var9) {
         throw Throwables.propagate(var9);
      }

      Function<CompositeData, StreamSummary> toStreamSummary = new Function<CompositeData, StreamSummary>() {
         public StreamSummary apply(CompositeData input) {
            return StreamSummaryCompositeData.fromCompositeData(input);
         }
      };
      SessionInfo info = new SessionInfo(peer, ((Integer)values[8]).intValue(), connecting, fromArrayOfCompositeData((CompositeData[])((CompositeData[])values[3]), toStreamSummary), fromArrayOfCompositeData((CompositeData[])((CompositeData[])values[4]), toStreamSummary), StreamSession.State.valueOf((String)values[5]));
      Function<CompositeData, ProgressInfo> toProgressInfo = new Function<CompositeData, ProgressInfo>() {
         public ProgressInfo apply(CompositeData input) {
            return ProgressInfoCompositeData.fromCompositeData(input);
         }
      };
      Iterator var7 = fromArrayOfCompositeData((CompositeData[])((CompositeData[])values[6]), toProgressInfo).iterator();

      ProgressInfo progress;
      while(var7.hasNext()) {
         progress = (ProgressInfo)var7.next();
         info.updateProgress(progress);
      }

      var7 = fromArrayOfCompositeData((CompositeData[])((CompositeData[])values[7]), toProgressInfo).iterator();

      while(var7.hasNext()) {
         progress = (ProgressInfo)var7.next();
         info.updateProgress(progress);
      }

      return info;
   }

   private static <T> Collection<T> fromArrayOfCompositeData(CompositeData[] cds, Function<CompositeData, T> func) {
      return Lists.newArrayList(Iterables.transform(Arrays.asList(cds), func));
   }

   private static <T> CompositeData[] toArrayOfCompositeData(Collection<T> toConvert, Function<T, CompositeData> func) {
      CompositeData[] composites = new CompositeData[toConvert.size()];
      return (CompositeData[])Lists.newArrayList(Iterables.transform(toConvert, func)).toArray(composites);
   }

   static {
      try {
         ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, ArrayType.getArrayType(StreamSummaryCompositeData.COMPOSITE_TYPE), ArrayType.getArrayType(StreamSummaryCompositeData.COMPOSITE_TYPE), SimpleType.STRING, ArrayType.getArrayType(ProgressInfoCompositeData.COMPOSITE_TYPE), ArrayType.getArrayType(ProgressInfoCompositeData.COMPOSITE_TYPE), SimpleType.INTEGER};
         COMPOSITE_TYPE = new CompositeType(SessionInfo.class.getName(), "SessionInfo", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
