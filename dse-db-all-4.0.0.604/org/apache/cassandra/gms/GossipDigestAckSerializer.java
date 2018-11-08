package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.Serializer;

class GossipDigestAckSerializer implements Serializer<GossipDigestAck> {
   GossipDigestAckSerializer() {
   }

   public void serialize(GossipDigestAck gDigestAckMessage, DataOutputPlus out) throws IOException {
      GossipDigestSerializationHelper.serialize(gDigestAckMessage.gDigestList, out);
      out.writeInt(gDigestAckMessage.epStateMap.size());
      Iterator var3 = gDigestAckMessage.epStateMap.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var3.next();
         InetAddress ep = (InetAddress)entry.getKey();
         CompactEndpointSerializationHelper.serialize(ep, out);
         EndpointState.serializer.serialize(entry.getValue(), out);
      }

   }

   public GossipDigestAck deserialize(DataInputPlus in) throws IOException {
      List<GossipDigest> gDigestList = GossipDigestSerializationHelper.deserialize(in);
      int size = in.readInt();
      Map<InetAddress, EndpointState> epStateMap = new HashMap(size);

      for(int i = 0; i < size; ++i) {
         InetAddress ep = CompactEndpointSerializationHelper.deserialize(in);
         EndpointState epState = (EndpointState)EndpointState.serializer.deserialize(in);
         epStateMap.put(ep, epState);
      }

      return new GossipDigestAck(gDigestList, epStateMap);
   }

   public long serializedSize(GossipDigestAck ack) {
      long size = GossipDigestSerializationHelper.serializedSize(ack.gDigestList);
      size += (long)TypeSizes.sizeof(ack.epStateMap.size());

      Entry entry;
      for(Iterator var4 = ack.epStateMap.entrySet().iterator(); var4.hasNext(); size += (long)CompactEndpointSerializationHelper.serializedSize((InetAddress)entry.getKey()) + EndpointState.serializer.serializedSize(entry.getValue())) {
         entry = (Entry)var4.next();
      }

      return size;
   }
}
