package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.Serializer;

class GossipDigestAck2Serializer implements Serializer<GossipDigestAck2> {
   GossipDigestAck2Serializer() {
   }


    public void serialize(GossipDigestAck2 ack2, DataOutputPlus out) throws IOException {
        out.writeInt(ack2.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet()) {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, out);
            EndpointState.serializer.serialize(entry.getValue(), out);
        }
    }

   public GossipDigestAck2 deserialize(DataInputPlus in) throws IOException {
      int size = in.readInt();
      Map<InetAddress, EndpointState> epStateMap = new HashMap(size);

      for(int i = 0; i < size; ++i) {
         InetAddress ep = CompactEndpointSerializationHelper.deserialize(in);
         EndpointState epState = (EndpointState)EndpointState.serializer.deserialize(in);
         epStateMap.put(ep, epState);
      }

      return new GossipDigestAck2(epStateMap);
   }

   public long serializedSize(GossipDigestAck2 ack2) {
      long size = TypeSizes.sizeof(ack2.epStateMap.size());
      for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet()) {
         size += (long)CompactEndpointSerializationHelper.serializedSize(entry.getKey()) + EndpointState.serializer.serializedSize(entry.getValue());
      }
      return size;
   }
}
