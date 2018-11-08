package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.Map;
import org.apache.cassandra.utils.Serializer;

public class GossipDigestAck2 {
   public static final Serializer<GossipDigestAck2> serializer = new GossipDigestAck2Serializer();
   final Map<InetAddress, EndpointState> epStateMap;

   GossipDigestAck2(Map<InetAddress, EndpointState> epStateMap) {
      this.epStateMap = epStateMap;
   }

   Map<InetAddress, EndpointState> getEndpointStateMap() {
      return this.epStateMap;
   }

   public String toString() {
      return String.format("ACK2(states=%s)", new Object[]{this.epStateMap});
   }
}
