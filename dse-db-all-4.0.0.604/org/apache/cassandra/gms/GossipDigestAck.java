package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.utils.Serializer;

public class GossipDigestAck {
   public static final Serializer<GossipDigestAck> serializer = new GossipDigestAckSerializer();
   final List<GossipDigest> gDigestList;
   final Map<InetAddress, EndpointState> epStateMap;

   GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddress, EndpointState> epStateMap) {
      this.gDigestList = gDigestList;
      this.epStateMap = epStateMap;
   }

   List<GossipDigest> getGossipDigestList() {
      return this.gDigestList;
   }

   Map<InetAddress, EndpointState> getEndpointStateMap() {
      return this.epStateMap;
   }

   public String toString() {
      return String.format("ACK(digests=%s, states=%s)", new Object[]{this.gDigestList, this.epStateMap});
   }
}
