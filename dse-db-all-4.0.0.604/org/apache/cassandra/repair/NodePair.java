package org.apache.cassandra.repair;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.Serializer;

public class NodePair {
   public static Serializer<NodePair> serializer = new NodePair.NodePairSerializer();
   public final InetAddress endpoint1;
   public final InetAddress endpoint2;

   public NodePair(InetAddress endpoint1, InetAddress endpoint2) {
      this.endpoint1 = endpoint1;
      this.endpoint2 = endpoint2;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         NodePair nodePair = (NodePair)o;
         return this.endpoint1.equals(nodePair.endpoint1) && this.endpoint2.equals(nodePair.endpoint2);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.endpoint1, this.endpoint2});
   }

   public static class NodePairSerializer implements Serializer<NodePair> {
      public NodePairSerializer() {
      }

      public void serialize(NodePair nodePair, DataOutputPlus out) throws IOException {
         CompactEndpointSerializationHelper.serialize(nodePair.endpoint1, out);
         CompactEndpointSerializationHelper.serialize(nodePair.endpoint2, out);
      }

      public NodePair deserialize(DataInputPlus in) throws IOException {
         InetAddress ep1 = CompactEndpointSerializationHelper.deserialize(in);
         InetAddress ep2 = CompactEndpointSerializationHelper.deserialize(in);
         return new NodePair(ep1, ep2);
      }

      public long serializedSize(NodePair nodePair) {
         return (long)(2 * CompactEndpointSerializationHelper.serializedSize(nodePair.endpoint1));
      }
   }
}
