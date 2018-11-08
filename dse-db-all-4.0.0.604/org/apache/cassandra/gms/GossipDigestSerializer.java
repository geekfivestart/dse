package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.Serializer;

class GossipDigestSerializer implements Serializer<GossipDigest> {
   GossipDigestSerializer() {
   }

   public void serialize(GossipDigest gDigest, DataOutputPlus out) throws IOException {
      CompactEndpointSerializationHelper.serialize(gDigest.endpoint, out);
      out.writeInt(gDigest.generation);
      out.writeInt(gDigest.maxVersion);
   }

   public GossipDigest deserialize(DataInputPlus in) throws IOException {
      InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(in);
      int generation = in.readInt();
      int maxVersion = in.readInt();
      return new GossipDigest(endpoint, generation, maxVersion);
   }

   public long serializedSize(GossipDigest gDigest) {
      long size = (long)CompactEndpointSerializationHelper.serializedSize(gDigest.endpoint);
      size += (long)TypeSizes.sizeof(gDigest.generation);
      size += (long)TypeSizes.sizeof(gDigest.maxVersion);
      return size;
   }
}
