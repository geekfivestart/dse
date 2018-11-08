package org.apache.cassandra.gms;

import java.io.IOException;
import java.util.List;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

class GossipDigestSynSerializer implements Serializer<GossipDigestSyn> {
   GossipDigestSynSerializer() {
   }

   public void serialize(GossipDigestSyn gDigestSynMessage, DataOutputPlus out) throws IOException {
      out.writeUTF(gDigestSynMessage.clusterId);
      out.writeUTF(gDigestSynMessage.partioner);
      GossipDigestSerializationHelper.serialize(gDigestSynMessage.gDigests, out);
   }

   public GossipDigestSyn deserialize(DataInputPlus in) throws IOException {
      String clusterId = in.readUTF();
      String partioner = null;
      partioner = in.readUTF();
      List<GossipDigest> gDigests = GossipDigestSerializationHelper.deserialize(in);
      return new GossipDigestSyn(clusterId, partioner, gDigests);
   }

   public long serializedSize(GossipDigestSyn syn) {
      long size = (long)TypeSizes.sizeof(syn.clusterId);
      size += (long)TypeSizes.sizeof(syn.partioner);
      size += GossipDigestSerializationHelper.serializedSize(syn.gDigests);
      return size;
   }
}
