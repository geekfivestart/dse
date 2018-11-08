package org.apache.cassandra.gms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

class GossipDigestSerializationHelper {
   GossipDigestSerializationHelper() {
   }

   static void serialize(List<GossipDigest> gDigestList, DataOutputPlus out) throws IOException {
      out.writeInt(gDigestList.size());
      Iterator var2 = gDigestList.iterator();

      while(var2.hasNext()) {
         GossipDigest gDigest = (GossipDigest)var2.next();
         GossipDigest.serializer.serialize(gDigest, out);
      }

   }

   static List<GossipDigest> deserialize(DataInputPlus in) throws IOException {
      int size = in.readInt();
      List<GossipDigest> gDigests = new ArrayList(size);

      for(int i = 0; i < size; ++i) {
         gDigests.add(GossipDigest.serializer.deserialize(in));
      }

      return gDigests;
   }

   static long serializedSize(List<GossipDigest> digests) {
      long size = (long)TypeSizes.sizeof(digests.size());

      GossipDigest digest;
      for(Iterator var3 = digests.iterator(); var3.hasNext(); size += GossipDigest.serializer.serializedSize(digest)) {
         digest = (GossipDigest)var3.next();
      }

      return size;
   }
}
