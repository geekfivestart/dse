package org.apache.cassandra.gms;

import java.util.List;
import org.apache.cassandra.utils.Serializer;

public class GossipDigestSyn {
   public static final Serializer<GossipDigestSyn> serializer = new GossipDigestSynSerializer();
   final String clusterId;
   final String partioner;
   final List<GossipDigest> gDigests;

   public GossipDigestSyn(String clusterId, String partioner, List<GossipDigest> gDigests) {
      this.clusterId = clusterId;
      this.partioner = partioner;
      this.gDigests = gDigests;
   }

   List<GossipDigest> getGossipDigests() {
      return this.gDigests;
   }

   public String toString() {
      return String.format("SYN(cluster=%s, partitioner=%s, digests=%s)", new Object[]{this.clusterId, this.partioner, this.gDigests});
   }
}
