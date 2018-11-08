package org.apache.cassandra.gms;

import java.net.InetAddress;
import org.apache.cassandra.utils.Serializer;

public class GossipDigest implements Comparable<GossipDigest> {
   public static final Serializer<GossipDigest> serializer = new GossipDigestSerializer();
   final InetAddress endpoint;
   final int generation;
   final int maxVersion;

   GossipDigest(InetAddress ep, int gen, int version) {
      this.endpoint = ep;
      this.generation = gen;
      this.maxVersion = version;
   }

   InetAddress getEndpoint() {
      return this.endpoint;
   }

   int getGeneration() {
      return this.generation;
   }

   int getMaxVersion() {
      return this.maxVersion;
   }

   public int compareTo(GossipDigest gDigest) {
      return this.generation != gDigest.generation?this.generation - gDigest.generation:this.maxVersion - gDigest.maxVersion;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.endpoint);
      sb.append(":");
      sb.append(this.generation);
      sb.append(":");
      sb.append(this.maxVersion);
      return sb.toString();
   }
}
