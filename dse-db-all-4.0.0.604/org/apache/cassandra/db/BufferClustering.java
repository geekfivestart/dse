package org.apache.cassandra.db;

import java.nio.ByteBuffer;

public class BufferClustering extends AbstractBufferClusteringPrefix implements Clustering {
   BufferClustering(ByteBuffer... values) {
      super(ClusteringPrefix.Kind.CLUSTERING, values);
   }
}
