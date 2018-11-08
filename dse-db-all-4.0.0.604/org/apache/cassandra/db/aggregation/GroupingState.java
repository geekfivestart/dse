package org.apache.cassandra.db.aggregation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Function;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public final class GroupingState {
   public static final Versioned<ReadVerbs.ReadVersion, GroupingState.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new GroupingState.Serializer(x$0);
   });
   public static final GroupingState EMPTY_STATE = new GroupingState((ByteBuffer)null, (Clustering)null);
   private final ByteBuffer partitionKey;
   final Clustering clustering;

   public GroupingState(ByteBuffer partitionKey, Clustering clustering) {
      this.partitionKey = partitionKey;
      this.clustering = clustering;
   }

   public ByteBuffer partitionKey() {
      return this.partitionKey;
   }

   public Clustering clustering() {
      return this.clustering;
   }

   public boolean hasClustering() {
      return this.clustering != null;
   }

   public boolean equals(Object other) {
      if(!(other instanceof GroupingState)) {
         return false;
      } else {
         GroupingState that = (GroupingState)other;
         return Objects.equals(this.partitionKey, that.partitionKey) && Objects.equals(this.clustering, that.clustering);
      }
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(GroupingState state, DataOutputPlus out, ClusteringComparator comparator) throws IOException {
         boolean hasPartitionKey = state.partitionKey != null;
         out.writeBoolean(hasPartitionKey);
         if(hasPartitionKey) {
            ByteBufferUtil.writeWithVIntLength(state.partitionKey, out);
            boolean hasClustering = state.hasClustering();
            out.writeBoolean(hasClustering);
            if(hasClustering) {
               Clustering.serializer.serialize(state.clustering, out, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, comparator.subtypes());
            }
         }

      }

      public GroupingState deserialize(DataInputPlus in, ClusteringComparator comparator) throws IOException {
         if(!in.readBoolean()) {
            return GroupingState.EMPTY_STATE;
         } else {
            ByteBuffer partitionKey = ByteBufferUtil.readWithVIntLength(in);
            Clustering clustering = null;
            if(in.readBoolean()) {
               clustering = Clustering.serializer.deserialize(in, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, comparator.subtypes());
            }

            return new GroupingState(partitionKey, clustering);
         }
      }

      public long serializedSize(GroupingState state, ClusteringComparator comparator) {
         boolean hasPartitionKey = state.partitionKey != null;
         long size = (long)TypeSizes.sizeof(hasPartitionKey);
         if(hasPartitionKey) {
            size += (long)ByteBufferUtil.serializedSizeWithVIntLength(state.partitionKey);
            boolean hasClustering = state.hasClustering();
            size += (long)TypeSizes.sizeof(hasClustering);
            if(hasClustering) {
               size += Clustering.serializer.serializedSize(state.clustering, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, comparator.subtypes());
            }
         }

         return size;
      }
   }
}
