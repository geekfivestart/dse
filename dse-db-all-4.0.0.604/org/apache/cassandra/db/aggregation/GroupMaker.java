package org.apache.cassandra.db.aggregation;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class GroupMaker {
   public static final GroupMaker GROUP_EVERYTHING = new GroupMaker() {
      public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering) {
         return false;
      }

      public boolean returnAtLeastOneRow() {
         return true;
      }
   };

   public GroupMaker() {
   }

   public static GroupMaker newPkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, GroupingState state) {
      return new GroupMaker.PkPrefixGroupMaker(comparator, clusteringPrefixSize, state);
   }

   public static GroupMaker newPkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize) {
      return new GroupMaker.PkPrefixGroupMaker(comparator, clusteringPrefixSize);
   }

   public static GroupMaker newSelectorGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, Selector selector, GroupingState state) {
      return new GroupMaker.SelectorGroupMaker(comparator, clusteringPrefixSize, selector, state);
   }

   public static GroupMaker newSelectorGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, Selector selector) {
      return new GroupMaker.SelectorGroupMaker(comparator, clusteringPrefixSize, selector);
   }

   public abstract boolean isNewGroup(DecoratedKey var1, Clustering var2);

   public boolean returnAtLeastOneRow() {
      return false;
   }

   private static class SelectorGroupMaker extends GroupMaker.PkPrefixGroupMaker {
      private final Selector selector;
      private ByteBuffer lastOutput;
      private Selector.InputRow input = new Selector.InputRow(1, false, false);

      public SelectorGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, Selector selector, GroupingState state) {
         super(comparator, clusteringPrefixSize, state);
         this.selector = selector;
         this.lastOutput = this.lastClustering == null?null:this.executeSelector(this.lastClustering.get(clusteringPrefixSize - 1));
      }

      public SelectorGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, Selector selector) {
         super(comparator, clusteringPrefixSize);
         this.selector = selector;
      }

      public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering) {
         ByteBuffer output = Clustering.STATIC_CLUSTERING == clustering?null:this.executeSelector(clustering.get(this.clusteringPrefixSize - 1));
         boolean isNew = !partitionKey.getKey().equals(this.lastPartitionKey) || this.lastClustering == null || this.comparator.compare(this.lastClustering, clustering, this.clusteringPrefixSize - 1) != 0 || this.compareOutput(output) != 0;
         this.lastPartitionKey = partitionKey.getKey();
         this.lastClustering = Clustering.STATIC_CLUSTERING == clustering?null:clustering;
         this.lastOutput = output;
         return isNew;
      }

      private int compareOutput(ByteBuffer output) {
         return output == null?(this.lastOutput == null?0:-1):(this.lastOutput == null?1:this.selector.getType().compare(output, this.lastOutput));
      }

      private ByteBuffer executeSelector(ByteBuffer argument) {
         this.input.add(argument);
         this.selector.addInput(ProtocolVersion.CURRENT, this.input);
         ByteBuffer output = this.selector.getOutput(ProtocolVersion.CURRENT);
         this.selector.reset();
         this.input.reset(false);
         return output;
      }
   }

   private static class PkPrefixGroupMaker extends GroupMaker {
      protected final int clusteringPrefixSize;
      protected final ClusteringComparator comparator;
      protected ByteBuffer lastPartitionKey;
      protected Clustering lastClustering;

      public PkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, GroupingState state) {
         this(comparator, clusteringPrefixSize);
         this.lastPartitionKey = state.partitionKey();
         this.lastClustering = state.clustering;
      }

      public PkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize) {
         this.comparator = comparator;
         this.clusteringPrefixSize = clusteringPrefixSize;
      }

      public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering) {
         boolean isNew = !partitionKey.getKey().equals(this.lastPartitionKey) || this.lastClustering == null || this.comparator.compare(this.lastClustering, clustering, this.clusteringPrefixSize) != 0;
         this.lastPartitionKey = partitionKey.getKey();
         this.lastClustering = Clustering.STATIC_CLUSTERING == clustering?null:clustering;
         return isNew;
      }
   }
}
