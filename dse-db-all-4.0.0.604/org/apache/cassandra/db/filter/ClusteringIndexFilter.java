package org.apache.cassandra.db.filter;

import java.io.IOException;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.versioning.Versioned;

public interface ClusteringIndexFilter {
   Versioned<ReadVerbs.ReadVersion, ClusteringIndexFilter.Serializer> serializers = AbstractClusteringIndexFilter.serializers;

   boolean isReversed();

   ClusteringIndexFilter forPaging(ClusteringComparator var1, Clustering var2, boolean var3);

   boolean isFullyCoveredBy(CachedPartition var1);

   boolean isHeadFilter();

   boolean selectsAllPartition();

   boolean selects(Clustering var1);

   FlowableUnfilteredPartition filterNotIndexed(ColumnFilter var1, FlowableUnfilteredPartition var2);

   Slices getSlices(TableMetadata var1);

   UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter var1, Partition var2);

   FlowableUnfilteredPartition getFlowableUnfilteredPartition(ColumnFilter var1, Partition var2);

   boolean shouldInclude(SSTableReader var1);

   ClusteringIndexFilter.Kind kind();

   String toString(TableMetadata var1);

   String toCQLString(TableMetadata var1);

   public interface Serializer {
      void serialize(ClusteringIndexFilter var1, DataOutputPlus var2) throws IOException;

      ClusteringIndexFilter deserialize(DataInputPlus var1, TableMetadata var2) throws IOException;

      long serializedSize(ClusteringIndexFilter var1);
   }

   public interface InternalDeserializer {
      ClusteringIndexFilter deserialize(DataInputPlus var1, ReadVerbs.ReadVersion var2, TableMetadata var3, boolean var4) throws IOException;
   }

   public static enum Kind {
      SLICE(ClusteringIndexSliceFilter.deserializer),
      NAMES(ClusteringIndexNamesFilter.deserializer);

      protected final ClusteringIndexFilter.InternalDeserializer deserializer;

      private Kind(ClusteringIndexFilter.InternalDeserializer deserializer) {
         this.deserializer = deserializer;
      }
   }
}
