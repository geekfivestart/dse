package org.apache.cassandra.db.filter;

import io.reactivex.functions.Function;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

public class ClusteringIndexSliceFilter extends AbstractClusteringIndexFilter {
   static final ClusteringIndexFilter.InternalDeserializer deserializer = new ClusteringIndexSliceFilter.SliceDeserializer();
   private final Slices slices;

   public ClusteringIndexSliceFilter(Slices slices, boolean reversed) {
      super(reversed);
      this.slices = slices;
   }

   public Slices requestedSlices() {
      return this.slices;
   }

   public boolean selectsAllPartition() {
      return this.slices.size() == 1 && !this.slices.hasLowerBound() && !this.slices.hasUpperBound();
   }

   public boolean selects(Clustering clustering) {
      return this.slices.selects(clustering);
   }

   public ClusteringIndexSliceFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
      Slices newSlices = this.slices.forPaging(comparator, lastReturned, inclusive, this.reversed);
      return this.slices == newSlices?this:new ClusteringIndexSliceFilter(newSlices, this.reversed);
   }

   public boolean isFullyCoveredBy(CachedPartition partition) {
      return this.slices.hasUpperBound() && !partition.isEmpty()?partition.metadata().comparator.compare((ClusteringPrefix)this.slices.get(this.slices.size() - 1).end(), (ClusteringPrefix)partition.lastRow().clustering()) <= 0:false;
   }

   public boolean isHeadFilter() {
      return !this.reversed && this.slices.size() == 1 && !this.slices.hasLowerBound();
   }

   private Row filterNotIndexedStaticRow(ColumnFilter columnFilter, TableMetadata metadata, Row row) {
      return columnFilter.fetchedColumns().statics.isEmpty()?Rows.EMPTY_STATIC_ROW:row.filter(columnFilter, metadata);
   }

   private Unfiltered filterNotIndexedRow(ColumnFilter columnFilter, TableMetadata metadata, Slices.InOrderTester tester, Unfiltered unfiltered) {
      if(unfiltered instanceof Row) {
         Row row = (Row)unfiltered;
         return tester.includes(row.clustering())?row.filter(columnFilter, metadata):null;
      } else {
         return unfiltered;
      }
   }

   public FlowableUnfilteredPartition filterNotIndexed(ColumnFilter columnFilter, FlowableUnfilteredPartition partition) {
      Slices.InOrderTester tester = this.slices.inOrderTester(this.reversed);
      return partition.skippingMapContent((row) -> {
         return this.filterNotIndexedRow(columnFilter, partition.metadata(), tester, row);
      }, this.filterNotIndexedStaticRow(columnFilter, partition.metadata(), partition.staticRow()));
   }

   public Slices getSlices(TableMetadata metadata) {
      return this.slices;
   }

   public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition) {
      return partition.unfilteredIterator(columnFilter, this.slices, this.reversed);
   }

   public FlowableUnfilteredPartition getFlowableUnfilteredPartition(ColumnFilter columnFilter, Partition partition) {
      return partition.unfilteredPartition(columnFilter, this.slices, this.reversed);
   }

   public boolean shouldInclude(SSTableReader sstable) {
      List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
      List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;
      return !minClusteringValues.isEmpty() && !maxClusteringValues.isEmpty()?this.slices.intersects(minClusteringValues, maxClusteringValues):true;
   }

   public String toString(TableMetadata metadata) {
      return String.format("slice(slices=%s, reversed=%b)", new Object[]{this.slices, Boolean.valueOf(this.reversed)});
   }

   public String toCQLString(TableMetadata metadata) {
      StringBuilder sb = new StringBuilder();
      if(!this.selectsAllPartition()) {
         sb.append(this.slices.toCQLString(metadata));
      }

      this.appendOrderByToCQLString(metadata, sb);
      return sb.toString();
   }

   public ClusteringIndexFilter.Kind kind() {
      return ClusteringIndexFilter.Kind.SLICE;
   }

   public boolean equals(Object other) {
      if(!(other instanceof ClusteringIndexSliceFilter)) {
         return false;
      } else {
         ClusteringIndexSliceFilter that = (ClusteringIndexSliceFilter)other;
         return this.slices.equals(that.slices);
      }
   }

   protected void serializeInternal(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      Slices.serializer.serialize(this.slices, out, version.encodingVersion.clusteringVersion);
   }

   protected long serializedSizeInternal(ReadVerbs.ReadVersion version) {
      return Slices.serializer.serializedSize(this.slices, version.encodingVersion.clusteringVersion);
   }

   private static class SliceDeserializer implements ClusteringIndexFilter.InternalDeserializer {
      private SliceDeserializer() {
      }

      public ClusteringIndexFilter deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata, boolean reversed) throws IOException {
         Slices slices = Slices.serializer.deserialize(in, version.encodingVersion.clusteringVersion, metadata);
         return new ClusteringIndexSliceFilter(slices, reversed);
      }
   }
}
