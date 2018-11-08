package org.apache.cassandra.db.filter;

import io.reactivex.functions.Function;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;

public class ClusteringIndexNamesFilter extends AbstractClusteringIndexFilter {
   static final ClusteringIndexFilter.InternalDeserializer deserializer = new ClusteringIndexNamesFilter.NamesDeserializer(null);
   private final NavigableSet<Clustering> clusterings;
   private final NavigableSet<Clustering> clusteringsInQueryOrder;

   public ClusteringIndexNamesFilter(NavigableSet<Clustering> clusterings, boolean reversed) {
      super(reversed);

      assert !clusterings.contains(Clustering.STATIC_CLUSTERING);

      this.clusterings = clusterings;
      this.clusteringsInQueryOrder = reversed?clusterings.descendingSet():clusterings;
   }

   public NavigableSet<Clustering> requestedRows() {
      return this.clusterings;
   }

   public boolean selectsAllPartition() {
      return this.clusterings.isEmpty();
   }

   public boolean selects(Clustering clustering) {
      return this.clusterings.contains(clustering);
   }

   public ClusteringIndexNamesFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
      NavigableSet<Clustering> newClusterings = this.reversed?this.clusterings.headSet(lastReturned, inclusive):this.clusterings.tailSet(lastReturned, inclusive);
      return new ClusteringIndexNamesFilter(newClusterings, this.reversed);
   }

   public boolean isFullyCoveredBy(CachedPartition partition) {
      return partition.isEmpty()?false:this.clusterings.comparator().compare(this.clusterings.last(), partition.lastRow().clustering()) <= 0;
   }

   public boolean isHeadFilter() {
      return false;
   }

   private Row filterNotIndexedStaticRow(ColumnFilter columnFilter, TableMetadata metadata, Row row) {
      return columnFilter.fetchedColumns().statics.isEmpty()?Rows.EMPTY_STATIC_ROW:row.filter(columnFilter, metadata);
   }

   private Unfiltered filterNotIndexedRow(ColumnFilter columnFilter, TableMetadata metadata, Unfiltered unfiltered) {
      if(unfiltered instanceof Row) {
         Row row = (Row)unfiltered;
         return this.clusterings.contains(row.clustering())?row.filter(columnFilter, metadata):null;
      } else {
         return unfiltered;
      }
   }

   public FlowableUnfilteredPartition filterNotIndexed(ColumnFilter columnFilter, FlowableUnfilteredPartition partition) {
      return partition.skippingMapContent((row) -> {
         return this.filterNotIndexedRow(columnFilter, partition.metadata(), row);
      }, this.filterNotIndexedStaticRow(columnFilter, partition.metadata(), partition.staticRow()));
   }

   public Slices getSlices(TableMetadata metadata) {
      Slices.Builder builder = new Slices.Builder(metadata.comparator, this.clusteringsInQueryOrder.size());
      Iterator var3 = this.clusteringsInQueryOrder.iterator();

      while(var3.hasNext()) {
         Clustering clustering = (Clustering)var3.next();
         builder.add(Slice.make(clustering));
      }

      return builder.build();
   }

   public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition) {
      final Iterator<Clustering> clusteringIter = this.clusteringsInQueryOrder.iterator();
      final SearchIterator<Clustering, Row> searcher = partition.searchIterator(columnFilter, this.reversed);
      return new AbstractUnfilteredRowIterator(partition.metadata(), partition.partitionKey(), partition.partitionLevelDeletion(), columnFilter.fetchedColumns(), (Row)searcher.next(Clustering.STATIC_CLUSTERING), this.reversed, partition.stats()) {
         protected Unfiltered computeNext() {
            while(true) {
               if(clusteringIter.hasNext()) {
                  Row row = (Row)searcher.next(clusteringIter.next());
                  if(row == null) {
                     continue;
                  }

                  return row;
               }

               return (Unfiltered)this.endOfData();
            }
         }
      };
   }

   public FlowableUnfilteredPartition getFlowableUnfilteredPartition(ColumnFilter columnFilter, Partition partition) {
      final SearchIterator<Clustering, Row> searcher = partition.searchIterator(columnFilter, this.reversed);
      return new FlowableUnfilteredPartition.FlowSource(new PartitionHeader(partition.metadata(), partition.partitionKey(), partition.partitionLevelDeletion(), columnFilter.fetchedColumns(), this.reversed, partition.stats()), (Row)searcher.next(Clustering.STATIC_CLUSTERING)) {
         final Iterator<Clustering> clusteringIter;

         {
            this.clusteringIter = ClusteringIndexNamesFilter.this.clusteringsInQueryOrder.iterator();
         }

         public void requestNext() {
            while(true) {
               if(this.clusteringIter.hasNext()) {
                  Row row = (Row)searcher.next(this.clusteringIter.next());
                  if(row == null) {
                     continue;
                  }

                  this.subscriber.onNext(row);
                  return;
               }

               this.subscriber.onComplete();
               return;
            }
         }

         public void close() throws Exception {
         }
      };
   }

   public boolean shouldInclude(SSTableReader sstable) {
      ClusteringComparator comparator = sstable.metadata().comparator;
      List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
      List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;
      Iterator var5 = this.clusterings.iterator();

      Clustering clustering;
      do {
         if(!var5.hasNext()) {
            return false;
         }

         clustering = (Clustering)var5.next();
      } while(!Slice.make(clustering).intersects(comparator, minClusteringValues, maxClusteringValues));

      return true;
   }

   public String toString(TableMetadata metadata) {
      StringBuilder sb = new StringBuilder();
      sb.append("names(");
      int i = 0;
      Iterator var4 = this.clusterings.iterator();

      while(var4.hasNext()) {
         Clustering clustering = (Clustering)var4.next();
         sb.append(i++ == 0?"":", ").append(clustering.toString(metadata));
      }

      if(this.reversed) {
         sb.append(", reversed");
      }

      return sb.append(')').toString();
   }

   public String toCQLString(TableMetadata metadata) {
      if(!metadata.clusteringColumns().isEmpty() && this.clusterings.size() > 1) {
         StringBuilder sb = new StringBuilder();
         sb.append('(').append(ColumnMetadata.toCQLString((Iterable)metadata.clusteringColumns())).append(')');
         sb.append(this.clusterings.size() == 1?" = ":" IN (");
         int i = 0;
         Iterator var4 = this.clusterings.iterator();

         while(var4.hasNext()) {
            Clustering clustering = (Clustering)var4.next();
            sb.append(i++ == 0?"":", ").append("(").append(clustering.toCQLString(metadata)).append(")");
         }

         sb.append(this.clusterings.size() == 1?"":")");
         this.appendOrderByToCQLString(metadata, sb);
         return sb.toString();
      } else {
         return "";
      }
   }

   public ClusteringIndexFilter.Kind kind() {
      return ClusteringIndexFilter.Kind.NAMES;
   }

   public boolean equals(Object other) {
      if(!(other instanceof ClusteringIndexNamesFilter)) {
         return false;
      } else {
         ClusteringIndexNamesFilter that = (ClusteringIndexNamesFilter)other;
         return this.clusterings.equals(that.clusterings);
      }
   }

   protected void serializeInternal(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      ClusteringComparator comparator = (ClusteringComparator)this.clusterings.comparator();
      out.writeUnsignedVInt((long)this.clusterings.size());
      Iterator var4 = this.clusterings.iterator();

      while(var4.hasNext()) {
         Clustering clustering = (Clustering)var4.next();
         Clustering.serializer.serialize(clustering, out, version.encodingVersion.clusteringVersion, comparator.subtypes());
      }

   }

   protected long serializedSizeInternal(ReadVerbs.ReadVersion version) {
      ClusteringComparator comparator = (ClusteringComparator)this.clusterings.comparator();
      long size = (long)TypeSizes.sizeofUnsignedVInt((long)this.clusterings.size());

      Clustering clustering;
      for(Iterator var5 = this.clusterings.iterator(); var5.hasNext(); size += Clustering.serializer.serializedSize(clustering, version.encodingVersion.clusteringVersion, comparator.subtypes())) {
         clustering = (Clustering)var5.next();
      }

      return size;
   }

   private static class NamesDeserializer implements ClusteringIndexFilter.InternalDeserializer {
      private NamesDeserializer() {
      }

      public ClusteringIndexFilter deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata, boolean reversed) throws IOException {
         ClusteringComparator comparator = metadata.comparator;
         BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(comparator);
         int size = (int)in.readUnsignedVInt();

         for(int i = 0; i < size; ++i) {
            clusterings.add(Clustering.serializer.deserialize(in, version.encodingVersion.clusteringVersion, comparator.subtypes()));
         }

         return new ClusteringIndexNamesFilter(clusterings.build(), reversed);
      }
   }
}
