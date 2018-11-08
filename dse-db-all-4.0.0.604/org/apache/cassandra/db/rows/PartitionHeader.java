package org.apache.cassandra.db.rows;

import java.util.List;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;

public class PartitionHeader {
   public final TableMetadata metadata;
   public final boolean isReverseOrder;
   public final RegularAndStaticColumns columns;
   public final DecoratedKey partitionKey;
   public DeletionTime partitionLevelDeletion;
   public EncodingStats stats;

   public PartitionHeader(TableMetadata metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, RegularAndStaticColumns columns, boolean isReverseOrder, EncodingStats stats) {
      this.metadata = metadata;
      this.partitionKey = partitionKey;
      this.partitionLevelDeletion = partitionLevelDeletion;
      this.columns = columns;
      this.isReverseOrder = isReverseOrder;
      this.stats = stats;
   }

   public static PartitionHeader empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed) {
      return new PartitionHeader(metadata, partitionKey, DeletionTime.LIVE, RegularAndStaticColumns.NONE, reversed, EncodingStats.NO_STATS);
   }

   public PartitionHeader with(DeletionTime newPartitionLevelDeletion) {
      return new PartitionHeader(this.metadata, this.partitionKey, newPartitionLevelDeletion, this.columns, this.isReverseOrder, this.stats);
   }

   public String toString() {
      String cfs = String.format("table %s.%s", new Object[]{this.metadata.keyspace, this.metadata.name});
      return String.format("partition key %s deletion %s %s, columns: %s", new Object[]{this.partitionKey, this.partitionLevelDeletion, cfs, this.columns});
   }

   public static PartitionHeader merge(List<PartitionHeader> sources, UnfilteredRowIterators.MergeListener listener) {
      assert !sources.isEmpty() : "Expected at least one header to merge";

      if(sources.size() == 1 && listener == null) {
         return (PartitionHeader)sources.get(0);
      } else {
         PartitionHeader first = (PartitionHeader)sources.get(0);
         PartitionHeader.Merger merger = new PartitionHeader.Merger(sources.size(), first.metadata, first.partitionKey, first.isReverseOrder, listener);

         for(int i = 0; i < sources.size(); ++i) {
            merger.add(i, (PartitionHeader)sources.get(i));
         }

         return merger.merge();
      }
   }

   public boolean isEmpty() {
      return this.partitionLevelDeletion.isLive();
   }

   static class Merger {
      public final TableMetadata metadata;
      public final DecoratedKey partitionKey;
      public final boolean isReverseOrder;
      public Columns statics;
      public Columns regulars;
      public DeletionTime delTime;
      public DeletionTime[] delTimeVersions;
      public EncodingStats.Merger statsMerger;
      final UnfilteredRowIterators.MergeListener listener;

      public Merger(int size, TableMetadata metadata, DecoratedKey partitionKey, boolean reversed, UnfilteredRowIterators.MergeListener listener) {
         this.metadata = metadata;
         this.partitionKey = partitionKey;
         this.isReverseOrder = reversed;
         this.listener = listener;
         this.statics = Columns.NONE;
         this.regulars = Columns.NONE;
         this.delTime = DeletionTime.LIVE;
         if(listener != null) {
            this.delTimeVersions = new DeletionTime[size];
         }

      }

      public void add(int idx, PartitionHeader source) {
         DeletionTime currDelTime = source.partitionLevelDeletion;
         if(!this.delTime.supersedes(currDelTime)) {
            this.delTime = currDelTime;
         }

         if(this.listener != null) {
            this.delTimeVersions[idx] = currDelTime;
         }

         this.statics = this.statics.mergeTo(source.columns.statics);
         this.regulars = this.regulars.mergeTo(source.columns.regulars);
         EncodingStats stats = source.stats;
         if(!stats.equals(EncodingStats.NO_STATS)) {
            if(this.statsMerger == null) {
               this.statsMerger = new EncodingStats.Merger(stats);
            } else {
               this.statsMerger.mergeWith(stats);
            }
         }

      }

      public PartitionHeader merge() {
         if(this.listener != null) {
            this.listener.onMergedPartitionLevelDeletion(this.delTime, this.delTimeVersions);
         }

         return new PartitionHeader(this.metadata, this.partitionKey, this.delTime, new RegularAndStaticColumns(this.statics, this.regulars), this.isReverseOrder, this.statsMerger != null?this.statsMerger.get():EncodingStats.NO_STATS);
      }
   }
}
