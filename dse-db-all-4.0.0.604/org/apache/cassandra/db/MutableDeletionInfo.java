package org.apache.cassandra.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class MutableDeletionInfo implements DeletionInfo {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new MutableDeletionInfo(0L, 0));
   private DeletionTime partitionDeletion;
   private RangeTombstoneList ranges;

   public MutableDeletionInfo(long markedForDeleteAt, int localDeletionTime) {
      this(new DeletionTime(markedForDeleteAt, localDeletionTime == -2147483648?2147483647:localDeletionTime));
   }

   public MutableDeletionInfo(DeletionTime partitionDeletion) {
      this(partitionDeletion, (RangeTombstoneList)null);
   }

   public MutableDeletionInfo(DeletionTime partitionDeletion, RangeTombstoneList ranges) {
      this.partitionDeletion = partitionDeletion;
      this.ranges = ranges;
   }

   public static MutableDeletionInfo live() {
      return new MutableDeletionInfo(DeletionTime.LIVE);
   }

   public MutableDeletionInfo mutableCopy() {
      return new MutableDeletionInfo(this.partitionDeletion, this.ranges == null?null:this.ranges.copy());
   }

   public MutableDeletionInfo copy(AbstractAllocator allocator) {
      RangeTombstoneList rangesCopy = null;
      if(this.ranges != null) {
         rangesCopy = this.ranges.copy(allocator);
      }

      return new MutableDeletionInfo(this.partitionDeletion, rangesCopy);
   }

   public boolean isLive() {
      return this.partitionDeletion.isLive() && (this.ranges == null || this.ranges.isEmpty());
   }

   public void add(DeletionTime newInfo) {
      if(newInfo.supersedes(this.partitionDeletion)) {
         this.partitionDeletion = newInfo;
      }

   }

   public void add(RangeTombstone tombstone, ClusteringComparator comparator) {
      if(this.ranges == null) {
         this.ranges = new RangeTombstoneList(comparator, 1);
      }

      this.ranges.add(tombstone);
   }

   public DeletionInfo add(DeletionInfo newInfo) {
      this.add(newInfo.getPartitionDeletion());

      assert newInfo instanceof MutableDeletionInfo;

      RangeTombstoneList newRanges = ((MutableDeletionInfo)newInfo).ranges;
      if(this.ranges == null) {
         this.ranges = newRanges == null?null:newRanges.copy();
      } else if(newRanges != null) {
         this.ranges.addAll(newRanges);
      }

      return this;
   }

   public DeletionTime getPartitionDeletion() {
      return this.partitionDeletion;
   }

   public Iterator<RangeTombstone> rangeIterator(boolean reversed) {
      return this.ranges == null?Collections.emptyIterator():this.ranges.iterator(reversed);
   }

   public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed) {
      return this.ranges == null?Collections.emptyIterator():this.ranges.iterator(slice, reversed);
   }

   public RangeTombstone rangeCovering(Clustering name) {
      return this.ranges == null?null:this.ranges.search(name);
   }

   public int dataSize() {
      int size = TypeSizes.sizeof(this.partitionDeletion.markedForDeleteAt());
      return size + (this.ranges == null?0:this.ranges.dataSize());
   }

   public boolean hasRanges() {
      return this.ranges != null && !this.ranges.isEmpty();
   }

   public int rangeCount() {
      return this.hasRanges()?this.ranges.size():0;
   }

   public long maxTimestamp() {
      return this.ranges == null?this.partitionDeletion.markedForDeleteAt():Math.max(this.partitionDeletion.markedForDeleteAt(), this.ranges.maxMarkedAt());
   }

   public boolean mayModify(DeletionInfo delInfo) {
      return this.partitionDeletion.compareTo(delInfo.getPartitionDeletion()) > 0 || this.hasRanges();
   }

   public String toString() {
      return this.ranges != null && !this.ranges.isEmpty()?String.format("{%s, ranges=%s}", new Object[]{this.partitionDeletion, this.rangesAsString()}):String.format("{%s}", new Object[]{this.partitionDeletion});
   }

   private String rangesAsString() {
      assert !this.ranges.isEmpty();

      StringBuilder sb = new StringBuilder();
      ClusteringComparator cc = this.ranges.comparator();
      Iterator iter = this.rangeIterator(false);

      while(iter.hasNext()) {
         RangeTombstone i = (RangeTombstone)iter.next();
         sb.append(i.deletedSlice().toString(cc));
         sb.append('@');
         sb.append(i.deletionTime());
      }

      return sb.toString();
   }

   public DeletionInfo updateAllTimestamp(long timestamp) {
      if(this.partitionDeletion.markedForDeleteAt() != -9223372036854775808L) {
         this.partitionDeletion = new DeletionTime(timestamp, this.partitionDeletion.localDeletionTime());
      }

      if(this.ranges != null) {
         this.ranges.updateAllTimestamp(timestamp);
      }

      return this;
   }

   public boolean equals(Object o) {
      if(!(o instanceof MutableDeletionInfo)) {
         return false;
      } else {
         MutableDeletionInfo that = (MutableDeletionInfo)o;
         return this.partitionDeletion.equals(that.partitionDeletion) && Objects.equals(this.ranges, that.ranges);
      }
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{this.partitionDeletion, this.ranges});
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + this.partitionDeletion.unsharedHeapSize() + (this.ranges == null?0L:this.ranges.unsharedHeapSize());
   }

   public void collectStats(EncodingStats.Collector collector) {
      collector.update(this.partitionDeletion);
      if(this.ranges != null) {
         this.ranges.collectStats(collector);
      }

   }

   public static MutableDeletionInfo.Builder builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed) {
      return new MutableDeletionInfo.Builder(partitionLevelDeletion, comparator, reversed);
   }

   public static class Builder {
      private final MutableDeletionInfo deletion;
      private final ClusteringComparator comparator;
      private final boolean reversed;
      private RangeTombstoneMarker openMarker;

      private Builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed) {
         this.deletion = new MutableDeletionInfo(partitionLevelDeletion);
         this.comparator = comparator;
         this.reversed = reversed;
      }

      public void add(RangeTombstoneMarker marker) {
         if(marker.isClose(this.reversed)) {
            DeletionTime openDeletion = this.openMarker.openDeletionTime(this.reversed);

            assert marker.closeDeletionTime(this.reversed).equals(openDeletion);

            ClusteringBound open = this.openMarker.openBound(this.reversed);
            ClusteringBound close = marker.closeBound(this.reversed);
            Slice slice = this.reversed?Slice.make(close, open):Slice.make(open, close);
            this.deletion.add(new RangeTombstone(slice, openDeletion), this.comparator);
         }

         if(marker.isOpen(this.reversed)) {
            this.openMarker = marker;
         }

      }

      public MutableDeletionInfo build() {
         return this.deletion;
      }
   }
}
