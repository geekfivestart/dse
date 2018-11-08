package org.apache.cassandra.db;

import com.google.common.collect.Iterators;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class RangeTombstoneList implements Iterable<RangeTombstone>, IMeasurableMemory {
   private static long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList((ClusteringComparator)null, 0));
   private final ClusteringComparator comparator;
   private ClusteringBound[] starts;
   private ClusteringBound[] ends;
   private long[] markedAts;
   private int[] delTimes;
   private long boundaryHeapSize;
   private int size;

   private RangeTombstoneList(ClusteringComparator comparator, ClusteringBound[] starts, ClusteringBound[] ends, long[] markedAts, int[] delTimes, long boundaryHeapSize, int size) {
      assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;

      this.comparator = comparator;
      this.starts = starts;
      this.ends = ends;
      this.markedAts = markedAts;
      this.delTimes = delTimes;
      this.size = size;
      this.boundaryHeapSize = boundaryHeapSize;
   }

   public RangeTombstoneList(ClusteringComparator comparator, int capacity) {
      this(comparator, new ClusteringBound[capacity], new ClusteringBound[capacity], new long[capacity], new int[capacity], 0L, 0);
   }

   public boolean isEmpty() {
      return this.size == 0;
   }

   public int size() {
      return this.size;
   }

   public ClusteringComparator comparator() {
      return this.comparator;
   }

   public RangeTombstoneList copy() {
      return new RangeTombstoneList(this.comparator, (ClusteringBound[])Arrays.copyOf(this.starts, this.size), (ClusteringBound[])Arrays.copyOf(this.ends, this.size), Arrays.copyOf(this.markedAts, this.size), Arrays.copyOf(this.delTimes, this.size), this.boundaryHeapSize, this.size);
   }

   public RangeTombstoneList copy(AbstractAllocator allocator) {
      RangeTombstoneList copy = new RangeTombstoneList(this.comparator, new ClusteringBound[this.size], new ClusteringBound[this.size], Arrays.copyOf(this.markedAts, this.size), Arrays.copyOf(this.delTimes, this.size), this.boundaryHeapSize, this.size);

      for(int i = 0; i < this.size; ++i) {
         copy.starts[i] = clone(this.starts[i], allocator);
         copy.ends[i] = clone(this.ends[i], allocator);
      }

      return copy;
   }

   private static ClusteringBound clone(ClusteringBound bound, AbstractAllocator allocator) {
      ByteBuffer[] values = new ByteBuffer[bound.size()];

      for(int i = 0; i < values.length; ++i) {
         values[i] = allocator.clone(bound.get(i));
      }

      return new ClusteringBound(bound.kind(), values);
   }

   public void add(RangeTombstone tombstone) {
      this.add(tombstone.deletedSlice().start(), tombstone.deletedSlice().end(), tombstone.deletionTime().markedForDeleteAt(), tombstone.deletionTime().localDeletionTime());
   }

   public void add(ClusteringBound start, ClusteringBound end, long markedAt, int delTime) {
      if(this.isEmpty()) {
         this.addInternal(0, start, end, markedAt, delTime);
      } else {
         int c = this.comparator.compare((ClusteringPrefix)this.ends[this.size - 1], (ClusteringPrefix)start);
         if(c <= 0) {
            this.addInternal(this.size, start, end, markedAt, delTime);
         } else {
            int pos = Arrays.binarySearch(this.ends, 0, this.size, start, this.comparator);
            this.insertFrom(pos >= 0?pos + 1:-pos - 1, start, end, markedAt, delTime);
         }

         this.boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
      }
   }

   public void addAll(RangeTombstoneList tombstones) {
      if(!tombstones.isEmpty()) {
         if(this.isEmpty()) {
            copyArrays(tombstones, this);
         } else {
            int i;
            if(this.size > 10 * tombstones.size) {
               for(i = 0; i < tombstones.size; ++i) {
                  this.add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
               }
            } else {
               i = 0;
               int j = 0;

               while(i < this.size && j < tombstones.size) {
                  if(this.comparator.compare((ClusteringPrefix)tombstones.starts[j], (ClusteringPrefix)this.ends[i]) < 0) {
                     this.insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                     ++j;
                  } else {
                     ++i;
                  }
               }

               while(j < tombstones.size) {
                  this.addInternal(this.size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                  ++j;
               }
            }

         }
      }
   }

   public boolean isDeleted(Clustering clustering, Cell cell) {
      int idx = this.searchInternal(clustering, 0, this.size);
      return idx >= 0 && (cell.isCounterCell() || this.markedAts[idx] >= cell.timestamp());
   }

   public DeletionTime searchDeletionTime(Clustering name) {
      int idx = this.searchInternal(name, 0, this.size);
      return idx < 0?null:new DeletionTime(this.markedAts[idx], this.delTimes[idx]);
   }

   public RangeTombstone search(Clustering name) {
      int idx = this.searchInternal(name, 0, this.size);
      return idx < 0?null:this.rangeTombstone(idx);
   }

   private int searchInternal(ClusteringPrefix name, int startIdx, int endIdx) {
      if(this.isEmpty()) {
         return -1;
      } else {
         int pos = Arrays.binarySearch(this.starts, startIdx, endIdx, name, this.comparator);
         if(pos >= 0) {
            return -pos - 1;
         } else {
            int idx = -pos - 2;
            return idx < 0?-1:(this.comparator.compare((ClusteringPrefix)name, (ClusteringPrefix)this.ends[idx]) < 0?idx:-idx - 2);
         }
      }
   }

   public int dataSize() {
      int dataSize = TypeSizes.sizeof(this.size);

      for(int i = 0; i < this.size; ++i) {
         dataSize += this.starts[i].dataSize() + this.ends[i].dataSize();
         dataSize += TypeSizes.sizeof(this.markedAts[i]);
         dataSize += TypeSizes.sizeof(this.delTimes[i]);
      }

      return dataSize;
   }

   public long maxMarkedAt() {
      long max = -9223372036854775808L;

      for(int i = 0; i < this.size; ++i) {
         max = Math.max(max, this.markedAts[i]);
      }

      return max;
   }

   public void collectStats(EncodingStats.Collector collector) {
      for(int i = 0; i < this.size; ++i) {
         collector.updateTimestamp(this.markedAts[i]);
         collector.updateLocalDeletionTime(this.delTimes[i]);
      }

   }

   public void updateAllTimestamp(long timestamp) {
      for(int i = 0; i < this.size; ++i) {
         this.markedAts[i] = timestamp;
      }

   }

   private RangeTombstone rangeTombstone(int idx) {
      return new RangeTombstone(Slice.make(this.starts[idx], this.ends[idx]), new DeletionTime(this.markedAts[idx], this.delTimes[idx]));
   }

   private RangeTombstone rangeTombstoneWithNewStart(int idx, ClusteringBound newStart) {
      return new RangeTombstone(Slice.make(newStart, this.ends[idx]), new DeletionTime(this.markedAts[idx], this.delTimes[idx]));
   }

   private RangeTombstone rangeTombstoneWithNewEnd(int idx, ClusteringBound newEnd) {
      return new RangeTombstone(Slice.make(this.starts[idx], newEnd), new DeletionTime(this.markedAts[idx], this.delTimes[idx]));
   }

   private RangeTombstone rangeTombstoneWithNewBounds(int idx, ClusteringBound newStart, ClusteringBound newEnd) {
      return new RangeTombstone(Slice.make(newStart, newEnd), new DeletionTime(this.markedAts[idx], this.delTimes[idx]));
   }

   public Iterator<RangeTombstone> iterator() {
      return this.iterator(false);
   }

   public Iterator<RangeTombstone> iterator(boolean reversed) {
      return reversed?new AbstractIterator<RangeTombstone>() {
         private int idx;

         {
            this.idx = RangeTombstoneList.this.size - 1;
         }

         protected RangeTombstone computeNext() {
            return this.idx < 0?(RangeTombstone)this.endOfData():RangeTombstoneList.this.rangeTombstone(this.idx--);
         }
      }:new AbstractIterator<RangeTombstone>() {
         private int idx;

         protected RangeTombstone computeNext() {
            return this.idx >= RangeTombstoneList.this.size?(RangeTombstone)this.endOfData():RangeTombstoneList.this.rangeTombstone(this.idx++);
         }
      };
   }

   public Iterator<RangeTombstone> iterator(Slice slice, boolean reversed) {
      return reversed?this.reverseIterator(slice):this.forwardIterator(slice);
   }

   private Iterator<RangeTombstone> forwardIterator(final Slice slice) {
      int startIdx = slice.start() == ClusteringBound.BOTTOM?0:this.searchInternal(slice.start(), 0, this.size);
      final int start = startIdx < 0?-startIdx - 1:startIdx;
      if(start >= this.size) {
         return Collections.emptyIterator();
      } else {
         int finishIdx = slice.end() == ClusteringBound.TOP?this.size - 1:this.searchInternal(slice.end(), start, this.size);
         final int finish = finishIdx < 0?-finishIdx - 2:finishIdx;
         if(start > finish) {
            return Collections.emptyIterator();
         } else if(start == finish) {
            ClusteringBound s = this.comparator.compare((ClusteringPrefix)this.starts[start], (ClusteringPrefix)slice.start()) < 0?slice.start():this.starts[start];
            ClusteringBound e = this.comparator.compare((ClusteringPrefix)slice.end(), (ClusteringPrefix)this.ends[start]) < 0?slice.end():this.ends[start];
            return Iterators.singletonIterator(this.rangeTombstoneWithNewBounds(start, s, e));
         } else {
            return new AbstractIterator<RangeTombstone>() {
               private int idx = start;

               protected RangeTombstone computeNext() {
                  return this.idx < RangeTombstoneList.this.size && this.idx <= finish?(this.idx == start && RangeTombstoneList.this.comparator.compare((ClusteringPrefix)RangeTombstoneList.this.starts[this.idx], (ClusteringPrefix)slice.start()) < 0?RangeTombstoneList.this.rangeTombstoneWithNewStart(this.idx++, slice.start()):(this.idx == finish && RangeTombstoneList.this.comparator.compare((ClusteringPrefix)slice.end(), (ClusteringPrefix)RangeTombstoneList.this.ends[this.idx]) < 0?RangeTombstoneList.this.rangeTombstoneWithNewEnd(this.idx++, slice.end()):RangeTombstoneList.this.rangeTombstone(this.idx++))):(RangeTombstone)this.endOfData();
               }
            };
         }
      }
   }

   private Iterator<RangeTombstone> reverseIterator(final Slice slice) {
      int startIdx = slice.end() == ClusteringBound.TOP?this.size - 1:this.searchInternal(slice.end(), 0, this.size);
      final int start = startIdx < 0?-startIdx - 2:startIdx;
      if(start < 0) {
         return Collections.emptyIterator();
      } else {
         int finishIdx = slice.start() == ClusteringBound.BOTTOM?0:this.searchInternal(slice.start(), 0, start + 1);
         final int finish = finishIdx < 0?-finishIdx - 1:finishIdx;
         if(start < finish) {
            return Collections.emptyIterator();
         } else if(start == finish) {
            ClusteringBound s = this.comparator.compare((ClusteringPrefix)this.starts[start], (ClusteringPrefix)slice.start()) < 0?slice.start():this.starts[start];
            ClusteringBound e = this.comparator.compare((ClusteringPrefix)slice.end(), (ClusteringPrefix)this.ends[start]) < 0?slice.end():this.ends[start];
            return Iterators.singletonIterator(this.rangeTombstoneWithNewBounds(start, s, e));
         } else {
            return new AbstractIterator<RangeTombstone>() {
               private int idx = start;

               protected RangeTombstone computeNext() {
                  return this.idx >= 0 && this.idx >= finish?(this.idx == start && RangeTombstoneList.this.comparator.compare((ClusteringPrefix)slice.end(), (ClusteringPrefix)RangeTombstoneList.this.ends[this.idx]) < 0?RangeTombstoneList.this.rangeTombstoneWithNewEnd(this.idx--, slice.end()):(this.idx == finish && RangeTombstoneList.this.comparator.compare((ClusteringPrefix)RangeTombstoneList.this.starts[this.idx], (ClusteringPrefix)slice.start()) < 0?RangeTombstoneList.this.rangeTombstoneWithNewStart(this.idx--, slice.start()):RangeTombstoneList.this.rangeTombstone(this.idx--))):(RangeTombstone)this.endOfData();
               }
            };
         }
      }
   }

   public boolean equals(Object o) {
      if(!(o instanceof RangeTombstoneList)) {
         return false;
      } else {
         RangeTombstoneList that = (RangeTombstoneList)o;
         if(this.size != that.size) {
            return false;
         } else {
            for(int i = 0; i < this.size; ++i) {
               if(!this.starts[i].equals(that.starts[i])) {
                  return false;
               }

               if(!this.ends[i].equals(that.ends[i])) {
                  return false;
               }

               if(this.markedAts[i] != that.markedAts[i]) {
                  return false;
               }

               if(this.delTimes[i] != that.delTimes[i]) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public final int hashCode() {
      int result = this.size;

      for(int i = 0; i < this.size; ++i) {
         result += this.starts[i].hashCode() + this.ends[i].hashCode();
         result += (int)(this.markedAts[i] ^ this.markedAts[i] >>> 32);
         result += this.delTimes[i];
      }

      return result;
   }

   private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst) {
      dst.grow(src.size);
      System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
      System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
      System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
      System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
      dst.size = src.size;
      dst.boundaryHeapSize = src.boundaryHeapSize;
   }

   private void insertFrom(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime) {
      while(i < this.size) {
         assert start.isStart() && end.isEnd();

         assert i == 0 || this.comparator.compare((ClusteringPrefix)this.ends[i - 1], (ClusteringPrefix)start) <= 0;

         assert this.comparator.compare((ClusteringPrefix)start, (ClusteringPrefix)this.ends[i]) < 0;

         if(Slice.isEmpty(this.comparator, start, end)) {
            return;
         }

         ClusteringBound newEnd;
         if(markedAt > this.markedAts[i]) {
            if(this.comparator.compare((ClusteringPrefix)this.starts[i], (ClusteringPrefix)start) < 0) {
               newEnd = start.invert();
               if(!Slice.isEmpty(this.comparator, this.starts[i], newEnd)) {
                  this.addInternal(i, this.starts[i], newEnd, this.markedAts[i], this.delTimes[i]);
                  ++i;
                  this.setInternal(i, start, this.ends[i], this.markedAts[i], this.delTimes[i]);
               }
            }

            int endCmp = this.comparator.compare((ClusteringPrefix)end, (ClusteringPrefix)this.starts[i]);
            if(endCmp < 0) {
               this.addInternal(i, start, end, markedAt, delTime);
               return;
            }

            int cmp = this.comparator.compare((ClusteringPrefix)this.ends[i], (ClusteringPrefix)end);
            if(cmp <= 0) {
               if(i != this.size - 1 && this.comparator.compare((ClusteringPrefix)end, (ClusteringPrefix)this.starts[i + 1]) > 0) {
                  this.setInternal(i, start, this.starts[i + 1].invert(), markedAt, delTime);
                  start = this.starts[i + 1];
                  ++i;
                  continue;
               }

               this.setInternal(i, start, end, markedAt, delTime);
               return;
            }

            this.addInternal(i, start, end, markedAt, delTime);
            ++i;
            ClusteringBound newStart = end.invert();
            if(!Slice.isEmpty(this.comparator, newStart, this.ends[i])) {
               this.setInternal(i, newStart, this.ends[i], this.markedAts[i], this.delTimes[i]);
            }

            return;
         } else {
            if(this.comparator.compare((ClusteringPrefix)start, (ClusteringPrefix)this.starts[i]) < 0) {
               if(this.comparator.compare((ClusteringPrefix)end, (ClusteringPrefix)this.starts[i]) <= 0) {
                  this.addInternal(i, start, end, markedAt, delTime);
                  return;
               }

               newEnd = this.starts[i].invert();
               if(!Slice.isEmpty(this.comparator, start, newEnd)) {
                  this.addInternal(i, start, newEnd, markedAt, delTime);
                  ++i;
               }
            }

            if(this.comparator.compare((ClusteringPrefix)end, (ClusteringPrefix)this.ends[i]) <= 0) {
               return;
            }

            start = this.ends[i].invert();
            ++i;
         }
      }

      this.addInternal(i, start, end, markedAt, delTime);
   }

   private int capacity() {
      return this.starts.length;
   }

   private void addInternal(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime) {
      assert i >= 0;

      if(this.size == this.capacity()) {
         this.growToFree(i);
      } else if(i < this.size) {
         this.moveElements(i);
      }

      this.setInternal(i, start, end, markedAt, delTime);
      ++this.size;
   }

   private void growToFree(int i) {
      int newLength = this.capacity() * 3 / 2 + 1;
      this.grow(i, newLength);
   }

   private void grow(int newLength) {
      if(this.capacity() < newLength) {
         this.grow(-1, newLength);
      }

   }

   private void grow(int i, int newLength) {
      this.starts = grow(this.starts, this.size, newLength, i);
      this.ends = grow(this.ends, this.size, newLength, i);
      this.markedAts = grow(this.markedAts, this.size, newLength, i);
      this.delTimes = grow(this.delTimes, this.size, newLength, i);
   }

   private static ClusteringBound[] grow(ClusteringBound[] a, int size, int newLength, int i) {
      if(i >= 0 && i < size) {
         ClusteringBound[] newA = new ClusteringBound[newLength];
         System.arraycopy(a, 0, newA, 0, i);
         System.arraycopy(a, i, newA, i + 1, size - i);
         return newA;
      } else {
         return (ClusteringBound[])Arrays.copyOf(a, newLength);
      }
   }

   private static long[] grow(long[] a, int size, int newLength, int i) {
      if(i >= 0 && i < size) {
         long[] newA = new long[newLength];
         System.arraycopy(a, 0, newA, 0, i);
         System.arraycopy(a, i, newA, i + 1, size - i);
         return newA;
      } else {
         return Arrays.copyOf(a, newLength);
      }
   }

   private static int[] grow(int[] a, int size, int newLength, int i) {
      if(i >= 0 && i < size) {
         int[] newA = new int[newLength];
         System.arraycopy(a, 0, newA, 0, i);
         System.arraycopy(a, i, newA, i + 1, size - i);
         return newA;
      } else {
         return Arrays.copyOf(a, newLength);
      }
   }

   private void moveElements(int i) {
      if(i < this.size) {
         System.arraycopy(this.starts, i, this.starts, i + 1, this.size - i);
         System.arraycopy(this.ends, i, this.ends, i + 1, this.size - i);
         System.arraycopy(this.markedAts, i, this.markedAts, i + 1, this.size - i);
         System.arraycopy(this.delTimes, i, this.delTimes, i + 1, this.size - i);
         this.starts[i] = null;
      }
   }

   private void setInternal(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime) {
      if(this.starts[i] != null) {
         this.boundaryHeapSize -= this.starts[i].unsharedHeapSize() + this.ends[i].unsharedHeapSize();
      }

      this.starts[i] = start;
      this.ends[i] = end;
      this.markedAts[i] = markedAt;
      this.delTimes[i] = delTime;
      this.boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + this.boundaryHeapSize + ObjectSizes.sizeOfArray((Object[])this.starts) + ObjectSizes.sizeOfArray((Object[])this.ends) + ObjectSizes.sizeOfArray(this.markedAts) + ObjectSizes.sizeOfArray(this.delTimes);
   }
}
