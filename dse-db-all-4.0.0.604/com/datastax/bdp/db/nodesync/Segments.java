package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

class Segments {
   private final TableMetadata table;
   private final Collection<Range<Token>> localRanges;
   private final int depth;
   private final int size;
   private final List<Token> ranges;
   private final Token maxToken;
   private final Comparator<Token> comparator;
   private final List<Range<Token>> normalizedLocalRanges;

   private Segments(TableMetadata table, Collection<Range<Token>> localRanges, int depth, List<Token> ranges, Token maxToken, Comparator<Token> comparator) {
      assert ranges.size() % 2 == 0 : ranges;

      this.table = table;
      this.localRanges = localRanges;
      this.depth = depth;
      this.size = ranges.size() / 2;
      this.ranges = ranges;
      this.maxToken = maxToken;
      this.comparator = comparator;
      this.normalizedLocalRanges = Range.normalize(localRanges);
   }

   Collection<Range<Token>> localRanges() {
      return this.localRanges;
   }

   List<Range<Token>> normalizedLocalRanges() {
      return this.normalizedLocalRanges;
   }

   int depth() {
      return this.depth;
   }

   int size() {
      return this.size;
   }

   Segment get(int i) {
      int rangeIdx = 2 * i;
      Token left = (Token)this.ranges.get(rangeIdx);
      Token right = (Token)this.ranges.get(rangeIdx + 1);
      if(right == this.maxToken) {
         right = this.table.partitioner.getMinimumToken();
      }

      return new Segment(this.table, new Range(left, right));
   }

   int[] findFullyIncludedIn(Segment segment) {
      int startIdx = Collections.binarySearch(this.ranges, segment.range.left, this.comparator);
      if(startIdx < 0) {
         startIdx = -startIdx - 1;
      }

      if(!this.isStartBound(startIdx)) {
         ++startIdx;
      }

      if(startIdx >= this.ranges.size()) {
         return new int[]{0, 0};
      } else {
         int endIdx = this.ranges.size();
         if(!((Token)segment.range.right).isMinimum()) {
            endIdx = Collections.binarySearch(this.ranges.subList(startIdx, this.ranges.size()), segment.range.right, this.comparator);
            boolean isExactMatch = endIdx >= 0;
            if(!isExactMatch) {
               endIdx = -endIdx - 1;
            }

            endIdx += startIdx;
            if(!this.isStartBound(endIdx)) {
               endIdx = isExactMatch?endIdx + 1:endIdx - 1;
            }
         }

         return new int[]{startIdx / 2, endIdx / 2};
      }
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.table).append('{');

      for(int i = 0; i < this.size; ++i) {
         sb.append(i == 0?"":", ").append(this.get(i).range);
      }

      return sb.append('}').toString();
   }

   private boolean isStartBound(int idx) {
      return idx % 2 == 0;
   }

   static int depth(ColumnFamilyStore table, int localRangesCount) {
      return depth(NodeSyncHelpers.estimatedSizeOf(table), localRangesCount, NodeSyncHelpers.segmentSizeTarget());
   }

   @VisibleForTesting
   static int depth(long estimatedTableSizeInBytes, int localRangesCount, long maxSegmentSizeInBytes) {
      long rangeSize = estimatedTableSizeInBytes / (long)localRangesCount;

      int depth;
      for(depth = 0; rangeSize > maxSegmentSizeInBytes; rangeSize /= 2L) {
         ++depth;
      }

      return depth;
   }

   static Segments generate(TableMetadata table, Collection<Range<Token>> localRanges, int depth) {
      Token maxToken = table.partitioner.midpoint(table.partitioner.getMinimumToken(), table.partitioner.getMinimumToken());
      Comparator<Token> comparator = (t1, t2) -> {
         return t1 == maxToken?(t2 == maxToken?0:1):(t2 == maxToken?-1:t1.compareTo(t2));
      };
      List<Token> ranges = new ArrayList(estimateSegments(localRanges, depth) * 2);
      Iterator var6 = localRanges.iterator();

      while(var6.hasNext()) {
         Range<Token> localRange = (Range)var6.next();
         splitRangeAndUnwrap(localRange, depth, table.partitioner, ranges, maxToken);
      }

      ranges.sort(comparator);
      return new Segments(table, localRanges, depth, ranges, maxToken, comparator);
   }

   static Segments updateTable(Segments segments, TableMetadata table) {
      return new Segments(table, segments.localRanges, segments.depth, segments.ranges, segments.maxToken, segments.comparator);
   }

   @VisibleForTesting
   static int estimateSegments(Collection<Range<Token>> localRanges, int depth) {
      assert !localRanges.isEmpty();

      int segments = localRanges.size();

      for(int i = 0; i < depth; ++i) {
         segments *= 2;
      }

      Iterator var5 = localRanges.iterator();

      while(var5.hasNext()) {
         Range<Token> range = (Range)var5.next();
         if(range.isTrulyWrapAround()) {
            ++segments;
         }
      }

      return segments;
   }

   private static void splitRangeAndUnwrap(Range<Token> toSplit, int depth, IPartitioner partitioner, List<Token> output, Token maxToken) {
      assert depth >= 0 : "Invalid depth " + depth;

      if(depth == 0) {
         maybeUnwrap(toSplit, output, maxToken);
      } else if(depth == 1) {
         Token midpoint = midpoint((Token)toSplit.left, (Token)toSplit.right, partitioner);
         if(midpoint == null) {
            maybeUnwrap(toSplit, output, maxToken);
         } else {
            maybeUnwrap((Token)toSplit.left, midpoint, output, maxToken);
            maybeUnwrap(midpoint, (Token)toSplit.right, output, maxToken);
         }

      } else {
         Deque<Segments.RangeWithDepth> queue = new ArrayDeque(depth + 1);
         queue.offerFirst(withDepth((Token)toSplit.left, (Token)toSplit.right, 0));

         while(!queue.isEmpty()) {
            Segments.RangeWithDepth range = (Segments.RangeWithDepth)queue.poll();

            assert range.depth < depth;

            Token midpoint = midpoint(range.left, range.right, partitioner);
            if(midpoint == null) {
               maybeUnwrap(range.left, range.right, output, maxToken);
            } else {
               int nextDepth = range.depth + 1;
               if(nextDepth == depth) {
                  maybeUnwrap(range.left, midpoint, output, maxToken);
                  maybeUnwrap(midpoint, range.right, output, maxToken);
               } else {
                  queue.offerFirst(withDepth(midpoint, range.right, nextDepth));
                  queue.offerFirst(withDepth(range.left, midpoint, nextDepth));
               }
            }
         }

      }
   }

   private static void maybeUnwrap(Range<Token> range, List<Token> output, Token maxToken) {
      maybeUnwrap((Token)range.left, (Token)range.right, output, maxToken);
   }

   private static void maybeUnwrap(Token left, Token right, List<Token> output, Token maxToken) {
      if(Range.isTrulyWrapAround(left, right)) {
         add(left, maxToken, output);
         add(left.minValue(), right, output);
      } else {
         add(left, right.isMinimum()?maxToken:right, output);
      }

   }

   private static void add(Token left, Token right, List<Token> output) {
      output.add(left);
      output.add(right);
   }

   private static Token midpoint(Token left, Token right, IPartitioner partitioner) {
      Token midpoint = partitioner.midpoint(left, right);
      return !midpoint.equals(left) && !midpoint.equals(right)?midpoint:null;
   }

   private static Segments.RangeWithDepth withDepth(Token left, Token right, int depth) {
      return new Segments.RangeWithDepth(left, right, depth);
   }

   private static class RangeWithDepth {
      private final Token left;
      private final Token right;
      private final int depth;

      private RangeWithDepth(Token left, Token right, int depth) {
         this.left = left;
         this.right = right;
         this.depth = depth;
      }
   }
}
