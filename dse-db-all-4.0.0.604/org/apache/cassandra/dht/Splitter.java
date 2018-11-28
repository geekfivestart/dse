package org.apache.cassandra.dht;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Splitter {
   private final IPartitioner partitioner;

   protected Splitter(IPartitioner partitioner) {
      this.partitioner = partitioner;
   }

   public List<Token> splitOwnedRanges(int parts, List<Range<Token>> localRanges, boolean dontSplitRanges) {
      if (localRanges == null || localRanges.isEmpty() || parts == 1) {
         return Collections.singletonList(this.partitioner.getMaximumToken());
      }
      double totalTokens = 0.0;
      for (Range<Token> r : localRanges) {
         totalTokens += ((Token)r.left).size((Token)r.right);
      }
      double perPart = totalTokens / (double)parts;
      if (perPart == 0.0) {
         return Collections.singletonList(this.partitioner.getMaximumToken());
      }
      if (dontSplitRanges) {
         return this.splitOwnedRangesNoPartialRanges(localRanges, perPart, parts);
      }
      ArrayList<Token> boundaries = new ArrayList<Token>();
      double sum = 0.0;
      for (Range<Token> r : localRanges) {
         double currentRangeWidth = ((Token)r.left).size((Token)r.right);
         Token left = (Token)r.left;
         while (sum + currentRangeWidth >= perPart) {
            double withinRangeBoundary = perPart - sum;
            double ratio = withinRangeBoundary / currentRangeWidth;
            left = this.partitioner.split(left, (Token)r.right, Math.min(ratio, 1.0));
            boundaries.add(left);
            currentRangeWidth -= withinRangeBoundary;
            sum = 0.0;
         }
         sum += currentRangeWidth;
      }
      if (boundaries.size() < parts) {
         boundaries.add(this.partitioner.getMaximumToken());
      } else {
         boundaries.set(boundaries.size() - 1, this.partitioner.getMaximumToken());
      }
      assert (boundaries.size() == parts);
      return boundaries;
   }

   private List<Token> splitOwnedRangesNoPartialRanges(List<Range<Token>> localRanges, double perPart, int parts) {
      List<Token> boundaries = new ArrayList(parts);
      double sum = 0.0D;
      int i = 0;

      for(int rangesCount = localRanges.size(); boundaries.size() < parts - 1 && i < rangesCount - 1; ++i) {
         Range<Token> r = (Range)localRanges.get(i);
         Range<Token> nextRange = (Range)localRanges.get(i + 1);
         double currentRangeWidth = ((Token)r.left).size((Token)r.right);
         double nextRangeWidth = ((Token)nextRange.left).size((Token)nextRange.right);
         sum += currentRangeWidth;
         if(sum + nextRangeWidth > perPart) {
            double diffCurrent = Math.abs(sum - perPart);
            double diffNext = Math.abs(sum + nextRangeWidth - perPart);
            if(diffNext >= diffCurrent) {
               sum = 0.0D;
               boundaries.add(r.right);
            }
         }
      }

      boundaries.add(this.partitioner.getMaximumToken());
      return boundaries;
   }

   public Set<Range<Token>> split(Collection<Range<Token>> ranges, int parts) {
      int numRanges = ranges.size();
      if(numRanges >= parts) {
         return Sets.newHashSet(ranges);
      } else {
         int partsPerRange = (int)Math.ceil((double)parts / (double)numRanges);
         return (Set)ranges.stream().map((range) -> {
            return this.split(range, partsPerRange);
         }).flatMap(Collection::stream).collect(Collectors.toSet());
      }
   }

   private Set<Range<Token>> split(Range<Token> range, int parts) {
      Token left = (Token)range.left;
      Set<Range<Token>> subranges = new LinkedHashSet(parts);

      for(double i = 1.0D; i < (double)parts; ++i) {
         Token right = this.partitioner.split((Token)range.left, (Token)range.right, i / (double)parts);
         if(!left.equals(right)) {
            subranges.add(new Range(left, right));
         }

         left = right;
      }

      subranges.add(new Range(left, range.right));
      return subranges;
   }

   public double positionInRange(Token token, Range<Token> range) {
      if(((Token)range.left).equals(range.right)) {
         return this.positionInRange(token, new Range(this.partitioner.getMinimumToken(), this.partitioner.getMaximumToken()));
      } else if(token.equals(range.left)) {
         return 0.0D;
      } else if(token.equals(range.right)) {
         return 1.0D;
      } else if(!range.contains(token)) {
         return -1.0D;
      } else {
         double rangeSize = ((Token)range.left).size((Token)range.right);
         double positionSize = ((Token)range.left).size(token);
         return positionSize / rangeSize;
      }
   }
}
