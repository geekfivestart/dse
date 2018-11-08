package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

public class TPCBoundaries {
   private static final Token[] EMPTY_TOKEN_ARRAY = new Token[0];
   public static TPCBoundaries NONE;
   public static final TPCBoundaries LOCAL;
   private final Token[] boundaries;

   private TPCBoundaries(Token[] boundaries) {
      this.boundaries = boundaries;
   }

   private static TPCBoundaries computeLocalRanges(IPartitioner partitioner, int numCores) {
      return !partitioner.splitter().isPresent()?NONE:compute(Collections.singletonList(new Range(partitioner.getMinimumToken(), partitioner.getMaximumToken())), numCores);
   }

   public static TPCBoundaries compute(List<Range<Token>> localRanges, int numCores) {
      assert numCores > 0;

      if(numCores == 1) {
         return NONE;
      } else {
         IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

         assert partitioner.splitter().isPresent() : partitioner.getClass().getName() + " doesn't support cpu boundary splitting";

         Splitter splitter = (Splitter)partitioner.splitter().get();
         List<Token> boundaries = splitter.splitOwnedRanges(numCores, localRanges, false);
         return new TPCBoundaries((Token[])boundaries.subList(0, boundaries.size() - 1).toArray(EMPTY_TOKEN_ARRAY));
      }
   }

   int getCoreFor(Token tk) {
      for(int i = 0; i < this.boundaries.length; ++i) {
         if(tk.compareTo(this.boundaries[i]) < 0) {
            return i;
         }
      }

      return this.boundaries.length;
   }

   public int supportedCores() {
      return this.boundaries.length + 1;
   }

   public List<Range<Token>> asRanges() {
      IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
      List<Range<Token>> ranges = new ArrayList(this.boundaries.length + 1);
      Token left = partitioner.getMinimumToken();
      Token[] var4 = this.boundaries;
      int var5 = var4.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         Token right = var4[var6];
         ranges.add(new Range(left, right));
         left = right;
      }

      ranges.add(new Range(left, partitioner.getMaximumToken()));
      return ranges;
   }

   public String toString() {
      if(this.boundaries.length == 0) {
         return "core 0: (min, max)";
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append("core 0: (min, ").append(this.boundaries[0]).append(") ");

         for(int i = 0; i < this.boundaries.length - 1; ++i) {
            sb.append("core ").append(i + 1).append(": (").append(this.boundaries[i]).append(", ").append(this.boundaries[i + 1]).append("] ");
         }

         sb.append("core ").append(this.boundaries.length).append(": (").append(this.boundaries[this.boundaries.length - 1]).append(", max)");
         return sb.toString();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         TPCBoundaries that = (TPCBoundaries)o;
         return Arrays.equals(this.boundaries, that.boundaries);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Arrays.hashCode(this.boundaries);
   }

   static {
      NONE = new TPCBoundaries(EMPTY_TOKEN_ARRAY);
      LOCAL = computeLocalRanges(DatabaseDescriptor.getPartitioner(), TPC.getNumCores());
   }
}
