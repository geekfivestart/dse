package org.apache.cassandra.db;

import java.util.Objects;

public class RangeTombstone {
   private final Slice slice;
   private final DeletionTime deletion;

   public RangeTombstone(Slice slice, DeletionTime deletion) {
      this.slice = slice;
      this.deletion = deletion;
   }

   public Slice deletedSlice() {
      return this.slice;
   }

   public DeletionTime deletionTime() {
      return this.deletion;
   }

   public String toString(ClusteringComparator comparator) {
      return this.slice.toString(comparator) + '@' + this.deletion;
   }

   public boolean equals(Object other) {
      if(!(other instanceof RangeTombstone)) {
         return false;
      } else {
         RangeTombstone that = (RangeTombstone)other;
         return this.deletedSlice().equals(that.deletedSlice()) && this.deletionTime().equals(that.deletionTime());
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.deletedSlice(), this.deletionTime()});
   }
}
