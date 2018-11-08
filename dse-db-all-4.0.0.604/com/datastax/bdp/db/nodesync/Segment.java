package com.datastax.bdp.db.nodesync;

import java.util.Objects;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

public class Segment implements Comparable<Segment> {
   public final TableMetadata table;
   public final Range<Token> range;

   public Segment(TableMetadata table, Range<Token> range) {
      assert table != null && range != null;

      assert !range.isTrulyWrapAround() : range + " is wrapping around";

      this.table = table;
      this.range = range;
   }

   public int compareTo(Segment that) {
      int cmp = ((Token)this.range.left).compareTo(that.range.left);
      if(cmp != 0) {
         return cmp;
      } else {
         cmp = ((Token)this.range.right).compareTo(that.range.right);
         return cmp != 0?cmp:this.table.id.compareTo(that.table.id);
      }
   }

   public boolean equals(Object o) {
      if(!(o instanceof Segment)) {
         return false;
      } else {
         Segment that = (Segment)o;
         return this.table.id.equals(that.table.id) && this.range.equals(that.range);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.table, this.range});
   }

   public String toString() {
      return String.format("%s-%s", new Object[]{this.table, this.range});
   }
}
