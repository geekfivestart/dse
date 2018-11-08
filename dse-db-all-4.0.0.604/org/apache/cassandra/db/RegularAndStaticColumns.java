package org.apache.cassandra.db;

import com.google.common.collect.Iterators;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;

public class RegularAndStaticColumns implements Iterable<ColumnMetadata> {
   public static RegularAndStaticColumns NONE;
   public final Columns statics;
   public final Columns regulars;

   public RegularAndStaticColumns(Columns statics, Columns regulars) {
      assert statics != null && regulars != null;

      this.statics = statics;
      this.regulars = regulars;
   }

   public static RegularAndStaticColumns of(ColumnMetadata column) {
      return new RegularAndStaticColumns(column.isStatic()?Columns.of(column):Columns.NONE, column.isStatic()?Columns.NONE:Columns.of(column));
   }

   public RegularAndStaticColumns without(ColumnMetadata column) {
      return new RegularAndStaticColumns(column.isStatic()?this.statics.without(column):this.statics, column.isStatic()?this.regulars:this.regulars.without(column));
   }

   public RegularAndStaticColumns mergeTo(RegularAndStaticColumns that) {
      if(this == that) {
         return this;
      } else {
         Columns statics = this.statics.mergeTo(that.statics);
         Columns regulars = this.regulars.mergeTo(that.regulars);
         return statics == this.statics && regulars == this.regulars?this:(statics == that.statics && regulars == that.regulars?that:new RegularAndStaticColumns(statics, regulars));
      }
   }

   public boolean isEmpty() {
      return this.statics.isEmpty() && this.regulars.isEmpty();
   }

   public Columns columns(boolean isStatic) {
      return isStatic?this.statics:this.regulars;
   }

   public boolean contains(ColumnMetadata column) {
      return column.isStatic()?this.statics.contains(column):this.regulars.contains(column);
   }

   public boolean includes(RegularAndStaticColumns columns) {
      return this.statics.containsAll(columns.statics) && this.regulars.containsAll(columns.regulars);
   }

   public Iterator<ColumnMetadata> iterator() {
      return Iterators.concat(this.statics.iterator(), this.regulars.iterator());
   }

   public Iterator<ColumnMetadata> selectOrderIterator() {
      return Iterators.concat(this.statics.selectOrderIterator(), this.regulars.selectOrderIterator());
   }

   public int size() {
      return this.regulars.size() + this.statics.size();
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[").append(this.statics).append(" | ").append(this.regulars).append("]");
      return sb.toString();
   }

   public boolean equals(Object other) {
      if(!(other instanceof RegularAndStaticColumns)) {
         return false;
      } else {
         RegularAndStaticColumns that = (RegularAndStaticColumns)other;
         return this.statics.equals(that.statics) && this.regulars.equals(that.regulars);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.statics, this.regulars});
   }

   public static RegularAndStaticColumns.Builder builder() {
      return new RegularAndStaticColumns.Builder();
   }

   static {
      NONE = new RegularAndStaticColumns(Columns.NONE, Columns.NONE);
   }

   public static class Builder {
      private BTreeSet.Builder<ColumnMetadata> regularColumns;
      private BTreeSet.Builder<ColumnMetadata> staticColumns;

      public Builder() {
      }

      public RegularAndStaticColumns.Builder add(ColumnMetadata c) {
         if(c.isStatic()) {
            if(this.staticColumns == null) {
               this.staticColumns = BTreeSet.builder(Comparator.naturalOrder());
            }

            this.staticColumns.add(c);
         } else {
            assert c.isRegular();

            if(this.regularColumns == null) {
               this.regularColumns = BTreeSet.builder(Comparator.naturalOrder());
            }

            this.regularColumns.add(c);
         }

         return this;
      }

      public RegularAndStaticColumns.Builder addAll(Iterable<ColumnMetadata> columns) {
         Iterator var2 = columns.iterator();

         while(var2.hasNext()) {
            ColumnMetadata c = (ColumnMetadata)var2.next();
            this.add(c);
         }

         return this;
      }

      public RegularAndStaticColumns.Builder addAll(RegularAndStaticColumns columns) {
         if(this.regularColumns == null && !columns.regulars.isEmpty()) {
            this.regularColumns = BTreeSet.builder(Comparator.naturalOrder());
         }

         Iterator var2 = columns.regulars.iterator();

         ColumnMetadata c;
         while(var2.hasNext()) {
            c = (ColumnMetadata)var2.next();
            this.regularColumns.add(c);
         }

         if(this.staticColumns == null && !columns.statics.isEmpty()) {
            this.staticColumns = BTreeSet.builder(Comparator.naturalOrder());
         }

         var2 = columns.statics.iterator();

         while(var2.hasNext()) {
            c = (ColumnMetadata)var2.next();
            this.staticColumns.add(c);
         }

         return this;
      }

      public RegularAndStaticColumns build() {
         return new RegularAndStaticColumns(this.staticColumns == null?Columns.NONE:Columns.from(this.staticColumns.build()), this.regularColumns == null?Columns.NONE:Columns.from(this.regularColumns.build()));
      }
   }
}
