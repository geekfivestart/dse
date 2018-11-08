package org.apache.cassandra.cql3;

import java.util.Objects;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

public class PageSize {
   public static PageSize NULL = new PageSize();
   private final int size;
   private final PageSize.PageUnit unit;

   public PageSize(int size, PageSize.PageUnit unit) {
      if(size <= 0) {
         throw new IllegalArgumentException("A page size should be strictly positive, got " + size);
      } else {
         this.size = size;
         this.unit = unit;
      }
   }

   private PageSize() {
      this.size = -1;
      this.unit = PageSize.PageUnit.ROWS;
   }

   public static PageSize rowsSize(int count) {
      return new PageSize(count, PageSize.PageUnit.ROWS);
   }

   public static PageSize bytesSize(int size) {
      return new PageSize(size, PageSize.PageUnit.BYTES);
   }

   public int rawSize() {
      return this.size;
   }

   public boolean isInRows() {
      return this.unit == PageSize.PageUnit.ROWS;
   }

   public boolean isInBytes() {
      return this.unit == PageSize.PageUnit.BYTES;
   }

   public int inRows() {
      RequestValidations.checkTrue(this.unit == PageSize.PageUnit.ROWS, "A page size expressed in bytes is unsupported");
      return this.size;
   }

   public int inEstimatedRows(int avgRowSize) {
      if(this.unit == PageSize.PageUnit.ROWS) {
         return this.size;
      } else {
         if(avgRowSize == 0) {
            avgRowSize = 1;
         }

         return Math.max(1, this.size / avgRowSize);
      }
   }

   public int inEstimatedBytes(int avgRowSize) {
      if(this.unit == PageSize.PageUnit.BYTES) {
         return this.size;
      } else {
         if(avgRowSize == 0) {
            avgRowSize = 1;
         }

         return this.size * avgRowSize;
      }
   }

   public boolean isComplete(int rowCount, int sizeInBytes) {
      return this.isInRows()?rowCount >= this.size:sizeInBytes >= this.size;
   }

   public boolean isLarger(int rowCount) {
      return this == NULL || this.isInRows() && rowCount <= this.rawSize();
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.size), this.unit});
   }

   public final boolean equals(Object o) {
      if(!(o instanceof PageSize)) {
         return false;
      } else {
         PageSize that = (PageSize)o;
         return this.size == that.size && this.unit == that.unit;
      }
   }

   public String toString() {
      return this.unit.toString((long)this.size);
   }

   public static enum PageUnit {
      BYTES {
         String toString(long value) {
            return Units.toString(value, SizeUnit.BYTES);
         }
      },
      ROWS {
         String toString(long value) {
            return String.format("%d rows", new Object[]{Long.valueOf(value)});
         }
      };

      private PageUnit() {
      }

      abstract String toString(long var1);
   }
}
