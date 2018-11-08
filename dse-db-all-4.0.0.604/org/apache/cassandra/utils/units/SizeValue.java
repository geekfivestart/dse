package org.apache.cassandra.utils.units;

public class SizeValue implements Comparable<SizeValue> {
   public static final SizeValue ZERO;
   final long value;
   final SizeUnit unit;

   private SizeValue(long value, SizeUnit unit) {
      assert value >= 0L && value != 9223372036854775807L;

      this.value = value;
      this.unit = unit;
   }

   public static SizeValue of(long value, SizeUnit unit) {
      if(value < 0L) {
         throw new IllegalArgumentException("Invalid negative value for a size in bytes: " + value);
      } else if(value == 9223372036854775807L) {
         throw new IllegalArgumentException("Invalid value for a size in bytes, cannot be Long.MAX_VALUE");
      } else {
         return new SizeValue(value, unit);
      }
   }

   public long in(SizeUnit destinationUnit) {
      return destinationUnit.convert(this.value, this.unit);
   }

   SizeUnit smallestRepresentableUnit() {
      return this.unit.smallestRepresentableUnit(this.value);
   }

   public String toRawString() {
      return this.unit.toString(this.value);
   }

   public String toLogString() {
      return this.unit.toLogString(this.value);
   }

   public String toString() {
      return this.unit.toHRString(this.value);
   }

   public int hashCode() {
      return Long.hashCode(this.in(this.smallestRepresentableUnit()));
   }

   public boolean equals(Object other) {
      if(!(other instanceof SizeValue)) {
         return false;
      } else {
         SizeValue that = (SizeValue)other;
         SizeUnit smallest = this.smallestRepresentableUnit();
         return smallest == that.smallestRepresentableUnit() && this.in(smallest) == that.in(smallest);
      }
   }

   public int compareTo(SizeValue that) {
      SizeUnit thisSmallest = this.smallestRepresentableUnit();
      SizeUnit thatSmallest = that.smallestRepresentableUnit();
      return thisSmallest == thatSmallest?Long.compare(this.in(thisSmallest), that.in(thatSmallest)):(thisSmallest.compareTo(thatSmallest) > 0?1:-1);
   }

   static {
      ZERO = new SizeValue(0L, SizeUnit.BYTES);
   }
}
