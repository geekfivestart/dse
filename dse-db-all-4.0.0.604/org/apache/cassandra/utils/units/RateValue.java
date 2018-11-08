package org.apache.cassandra.utils.units;

import org.apache.cassandra.utils.Comparables;

public class RateValue implements Comparable<RateValue> {
   public static final RateValue ZERO;
   private final long value;
   private final RateUnit unit;

   private RateValue(long value, RateUnit unit) {
      assert value >= 0L && value != 9223372036854775807L;

      this.value = value;
      this.unit = unit;
   }

   public static RateValue of(long value, RateUnit unit) {
      if(value < 0L) {
         throw new IllegalArgumentException("Invalid negative value for a rate: " + value);
      } else if(value == 9223372036854775807L) {
         throw new IllegalArgumentException("Invalid value for a rate, cannot be Long.MAX_VALUE");
      } else {
         return new RateValue(value, unit);
      }
   }

   public static RateValue compute(SizeValue size, TimeValue duration) {
      SizeUnit bestSizeUnit = size.smallestRepresentableUnit();
      return of(size.in(bestSizeUnit) / duration.value, RateUnit.of(bestSizeUnit, duration.unit));
   }

   public long in(RateUnit destinationUnit) {
      return destinationUnit.convert(this.value, this.unit);
   }

   public RateValue convert(RateUnit destinationUnit) {
      return of(this.in(destinationUnit), destinationUnit);
   }

   public TimeValue timeFor(SizeValue size) {
      RateUnit smallestForRate = this.smallestRepresentableUnit();
      SizeUnit smallestForSize = size.smallestRepresentableUnit();
      SizeUnit toConvert = (SizeUnit)Comparables.max(smallestForSize, smallestForRate.sizeUnit);
      return TimeValue.of(size.in(toConvert) / toConvert.convert(this.value, this.unit.sizeUnit), this.unit.timeUnit);
   }

   private RateUnit smallestRepresentableUnit() {
      return this.unit.smallestRepresentableUnit(this.value);
   }

   public String toRawString() {
      return this.unit.toString(this.value);
   }

   public String toString() {
      return this.unit.toHRString(this.value);
   }

   public int hashCode() {
      return Long.hashCode(this.in(this.smallestRepresentableUnit()));
   }

   public boolean equals(Object other) {
      if(!(other instanceof RateValue)) {
         return false;
      } else {
         RateValue that = (RateValue)other;
         RateUnit smallest = this.smallestRepresentableUnit();
         return smallest.equals(that.smallestRepresentableUnit()) && this.in(smallest) == that.in(smallest);
      }
   }

   public int compareTo(RateValue that) {
      RateUnit thisSmallest = this.smallestRepresentableUnit();
      RateUnit thatSmallest = that.smallestRepresentableUnit();
      return thisSmallest.equals(thatSmallest)?Long.compare(this.in(thisSmallest), that.in(thatSmallest)):(thisSmallest.compareTo(thatSmallest) > 0?1:-1);
   }

   static {
      ZERO = new RateValue(0L, RateUnit.B_S);
   }
}
