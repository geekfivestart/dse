package org.apache.cassandra.utils.units;

import java.util.concurrent.TimeUnit;

public class TimeValue implements Comparable<TimeValue> {
   public static final TimeValue ZERO;
   final long value;
   final TimeUnit unit;

   private TimeValue(long value, TimeUnit unit) {
      this.value = value;
      this.unit = unit;
   }

   public static TimeValue of(long value, TimeUnit unit) {
      return new TimeValue(value, unit);
   }

   public long in(TimeUnit destinationUnit) {
      return destinationUnit.convert(this.value, this.unit);
   }

   static TimeUnit smallestRepresentableUnit(long value, TimeUnit unit) {
      long v = value;
      int i = unit.ordinal();

      TimeUnit u;
      TimeUnit current;
      for(u = unit; i > 0 && v < 9223372036854775807L; v = u.convert(v, current)) {
         current = u;
         TimeUnit[] var10000 = TimeUnit.values();
         --i;
         u = var10000[i];
      }

      return u;
   }

   private TimeUnit smallestRepresentableUnit() {
      return smallestRepresentableUnit(this.value, this.unit);
   }

   public String toRawString() {
      return Units.formatValue(this.value) + (String)Units.TIME_UNIT_SYMBOL_FCT.apply(this.unit);
   }

   public String toString() {
      return Units.toString(this.value, this.unit);
   }

   public int hashCode() {
      return Long.hashCode(this.in(this.smallestRepresentableUnit()));
   }

   public boolean equals(Object other) {
      if(!(other instanceof TimeValue)) {
         return false;
      } else {
         TimeValue that = (TimeValue)other;
         TimeUnit smallest = this.smallestRepresentableUnit();
         return smallest == that.smallestRepresentableUnit() && this.in(smallest) == that.in(smallest);
      }
   }

   public int compareTo(TimeValue that) {
      TimeUnit thisSmallest = this.smallestRepresentableUnit();
      TimeUnit thatSmallest = that.smallestRepresentableUnit();
      return thisSmallest == thatSmallest?Long.compare(this.in(thisSmallest), that.in(thatSmallest)):(thisSmallest.compareTo(thatSmallest) > 0?1:-1);
   }

   static {
      ZERO = new TimeValue(0L, TimeUnit.NANOSECONDS);
   }
}
