package org.apache.cassandra.utils.units;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.Comparables;

public class RateUnit implements Comparable<RateUnit> {
   public static final RateUnit B_S;
   public static final RateUnit KB_S;
   public static final RateUnit MB_S;
   public static final RateUnit GB_S;
   public static final RateUnit TB_S;
   final SizeUnit sizeUnit;
   final TimeUnit timeUnit;

   private RateUnit(SizeUnit sizeUnit, TimeUnit timeUnit) {
      this.sizeUnit = sizeUnit;
      this.timeUnit = timeUnit;
   }

   public static RateUnit of(SizeUnit sizeUnit, TimeUnit timeUnit) {
      return new RateUnit(sizeUnit, timeUnit);
   }

   public long convert(long sourceRate, RateUnit sourceUnit) {
      return sourceUnit.sizeUnit.compareTo(this.sizeUnit) < 0?this.sizeUnit.convert(sourceUnit.timeUnit.convert(sourceRate, this.timeUnit), sourceUnit.sizeUnit):sourceUnit.timeUnit.convert(this.sizeUnit.convert(sourceRate, sourceUnit.sizeUnit), this.timeUnit);
   }

   public String toHRString(long value) {
      return Units.toString(value, this);
   }

   public String toString(long value) {
      return Units.formatValue(value) + this;
   }

   static String toString(SizeUnit sizeUnit, TimeUnit timeUnit) {
      return String.format("%s/%s", new Object[]{sizeUnit.symbol, Units.TIME_UNIT_SYMBOL_FCT.apply(timeUnit)});
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.sizeUnit, this.timeUnit});
   }

   public boolean equals(Object other) {
      if(!(other instanceof RateUnit)) {
         return false;
      } else {
         RateUnit that = (RateUnit)other;
         return this.sizeUnit == that.sizeUnit && this.timeUnit == that.timeUnit;
      }
   }

   public String toString() {
      return toString(this.sizeUnit, this.timeUnit);
   }

   RateUnit smallestRepresentableUnit(long value) {
      if(value == 9223372036854775807L) {
         return this;
      } else {
         SizeUnit nextSizeUnit = next(this.sizeUnit);
         TimeUnit nextTimeUnit = next(this.timeUnit);
         long vSize = nextSizeUnit == null?9223372036854775807L:nextSizeUnit.convert(value, this.sizeUnit);
         long vTime = nextTimeUnit == null?9223372036854775807L:this.timeUnit.convert(value, nextTimeUnit);
         RateUnit smallestWithSize = vSize == 9223372036854775807L?this:of(nextSizeUnit, this.timeUnit).smallestRepresentableUnit(vSize);
         RateUnit smallestWithTime = vTime == 9223372036854775807L?this:of(this.sizeUnit, nextTimeUnit).smallestRepresentableUnit(vTime);
         return (RateUnit)Comparables.min(smallestWithSize, smallestWithTime);
      }
   }

   private static SizeUnit next(SizeUnit unit) {
      int ordinal = unit.ordinal();
      return ordinal == 0?null:SizeUnit.values()[ordinal - 1];
   }

   private static TimeUnit next(TimeUnit unit) {
      int ordinal = unit.ordinal();
      return ordinal == TimeUnit.values().length - 1?null:TimeUnit.values()[ordinal + 1];
   }

   public int compareTo(RateUnit that) {
      if(this.sizeUnit == that.sizeUnit) {
         return that.timeUnit.compareTo(this.timeUnit);
      } else if(this.timeUnit == that.timeUnit) {
         return this.sizeUnit.compareTo(that.sizeUnit);
      } else {
         long thisScale;
         long thatScale;
         if(this.sizeUnit.compareTo(that.sizeUnit) < 0) {
            if(this.timeUnit.compareTo(that.timeUnit) < 0) {
               thisScale = valueDiff(this.sizeUnit, that.sizeUnit);
               thatScale = valueDiff(this.timeUnit, that.timeUnit);
               return Long.compare(thatScale, thisScale);
            } else {
               return -1;
            }
         } else if(this.timeUnit.compareTo(that.timeUnit) < 0) {
            return 1;
         } else {
            thisScale = valueDiff(that.sizeUnit, this.sizeUnit);
            thatScale = valueDiff(that.timeUnit, this.timeUnit);
            return Long.compare(thisScale, thatScale);
         }
      }
   }

   private static long valueDiff(SizeUnit min, SizeUnit max) {
      return 1024L * (long)(max.ordinal() - min.ordinal());
   }

   private static long valueDiff(TimeUnit min, TimeUnit max) {
      TimeUnit[] all = TimeUnit.values();
      long val = 1L;

      for(int i = min.ordinal(); i < max.ordinal(); ++i) {
         val *= Units.TIME_UNIT_SCALE_FCT.applyAsLong(all[i]);
      }

      return val;
   }

   static {
      B_S = of(SizeUnit.BYTES, TimeUnit.SECONDS);
      KB_S = of(SizeUnit.KILOBYTES, TimeUnit.SECONDS);
      MB_S = of(SizeUnit.MEGABYTES, TimeUnit.SECONDS);
      GB_S = of(SizeUnit.GIGABYTES, TimeUnit.SECONDS);
      TB_S = of(SizeUnit.GIGABYTES, TimeUnit.SECONDS);
   }
}
