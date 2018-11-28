package org.apache.cassandra.utils.units;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class Units {
   static final ToLongFunction<TimeUnit> TIME_UNIT_SCALE_FCT = u -> {
      switch (u) {
         case NANOSECONDS:
         case MICROSECONDS:
         case MILLISECONDS: {
            return 1000L;
         }
         case SECONDS:
         case MINUTES: {
            return 60L;
         }
         case HOURS: {
            return 24L;
         }
         case DAYS: {
            return 365L;
         }
      }
      throw new AssertionError();
   };
   static final Function<TimeUnit, String> TIME_UNIT_SYMBOL_FCT = u -> {
      switch (u) {
         case NANOSECONDS: {
            return "ns";
         }
         case MICROSECONDS: {
            return "us";
         }
         case MILLISECONDS: {
            return "ms";
         }
         case SECONDS: {
            return "s";
         }
         case MINUTES: {
            return "m";
         }
         case HOURS: {
            return "h";
         }
         case DAYS: {
            return "d";
         }
      }
      throw new AssertionError();
   };

   private static final ToLongFunction<SizeUnit> SIZE_UNIT_SCALE_FCT = (u) -> {
      return 1024L;
   };
   private static final Function<SizeUnit, String> SIZE_UNIT_SYMBOL_FCT = (u) -> {
      return u.symbol;
   };

   public Units() {
   }

   public static String toString(long value, TimeUnit unit) {
      return toString(value, unit, TimeUnit.class, TIME_UNIT_SCALE_FCT, TIME_UNIT_SYMBOL_FCT);
   }

   public static String toString(long value, SizeUnit unit) {
      return toString(value, unit, SizeUnit.class, SIZE_UNIT_SCALE_FCT, SIZE_UNIT_SYMBOL_FCT);
   }

   public static String toLogString(long value, SizeUnit unit) {
      return String.format("%s (%s)", new Object[]{SizeUnit.BYTES.toString(unit.toBytes(value)), toString(value, unit)});
   }

   public static String toString(long value, RateUnit unit) {
      value = RateUnit.of(unit.sizeUnit, TimeUnit.SECONDS).convert(value, unit);
      return toString(value, unit.sizeUnit, SizeUnit.class, SIZE_UNIT_SCALE_FCT, (u) -> {
         return RateUnit.toString(u, unit.timeUnit);
      });
   }

   static String formatValue(long value) {
      String v = Long.toString(value);
      int l = v.length();
      int digits = value < 0L?l - 1:l;
      int commaCount = commaCount(digits);
      if(commaCount == 0) {
         return v;
      } else {
         char[] chars = new char[l + commaCount];
         int signShift = value < 0L?1:0;

         int i;
         for(i = 0; i < digits; ++i) {
            chars[signShift + i + (commaCount - commaCount(digits - i))] = v.charAt(signShift + i);
         }

         for(i = 1; i <= commaCount; ++i) {
            chars[chars.length - 4 * i] = 44;
         }

         if(value < 0L) {
            chars[0] = 45;
         }

         return new String(chars);
      }
   }

   private static int commaCount(int digits) {
      return (digits - 1) / 3;
   }

   private static <E extends Enum<E>> String toString(long value, E unit, Class<E> klass, ToLongFunction<E> scaleFct, Function<E, String> symbolFct) {
      E[] enumVals = (E[])klass.getEnumConstants();
      long v = value;
      int i = unit.ordinal();
      long remainder = 0L;

      for(long scale = scaleFct.applyAsLong(unit); i < enumVals.length - 1 && v >= scale; scale = scaleFct.applyAsLong(unit)) {
         remainder = v % scale;
         v /= scale;
         ++i;
         unit = enumVals[i];
      }

      if(v < 10L && remainder != 0L) {
         long prevScale = scaleFct.applyAsLong(enumVals[i - 1]);
         int decimal = Math.round((float)remainder / (float)prevScale * 10.0F);
         return decimal == 0?fmt(v, unit, symbolFct):(decimal == 10?fmt(v + 1L, unit, symbolFct):formatValue(v) + '.' + decimal + (String)symbolFct.apply(unit));
      } else {
         return fmt(v, unit, symbolFct);
      }
   }

   private static <E extends Enum<E>> String fmt(long value, E unit, Function<E, String> symbolFct) {
      return formatValue(value) + (String)symbolFct.apply(unit);
   }
}
