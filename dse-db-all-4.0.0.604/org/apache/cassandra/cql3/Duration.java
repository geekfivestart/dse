package org.apache.cassandra.cql3;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.serializers.MarshalException;

public final class Duration {
   public static final long NANOS_PER_MICRO = 1000L;
   public static final long NANOS_PER_MILLI = 1000000L;
   public static final long NANOS_PER_SECOND = 1000000000L;
   public static final long NANOS_PER_MINUTE = 60000000000L;
   public static final long NANOS_PER_HOUR = 3600000000000L;
   public static final int DAYS_PER_WEEK = 7;
   public static final int MONTHS_PER_YEAR = 12;
   private static final Pattern STANDARD_PATTERN = Pattern.compile("\\G(\\d+)(y|Y|mo|MO|mO|Mo|w|W|d|D|h|H|s|S|ms|MS|mS|Ms|us|US|uS|Us|µs|µS|ns|NS|nS|Ns|m|M)");
   private static final Pattern ISO8601_PATTERN = Pattern.compile("P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)S)?)?");
   private static final Pattern ISO8601_WEEK_PATTERN = Pattern.compile("P(\\d+)W");
   private static final Pattern ISO8601_ALTERNATIVE_PATTERN = Pattern.compile("P(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})");
   private final int months;
   private final int days;
   private final long nanoseconds;
   private static final FastThreadLocal<Calendar> CALENDAR_PROVIDER = new FastThreadLocal<Calendar>() {
      public Calendar initialValue() {
         return Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US);
      }
   };

   private Duration(int months, int days, long nanoseconds) {
      assert months >= 0 && days >= 0 && nanoseconds >= 0L || months <= 0 && days <= 0 && nanoseconds <= 0L;

      this.months = months;
      this.days = days;
      this.nanoseconds = nanoseconds;
   }

   public static Duration newInstance(int months, int days, long nanoseconds) {
      return new Duration(months, days, nanoseconds);
   }

   public static Duration from(String input) {
      boolean isNegative = input.startsWith("-");
      String source = isNegative?input.substring(1):input;
      return source.startsWith("P")?(source.endsWith("W")?parseIso8601WeekFormat(isNegative, source):(source.contains("-")?parseIso8601AlternativeFormat(isNegative, source):parseIso8601Format(isNegative, source))):parseStandardFormat(isNegative, source);
   }

   private static Duration parseIso8601Format(boolean isNegative, String source) {
      Matcher matcher = ISO8601_PATTERN.matcher(source);
      if(!matcher.matches()) {
         throw RequestValidations.invalidRequest("Unable to convert '%s' to a duration", new Object[]{source});
      } else {
         Duration.Builder builder = new Duration.Builder(isNegative);
         if(matcher.group(1) != null) {
            builder.addYears(groupAsLong(matcher, 2));
         }

         if(matcher.group(3) != null) {
            builder.addMonths(groupAsLong(matcher, 4));
         }

         if(matcher.group(5) != null) {
            builder.addDays(groupAsLong(matcher, 6));
         }

         if(matcher.group(7) != null) {
            if(matcher.group(8) != null) {
               builder.addHours(groupAsLong(matcher, 9));
            }

            if(matcher.group(10) != null) {
               builder.addMinutes(groupAsLong(matcher, 11));
            }

            if(matcher.group(12) != null) {
               builder.addSeconds(groupAsLong(matcher, 13));
            }
         }

         return builder.build();
      }
   }

   private static Duration parseIso8601AlternativeFormat(boolean isNegative, String source) {
      Matcher matcher = ISO8601_ALTERNATIVE_PATTERN.matcher(source);
      if(!matcher.matches()) {
         throw RequestValidations.invalidRequest("Unable to convert '%s' to a duration", new Object[]{source});
      } else {
         return (new Duration.Builder(isNegative)).addYears(groupAsLong(matcher, 1)).addMonths(groupAsLong(matcher, 2)).addDays(groupAsLong(matcher, 3)).addHours(groupAsLong(matcher, 4)).addMinutes(groupAsLong(matcher, 5)).addSeconds(groupAsLong(matcher, 6)).build();
      }
   }

   private static Duration parseIso8601WeekFormat(boolean isNegative, String source) {
      Matcher matcher = ISO8601_WEEK_PATTERN.matcher(source);
      if(!matcher.matches()) {
         throw RequestValidations.invalidRequest("Unable to convert '%s' to a duration", new Object[]{source});
      } else {
         return (new Duration.Builder(isNegative)).addWeeks(groupAsLong(matcher, 1)).build();
      }
   }

   private static Duration parseStandardFormat(boolean isNegative, String source) {
      Matcher matcher = STANDARD_PATTERN.matcher(source);
      if(!matcher.find()) {
         throw RequestValidations.invalidRequest("Unable to convert '%s' to a duration", new Object[]{source});
      } else {
         Duration.Builder builder = new Duration.Builder(isNegative);
         boolean done = false;

         do {
            long number = groupAsLong(matcher, 1);
            String symbol = matcher.group(2);
            add(builder, number, symbol);
            done = matcher.end() == source.length();
         } while(matcher.find());

         if(!done) {
            throw RequestValidations.invalidRequest("Unable to convert '%s' to a duration", new Object[]{source});
         } else {
            return builder.build();
         }
      }
   }

   private static long groupAsLong(Matcher matcher, int group) {
      return Long.parseLong(matcher.group(group));
   }

   private static Duration.Builder add(Duration.Builder builder, long number, String symbol) {
      String var4 = symbol.toLowerCase();
      byte var5 = -1;
      switch(var4.hashCode()) {
      case 100:
         if(var4.equals("d")) {
            var5 = 3;
         }
         break;
      case 104:
         if(var4.equals("h")) {
            var5 = 4;
         }
         break;
      case 109:
         if(var4.equals("m")) {
            var5 = 5;
         }
         break;
      case 115:
         if(var4.equals("s")) {
            var5 = 6;
         }
         break;
      case 119:
         if(var4.equals("w")) {
            var5 = 2;
         }
         break;
      case 121:
         if(var4.equals("y")) {
            var5 = 0;
         }
         break;
      case 3490:
         if(var4.equals("mo")) {
            var5 = 1;
         }
         break;
      case 3494:
         if(var4.equals("ms")) {
            var5 = 7;
         }
         break;
      case 3525:
         if(var4.equals("ns")) {
            var5 = 10;
         }
         break;
      case 3742:
         if(var4.equals("us")) {
            var5 = 8;
         }
         break;
      case 5726:
         if(var4.equals("µs")) {
            var5 = 9;
         }
      }

      switch(var5) {
      case 0:
         return builder.addYears(number);
      case 1:
         return builder.addMonths(number);
      case 2:
         return builder.addWeeks(number);
      case 3:
         return builder.addDays(number);
      case 4:
         return builder.addHours(number);
      case 5:
         return builder.addMinutes(number);
      case 6:
         return builder.addSeconds(number);
      case 7:
         return builder.addMillis(number);
      case 8:
      case 9:
         return builder.addMicros(number);
      case 10:
         return builder.addNanos(number);
      default:
         throw new MarshalException(String.format("Unknown duration symbol '%s'", new Object[]{symbol}));
      }
   }

   public int getMonths() {
      return this.months;
   }

   public int getDays() {
      return this.days;
   }

   public long getNanoseconds() {
      return this.nanoseconds;
   }

   public long addTo(long timeInMillis) {
      return add(timeInMillis, this.months, this.days, this.nanoseconds);
   }

   public long substractFrom(long timeInMillis) {
      return add(timeInMillis, -this.months, -this.days, -this.nanoseconds);
   }

   private static long add(long timeInMillis, int months, int days, long nanoseconds) {
      if(months == 0) {
         long durationInMillis = (long)days * 86400000L + nanoseconds / 1000000L;
         return timeInMillis + durationInMillis;
      } else {
         Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US);
         calendar.setTimeInMillis(timeInMillis);
         calendar.add(2, months);
         calendar.add(5, days);
         calendar.add(14, (int)(nanoseconds / 1000000L));
         return calendar.getTimeInMillis();
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.days), Integer.valueOf(this.months), Long.valueOf(this.nanoseconds)});
   }

   public boolean equals(Object obj) {
      if(!(obj instanceof Duration)) {
         return false;
      } else {
         Duration other = (Duration)obj;
         return this.days == other.days && this.months == other.months && this.nanoseconds == other.nanoseconds;
      }
   }

   public static long floorTimestamp(long timeInMillis, Duration duration, long startingTimeInMillis) {
      RequestValidations.checkFalse(startingTimeInMillis > timeInMillis, "The floor function starting time is greater than the provided time");
      RequestValidations.checkFalse(duration.isNegative(), "Negative durations are not supported by the floor function");
      if(duration.months == 0) {
         long durationInMillis = (long)duration.days * 86400000L + duration.nanoseconds / 1000000L;
         if(durationInMillis == 0L) {
            return timeInMillis;
         } else {
            long delta = (timeInMillis - startingTimeInMillis) % durationInMillis;
            return timeInMillis - delta;
         }
      } else {
         Calendar calendar = (Calendar)CALENDAR_PROVIDER.get();
         calendar.setTimeInMillis(timeInMillis);
         int year = calendar.get(1);
         int month = calendar.get(2);
         calendar.setTimeInMillis(startingTimeInMillis);
         int startingYear = calendar.get(1);
         int startingMonth = calendar.get(2);
         int durationInMonths = (year - startingYear) * 12 + (month - startingMonth);
         int multiplier = durationInMonths / duration.months;
         calendar.add(2, multiplier * duration.months);
         if(duration.days == 0 && duration.nanoseconds == 0L) {
            return calendar.getTimeInMillis();
         } else {
            long durationInMillis = (long)duration.days * 86400000L + duration.nanoseconds / 1000000L;

            long floor;
            for(floor = calendar.getTimeInMillis() + (long)multiplier * durationInMillis; floor > timeInMillis; floor = calendar.getTimeInMillis() + (long)multiplier * durationInMillis) {
               --multiplier;
               calendar.add(2, -duration.months);
            }

            return floor < startingTimeInMillis?startingTimeInMillis:floor;
         }
      }
   }

   public static long floorTime(long timeInNanos, Duration duration) {
      RequestValidations.checkFalse(duration.isNegative(), "Negative durations are not supported by the floor function");
      RequestValidations.checkFalse(duration.getMonths() != 0 || duration.getDays() != 0 || duration.getNanoseconds() > 86400000000000L, "For time values, the floor can only be computed for durations smaller that a day");
      if(duration.nanoseconds == 0L) {
         return timeInNanos;
      } else {
         long delta = timeInNanos % duration.nanoseconds;
         return timeInNanos - delta;
      }
   }

   public boolean isNegative() {
      return this.nanoseconds < 0L || this.days < 0 || this.months < 0;
   }

   public String toString() {
      StringBuilder builder = new StringBuilder();
      if(this.isNegative()) {
         builder.append('-');
      }

      long remainder = append(builder, (long)Math.abs(this.months), 12L, "y");
      append(builder, remainder, 1L, "mo");
      append(builder, (long)Math.abs(this.days), 1L, "d");
      if(this.nanoseconds != 0L) {
         remainder = append(builder, Math.abs(this.nanoseconds), 3600000000000L, "h");
         remainder = append(builder, remainder, 60000000000L, "m");
         remainder = append(builder, remainder, 1000000000L, "s");
         remainder = append(builder, remainder, 1000000L, "ms");
         remainder = append(builder, remainder, 1000L, "us");
         append(builder, remainder, 1L, "ns");
      }

      return builder.toString();
   }

   public boolean hasDayPrecision() {
      return this.getNanoseconds() == 0L;
   }

   public boolean hasMillisecondPrecision() {
      return Long.numberOfTrailingZeros(this.getNanoseconds()) >= 6;
   }

   private static long append(StringBuilder builder, long dividend, long divisor, String unit) {
      if(dividend != 0L && dividend >= divisor) {
         builder.append(dividend / divisor).append(unit);
         return dividend % divisor;
      } else {
         return dividend;
      }
   }

   private static class Builder {
      private final boolean isNegative;
      private int months;
      private int days;
      private long nanoseconds;
      private int currentUnitIndex;

      public Builder(boolean isNegative) {
         this.isNegative = isNegative;
      }

      public Duration.Builder addYears(long numberOfYears) {
         this.validateOrder(1);
         this.validateMonths(numberOfYears, 12);
         this.months = (int)((long)this.months + numberOfYears * 12L);
         return this;
      }

      public Duration.Builder addMonths(long numberOfMonths) {
         this.validateOrder(2);
         this.validateMonths(numberOfMonths, 1);
         this.months = (int)((long)this.months + numberOfMonths);
         return this;
      }

      public Duration.Builder addWeeks(long numberOfWeeks) {
         this.validateOrder(3);
         this.validateDays(numberOfWeeks, 7);
         this.days = (int)((long)this.days + numberOfWeeks * 7L);
         return this;
      }

      public Duration.Builder addDays(long numberOfDays) {
         this.validateOrder(4);
         this.validateDays(numberOfDays, 1);
         this.days = (int)((long)this.days + numberOfDays);
         return this;
      }

      public Duration.Builder addHours(long numberOfHours) {
         this.validateOrder(5);
         this.validateNanos(numberOfHours, 3600000000000L);
         this.nanoseconds += numberOfHours * 3600000000000L;
         return this;
      }

      public Duration.Builder addMinutes(long numberOfMinutes) {
         this.validateOrder(6);
         this.validateNanos(numberOfMinutes, 60000000000L);
         this.nanoseconds += numberOfMinutes * 60000000000L;
         return this;
      }

      public Duration.Builder addSeconds(long numberOfSeconds) {
         this.validateOrder(7);
         this.validateNanos(numberOfSeconds, 1000000000L);
         this.nanoseconds += numberOfSeconds * 1000000000L;
         return this;
      }

      public Duration.Builder addMillis(long numberOfMillis) {
         this.validateOrder(8);
         this.validateNanos(numberOfMillis, 1000000L);
         this.nanoseconds += numberOfMillis * 1000000L;
         return this;
      }

      public Duration.Builder addMicros(long numberOfMicros) {
         this.validateOrder(9);
         this.validateNanos(numberOfMicros, 1000L);
         this.nanoseconds += numberOfMicros * 1000L;
         return this;
      }

      public Duration.Builder addNanos(long numberOfNanos) {
         this.validateOrder(10);
         this.validateNanos(numberOfNanos, 1L);
         this.nanoseconds += numberOfNanos;
         return this;
      }

      private void validateMonths(long units, int monthsPerUnit) {
         this.validate(units, (long)((2147483647 - this.months) / monthsPerUnit), "months");
      }

      private void validateDays(long units, int daysPerUnit) {
         this.validate(units, (long)((2147483647 - this.days) / daysPerUnit), "days");
      }

      private void validateNanos(long units, long nanosPerUnit) {
         this.validate(units, (9223372036854775807L - this.nanoseconds) / nanosPerUnit, "nanoseconds");
      }

      private void validate(long units, long limit, String unitName) {
         RequestValidations.checkTrue(units <= limit, "Invalid duration. The total number of %s must be less or equal to %s", unitName, Integer.valueOf(2147483647));
      }

      private void validateOrder(int unitIndex) {
         if(unitIndex == this.currentUnitIndex) {
            throw RequestValidations.invalidRequest("Invalid duration. The %s are specified multiple times", new Object[]{this.getUnitName(unitIndex)});
         } else if(unitIndex <= this.currentUnitIndex) {
            throw RequestValidations.invalidRequest("Invalid duration. The %s should be after %s", new Object[]{this.getUnitName(this.currentUnitIndex), this.getUnitName(unitIndex)});
         } else {
            this.currentUnitIndex = unitIndex;
         }
      }

      private String getUnitName(int unitIndex) {
         switch(unitIndex) {
         case 1:
            return "years";
         case 2:
            return "months";
         case 3:
            return "weeks";
         case 4:
            return "days";
         case 5:
            return "hours";
         case 6:
            return "minutes";
         case 7:
            return "seconds";
         case 8:
            return "milliseconds";
         case 9:
            return "microseconds";
         case 10:
            return "nanoseconds";
         default:
            throw new AssertionError("unknown unit index: " + unitIndex);
         }
      }

      public Duration build() {
         return this.isNegative?new Duration(-this.months, -this.days, -this.nanoseconds):new Duration(this.months, this.days, this.nanoseconds);
      }
   }
}
