package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TimeSerializer implements TypeSerializer<Long> {
   public static final Pattern timePattern = Pattern.compile("^-?\\d+$");
   public static final TimeSerializer instance = new TimeSerializer();

   public TimeSerializer() {
   }

   public Long deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Long.valueOf(ByteBufferUtil.toLong(bytes));
   }

   public ByteBuffer serialize(Long value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.longValue());
   }

   public static Long timeStringToLong(String source) throws MarshalException {
      if(timePattern.matcher(source).matches()) {
         try {
            long result = Long.parseLong(source);
            if(result >= 0L && result < TimeUnit.DAYS.toNanos(1L)) {
               return Long.valueOf(result);
            } else {
               throw new NumberFormatException("Input long out of bounds: " + source);
            }
         } catch (NumberFormatException var3) {
            throw new MarshalException(String.format("Unable to make long (for time) from: '%s'", new Object[]{source}), var3);
         }
      } else {
         try {
            return parseTimeStrictly(source);
         } catch (IllegalArgumentException var4) {
            throw new MarshalException(String.format("(TimeType) Unable to coerce '%s' to a formatted time (long)", new Object[]{source}), var4);
         }
      }
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 8) {
         throw new MarshalException(String.format("Expected 8 byte long for time (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Long value) {
      if(value == null) {
         return "null";
      } else {
         int nano = (int)(value.longValue() % 1000L);
         value = Long.valueOf(value.longValue() - (long)nano);
         value = Long.valueOf(value.longValue() / 1000L);
         int micro = (int)(value.longValue() % 1000L);
         value = Long.valueOf(value.longValue() - (long)micro);
         value = Long.valueOf(value.longValue() / 1000L);
         int milli = (int)(value.longValue() % 1000L);
         value = Long.valueOf(value.longValue() - (long)milli);
         value = Long.valueOf(value.longValue() / 1000L);
         int seconds = (int)(value.longValue() % 60L);
         value = Long.valueOf(value.longValue() - (long)seconds);
         value = Long.valueOf(value.longValue() / 60L);
         int minutes = (int)(value.longValue() % 60L);
         value = Long.valueOf(value.longValue() - (long)minutes);
         value = Long.valueOf(value.longValue() / 60L);
         int hours = (int)(value.longValue() % 24L);
         value = Long.valueOf(value.longValue() - (long)hours);
         value = Long.valueOf(value.longValue() / 24L);

         assert value.longValue() == 0L;

         StringBuilder sb = new StringBuilder();
         this.leftPadZeros(hours, 2, sb);
         sb.append(":");
         this.leftPadZeros(minutes, 2, sb);
         sb.append(":");
         this.leftPadZeros(seconds, 2, sb);
         sb.append(".");
         this.leftPadZeros(milli, 3, sb);
         this.leftPadZeros(micro, 3, sb);
         this.leftPadZeros(nano, 3, sb);
         return sb.toString();
      }
   }

   private void leftPadZeros(int value, int digits, StringBuilder sb) {
      for(int i = 1; i < digits; ++i) {
         if((double)value < Math.pow(10.0D, (double)i)) {
            sb.append("0");
         }
      }

      sb.append(value);
   }

   public Class<Long> getType() {
      return Long.class;
   }

   private static Long parseTimeStrictly(String s) throws IllegalArgumentException {
      long a_nanos = 0L;
      String formatError = "Timestamp format must be hh:mm:ss[.fffffffff]";
      String zeros = "000000000";
      if(s == null) {
         throw new IllegalArgumentException(formatError);
      } else {
         s = s.trim();
         int firstColon = s.indexOf(58);
         int secondColon = s.indexOf(58, firstColon + 1);
         if(firstColon > 0 && secondColon > 0 && secondColon < s.length() - 1) {
            int period = s.indexOf(46, secondColon + 1);
            long hour = (long)Integer.parseInt(s.substring(0, firstColon));
            if(hour >= 0L && hour < 24L) {
               long minute = (long)Integer.parseInt(s.substring(firstColon + 1, secondColon));
               if(minute >= 0L && minute < 60L) {
                  long second;
                  if(period > 0 && period < s.length() - 1) {
                     second = (long)Integer.parseInt(s.substring(secondColon + 1, period));
                     if(second < 0L || second >= 60L) {
                        throw new IllegalArgumentException("Second out of bounds.");
                     }

                     String nanos_s = s.substring(period + 1);
                     if(nanos_s.length() > 9) {
                        throw new IllegalArgumentException(formatError);
                     }

                     if(!Character.isDigit(nanos_s.charAt(0))) {
                        throw new IllegalArgumentException(formatError);
                     }

                     nanos_s = nanos_s + zeros.substring(0, 9 - nanos_s.length());
                     a_nanos = (long)Integer.parseInt(nanos_s);
                  } else {
                     if(period > 0) {
                        throw new IllegalArgumentException(formatError);
                     }

                     second = (long)Integer.parseInt(s.substring(secondColon + 1));
                     if(second < 0L || second >= 60L) {
                        throw new IllegalArgumentException("Second out of bounds.");
                     }
                  }

                  long rawTime = 0L;
                  rawTime += TimeUnit.HOURS.toNanos(hour);
                  rawTime += TimeUnit.MINUTES.toNanos(minute);
                  rawTime += TimeUnit.SECONDS.toNanos(second);
                  rawTime += a_nanos;
                  return Long.valueOf(rawTime);
               } else {
                  throw new IllegalArgumentException("Minute out of bounds.");
               }
            } else {
               throw new IllegalArgumentException("Hour out of bounds.");
            }
         } else {
            throw new IllegalArgumentException(formatError);
         }
      }
   }
}
