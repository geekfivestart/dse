package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class SimpleDateSerializer implements TypeSerializer<Integer> {
   private static final DateTimeFormatter formatter;
   private static final long minSupportedDateMillis;
   private static final long maxSupportedDateMillis;
   private static final long maxSupportedDays;
   private static final long byteOrderShift;
   private static final Pattern rawPattern;
   public static final SimpleDateSerializer instance;

   public SimpleDateSerializer() {
   }

   public Integer deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Integer.valueOf(ByteBufferUtil.toInt(bytes));
   }

   public ByteBuffer serialize(Integer value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.intValue());
   }

   public static int dateStringToDays(String source) throws MarshalException {
      if(rawPattern.matcher(source).matches()) {
         try {
            long result = Long.parseLong(source);
            if(result >= 0L && result <= maxSupportedDays) {
               if(result >= 2147483647L) {
                  result -= byteOrderShift;
               }

               return (int)result;
            } else {
               throw new NumberFormatException("Input out of bounds: " + source);
            }
         } catch (NumberFormatException var4) {
            throw new MarshalException(String.format("Unable to make unsigned int (for date) from: '%s'", new Object[]{source}), var4);
         }
      } else {
         try {
            DateTime parsed = formatter.parseDateTime(source);
            long millis = parsed.getMillis();
            if(millis < minSupportedDateMillis) {
               throw new MarshalException(String.format("Input date %s is less than min supported date %s", new Object[]{source, (new LocalDate(minSupportedDateMillis)).toString()}));
            } else if(millis > maxSupportedDateMillis) {
               throw new MarshalException(String.format("Input date %s is greater than max supported date %s", new Object[]{source, (new LocalDate(maxSupportedDateMillis)).toString()}));
            } else {
               return timeInMillisToDay(millis);
            }
         } catch (IllegalArgumentException var5) {
            throw new MarshalException(String.format("Unable to coerce '%s' to a formatted date (long)", new Object[]{source}), var5);
         }
      }
   }

   public static int timeInMillisToDay(long millis) {
      Integer result = Integer.valueOf((int)TimeUnit.MILLISECONDS.toDays(millis));
      result = Integer.valueOf(result.intValue() - -2147483648);
      return result.intValue();
   }

   public static long dayToTimeInMillis(int days) {
      return TimeUnit.DAYS.toMillis((long)(days - -2147483648));
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 4) {
         throw new MarshalException(String.format("Expected 4 byte long for date (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Integer value) {
      return value == null?"":formatter.print(new LocalDate(dayToTimeInMillis(value.intValue()), DateTimeZone.UTC));
   }

   public Class<Integer> getType() {
      return Integer.class;
   }

   static {
      formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
      minSupportedDateMillis = TimeUnit.DAYS.toMillis(-2147483648L);
      maxSupportedDateMillis = TimeUnit.DAYS.toMillis(2147483647L);
      maxSupportedDays = (long)Math.pow(2.0D, 32.0D) - 1L;
      byteOrderShift = (long)Math.pow(2.0D, 31.0D) * 2L;
      rawPattern = Pattern.compile("^-?\\d+$");
      instance = new SimpleDateSerializer();
   }
}
