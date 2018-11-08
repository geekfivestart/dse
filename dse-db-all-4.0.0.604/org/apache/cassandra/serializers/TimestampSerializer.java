package org.apache.cassandra.serializers;

import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TimestampSerializer implements TypeSerializer<Date> {
   private static final String[] dateStringPatterns = new String[]{"yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm z", "yyyy-MM-dd HH:mm zz", "yyyy-MM-dd HH:mm zzz", "yyyy-MM-dd HH:mmX", "yyyy-MM-dd HH:mmXX", "yyyy-MM-dd HH:mmXXX", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss z", "yyyy-MM-dd HH:mm:ss zz", "yyyy-MM-dd HH:mm:ss zzz", "yyyy-MM-dd HH:mm:ssX", "yyyy-MM-dd HH:mm:ssXX", "yyyy-MM-dd HH:mm:ssXXX", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS z", "yyyy-MM-dd HH:mm:ss.SSS zz", "yyyy-MM-dd HH:mm:ss.SSS zzz", "yyyy-MM-dd HH:mm:ss.SSSX", "yyyy-MM-dd HH:mm:ss.SSSXX", "yyyy-MM-dd HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd'T'HH:mm z", "yyyy-MM-dd'T'HH:mm zz", "yyyy-MM-dd'T'HH:mm zzz", "yyyy-MM-dd'T'HH:mmX", "yyyy-MM-dd'T'HH:mmXX", "yyyy-MM-dd'T'HH:mmXXX", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss z", "yyyy-MM-dd'T'HH:mm:ss zz", "yyyy-MM-dd'T'HH:mm:ss zzz", "yyyy-MM-dd'T'HH:mm:ssX", "yyyy-MM-dd'T'HH:mm:ssXX", "yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS z", "yyyy-MM-dd'T'HH:mm:ss.SSS zz", "yyyy-MM-dd'T'HH:mm:ss.SSS zzz", "yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ss.SSSXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd", "yyyy-MM-dd z", "yyyy-MM-dd zz", "yyyy-MM-dd zzz", "yyyy-MM-ddX", "yyyy-MM-ddXX", "yyyy-MM-ddXXX"};
   private static final String DEFAULT_FORMAT;
   private static final Pattern timestampPattern;
   private static final FastThreadLocal<SimpleDateFormat> FORMATTER;
   private static final String UTC_FORMAT;
   private static final FastThreadLocal<SimpleDateFormat> FORMATTER_UTC;
   private static final String TO_JSON_FORMAT;
   private static final FastThreadLocal<SimpleDateFormat> FORMATTER_TO_JSON;
   public static final TimestampSerializer instance;

   public TimestampSerializer() {
   }

   public Date deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:new Date(ByteBufferUtil.toLong(bytes));
   }

   public ByteBuffer serialize(Date value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.getTime());
   }

   public static long dateStringToTimestamp(String source) throws MarshalException {
      if(source.equalsIgnoreCase("now")) {
         return System.currentTimeMillis();
      } else if(timestampPattern.matcher(source).matches()) {
         try {
            return Long.parseLong(source);
         } catch (NumberFormatException var2) {
            throw new MarshalException(String.format("Unable to make long (for date) from: '%s'", new Object[]{source}), var2);
         }
      } else {
         Date date = parseDate(addMissingZeroToMillisIfNeeded(source));
         if(date == null) {
            throw new MarshalException(String.format("Unable to coerce '%s' to a formatted date (long)", new Object[]{source}));
         } else {
            return date.getTime();
         }
      }
   }

   private static Date parseDate(String source) {
      SimpleDateFormat parser = new SimpleDateFormat();
      parser.setLenient(false);
      ParsePosition pos = new ParsePosition(0);
      String[] var3 = dateStringPatterns;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         String parsePattern = var3[var5];
         parser.applyPattern(parsePattern);
         pos.setIndex(0);
         Date date = parser.parse(source, pos);
         if(date != null && pos.getIndex() == source.length()) {
            return date;
         }
      }

      return null;
   }

   private static String addMissingZeroToMillisIfNeeded(String source) {
      int index = source.indexOf(46);
      if(index == -1) {
         return source;
      } else {
         ++index;

         int numberOfDigits;
         for(numberOfDigits = 0; index < source.length() && Character.isDigit(source.charAt(index)); ++index) {
            ++numberOfDigits;
         }

         if(numberOfDigits >= 3) {
            return source;
         } else {
            int zerosToAdd = 3 - numberOfDigits;
            StringBuilder builder = (new StringBuilder(source.length() + zerosToAdd)).append(source);

            for(int i = 0; i < zerosToAdd; ++i) {
               builder.insert(index, 0);
            }

            return builder.toString();
         }
      }
   }

   public static SimpleDateFormat getJsonDateFormatter() {
      return (SimpleDateFormat)FORMATTER_TO_JSON.get();
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 8 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 8 or 0 byte long for date (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Date value) {
      return value == null?"":((SimpleDateFormat)FORMATTER.get()).format(value);
   }

   public String toStringUTC(Date value) {
      return value == null?"":((SimpleDateFormat)FORMATTER_UTC.get()).format(value);
   }

   public Class<Date> getType() {
      return Date.class;
   }

   public String toCQLLiteral(ByteBuffer buffer) {
      return buffer != null && buffer.hasRemaining()?((SimpleDateFormat)FORMATTER_UTC.get()).format(this.deserialize(buffer)):"null";
   }

   static {
      DEFAULT_FORMAT = dateStringPatterns[6];
      timestampPattern = Pattern.compile("^-?\\d+$");
      FORMATTER = new FastThreadLocal<SimpleDateFormat>() {
         protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(TimestampSerializer.DEFAULT_FORMAT);
         }
      };
      UTC_FORMAT = dateStringPatterns[40];
      FORMATTER_UTC = new FastThreadLocal<SimpleDateFormat>() {
         protected SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat(TimestampSerializer.UTC_FORMAT);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
         }
      };
      TO_JSON_FORMAT = dateStringPatterns[19];
      FORMATTER_TO_JSON = new FastThreadLocal<SimpleDateFormat>() {
         protected SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat(TimestampSerializer.TO_JSON_FORMAT);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
         }
      };
      instance = new TimestampSerializer();
   }
}
