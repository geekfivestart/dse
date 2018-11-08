package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampType extends TemporalType<Date> {
   private static final Logger logger = LoggerFactory.getLogger(TimestampType.class);
   public static final TimestampType instance = new TimestampType();

   private TimestampType() {
      super(AbstractType.ComparisonType.CUSTOM, 8);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return LongType.compareLongs(o1, o2);
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalSignedFixedLengthNumber(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      return source.isEmpty()?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(TimestampSerializer.dateStringToTimestamp(source));
   }

   public ByteBuffer fromTimeInMillis(long millis) throws MarshalException {
      return ByteBufferUtil.bytes(millis);
   }

   public long toTimeInMillis(ByteBuffer value) {
      return ByteBufferUtil.toLong(value);
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(!(parsed instanceof Long) && !(parsed instanceof Integer)) {
         try {
            return new Constants.Value(instance.fromString((String)parsed));
         } catch (ClassCastException var3) {
            throw new MarshalException(String.format("Expected a long or a datestring representation of a timestamp value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
         }
      } else {
         return new Constants.Value(ByteBufferUtil.bytes(((Number)parsed).longValue()));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return '"' + TimestampSerializer.getJsonDateFormatter().format(TimestampSerializer.instance.deserialize(buffer)) + '"';
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(super.isCompatibleWith(previous)) {
         return true;
      } else if(previous instanceof DateType) {
         logger.warn("Changing from DateType to TimestampType is allowed, but be wary that they sort differently for pre-unix-epoch timestamps (negative timestamp values) and thus this change will corrupt your data if you have such negative timestamp. So unless you know that you don't have *any* pre-unix-epoch timestamp you should change back to DateType");
         return true;
      } else {
         return false;
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType || otherType == DateType.instance || otherType == LongType.instance;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TIMESTAMP;
   }

   public TypeSerializer<Date> getSerializer() {
      return TimestampSerializer.instance;
   }

   protected void validateDuration(Duration duration) {
      if(!duration.hasMillisecondPrecision()) {
         throw RequestValidations.invalidRequest("The duration must have a millisecond precision. Was: %s", new Object[]{duration});
      }
   }
}
