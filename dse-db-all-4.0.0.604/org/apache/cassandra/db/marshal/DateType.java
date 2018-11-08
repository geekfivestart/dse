package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Deprecated
public class DateType extends AbstractType<Date> {
   private static final Logger logger = LoggerFactory.getLogger(DateType.class);
   public static final DateType instance = new DateType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   DateType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, 8);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalFixedLength(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      return source.isEmpty()?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(TimestampSerializer.dateStringToTimestamp(source));
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof Long) {
         return new Constants.Value(ByteBufferUtil.bytes(((Long)parsed).longValue()));
      } else {
         try {
            return new Constants.Value(TimestampType.instance.fromString((String)parsed));
         } catch (ClassCastException var3) {
            throw new MarshalException(String.format("Expected a long or a datestring representation of a date value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
         }
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return '"' + TimestampSerializer.getJsonDateFormatter().format(TimestampSerializer.instance.deserialize(buffer)) + '"';
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(super.isCompatibleWith(previous)) {
         return true;
      } else if(previous instanceof TimestampType) {
         logger.warn("Changing from TimestampType to DateType is allowed, but be wary that they sort differently for pre-unix-epoch timestamps (negative timestamp values) and thus this change will corrupt your data if you have such negative timestamp. There is no reason to switch from DateType to TimestampType except if you were using DateType in the first place and switched to TimestampType by mistake.");
         return true;
      } else {
         return false;
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType || otherType == TimestampType.instance || otherType == LongType.instance;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TIMESTAMP;
   }

   public TypeSerializer<Date> getSerializer() {
      return TimestampSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
