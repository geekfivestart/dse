package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;

public class TimeType extends TemporalType<Long> {
   public static final TimeType instance = new TimeType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   private TimeType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.fixedLength(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      return this.decompose(TimeSerializer.timeStringToLong(source));
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType || otherType == LongType.instance;
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of a time value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return '"' + TimeSerializer.instance.toString(TimeSerializer.instance.deserialize(buffer)) + '"';
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TIME;
   }

   public TypeSerializer<Long> getSerializer() {
      return TimeSerializer.instance;
   }

   public ByteBuffer now() {
      return this.decompose(Long.valueOf(LocalTime.now(ZoneOffset.UTC).toNanoOfDay()));
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
