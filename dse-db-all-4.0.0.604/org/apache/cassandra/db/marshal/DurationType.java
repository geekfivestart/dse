package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.DurationSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DurationType extends AbstractType<Duration> {
   public static final DurationType instance = new DurationType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   DurationType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      return source.isEmpty()?ByteBufferUtil.EMPTY_BYTE_BUFFER:this.decompose(Duration.from(source));
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType;
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of a duration, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Duration)this.getSerializer().deserialize(buffer)).toString();
   }

   public TypeSerializer<Duration> getSerializer() {
      return DurationSerializer.instance;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.DURATION;
   }

   public boolean referencesDuration() {
      return true;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
