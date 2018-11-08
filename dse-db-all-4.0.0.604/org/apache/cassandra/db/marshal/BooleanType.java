package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanType extends AbstractType<Boolean> {
   private static final Logger logger = LoggerFactory.getLogger(BooleanType.class);
   public static final BooleanType instance = new BooleanType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   BooleanType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, 1, AbstractType.PrimitiveType.BOOLEAN, 0);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      byte b1 = o1.get(o1.position());
      byte b2 = o2.get(o2.position());
      return b1 == 0?(b2 == 0?0:-1):(b2 == 0?1:0);
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      if(!buf.hasRemaining()) {
         return null;
      } else {
         byte b = buf.get(buf.position());
         if(b != 0) {
            b = 1;
         }

         return ByteSource.oneByte(b);
      }
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(!source.isEmpty() && !source.equalsIgnoreCase(Boolean.FALSE.toString())) {
         if(source.equalsIgnoreCase(Boolean.TRUE.toString())) {
            return this.decompose(Boolean.valueOf(true));
         } else {
            throw new MarshalException(String.format("Unable to make boolean from '%s'", new Object[]{source}));
         }
      } else {
         return this.decompose(Boolean.valueOf(false));
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         return new Constants.Value(this.fromString((String)parsed));
      } else if(!(parsed instanceof Boolean)) {
         throw new MarshalException(String.format("Expected a boolean value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         return new Constants.Value(this.getSerializer().serialize((Boolean)parsed));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Boolean)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.BOOLEAN;
   }

   public TypeSerializer<Boolean> getSerializer() {
      return BooleanSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
