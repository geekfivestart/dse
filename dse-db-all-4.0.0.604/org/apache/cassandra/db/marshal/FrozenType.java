package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public class FrozenType extends AbstractType<Void> {
   protected FrozenType() {
      super(AbstractType.ComparisonType.NOT_COMPARABLE, -1);
   }

   public static AbstractType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> innerTypes = parser.getTypeParameters();
      if(innerTypes.size() != 1) {
         throw new SyntaxException("FrozenType() only accepts one parameter");
      } else {
         AbstractType<?> innerType = (AbstractType)innerTypes.get(0);
         return innerType.freeze();
      }
   }

   public String getString(ByteBuffer bytes) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      throw new UnsupportedOperationException();
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      throw new UnsupportedOperationException();
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
   }

   public TypeSerializer<Void> getSerializer() {
      throw new UnsupportedOperationException();
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      throw new UnsupportedOperationException();
   }
}
