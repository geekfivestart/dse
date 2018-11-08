package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public class EmptyType extends AbstractType<Void> {
   public static final EmptyType instance = new EmptyType();

   private EmptyType() {
      super(AbstractType.ComparisonType.FIXED_COMPARE, 0, AbstractType.PrimitiveType.NONE, 0);
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      return null;
   }

   public String getString(ByteBuffer bytes) {
      return "";
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(!source.isEmpty()) {
         throw new MarshalException(String.format("'%s' is not empty", new Object[]{source}));
      } else {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(!(parsed instanceof String)) {
         throw new MarshalException(String.format("Expected an empty string, but got: %s", new Object[]{parsed}));
      } else if(!((String)parsed).isEmpty()) {
         throw new MarshalException(String.format("'%s' is not empty", new Object[]{parsed}));
      } else {
         return new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.EMPTY;
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return "\"\"";
   }

   public TypeSerializer<Void> getSerializer() {
      return EmptySerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      throw new UnsupportedOperationException();
   }
}
