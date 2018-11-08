package org.apache.cassandra.db.marshal;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class InetAddressType extends AbstractType<InetAddress> {
   public static final InetAddressType instance = new InetAddressType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   InetAddressType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         InetAddress address;
         try {
            address = InetAddress.getByName(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("Unable to make inet address from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(address);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(instance.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of an inet value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return '"' + ((InetAddress)this.getSerializer().deserialize(buffer)).getHostAddress() + '"';
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.INET;
   }

   public TypeSerializer<InetAddress> getSerializer() {
      return InetAddressSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
