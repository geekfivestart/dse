package org.apache.cassandra.serializers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class InetAddressSerializer implements TypeSerializer<InetAddress> {
   public static final InetAddressSerializer instance = new InetAddressSerializer();

   public InetAddressSerializer() {
   }

   public InetAddress deserialize(ByteBuffer bytes) {
      if(bytes.remaining() == 0) {
         return null;
      } else {
         try {
            return InetAddress.getByAddress(ByteBufferUtil.getArray(bytes));
         } catch (UnknownHostException var3) {
            throw new AssertionError(var3);
         }
      }
   }

   public ByteBuffer serialize(InetAddress value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBuffer.wrap(value.getAddress());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 0) {
         try {
            InetAddress.getByAddress(ByteBufferUtil.getArray(bytes));
         } catch (UnknownHostException var3) {
            throw new MarshalException(String.format("Expected 4 or 16 byte inetaddress; got %s", new Object[]{ByteBufferUtil.bytesToHex(bytes)}));
         }
      }
   }

   public String toString(InetAddress value) {
      return value == null?"":value.getHostAddress();
   }

   public Class<InetAddress> getType() {
      return InetAddress.class;
   }
}
