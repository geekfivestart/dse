package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;

public interface TypeSerializer<T> {
   ByteBuffer serialize(T var1);

   T deserialize(ByteBuffer var1);

   void validate(ByteBuffer var1) throws MarshalException;

   String toString(T var1);

   Class<T> getType();

   default String toCQLLiteral(ByteBuffer buffer) {
      return buffer != null && buffer.hasRemaining()?this.toString(this.deserialize(buffer)):"null";
   }
}
