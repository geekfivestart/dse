package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;

public class TimeUUIDSerializer extends UUIDSerializer {
   public static final TimeUUIDSerializer instance = new TimeUUIDSerializer();

   public TimeUUIDSerializer() {
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      super.validate(bytes);
      ByteBuffer slice = bytes.slice();
      if(bytes.remaining() > 0) {
         slice.position(6);
         if((slice.get() & 240) != 16) {
            throw new MarshalException("Invalid version for TimeUUID type.");
         }
      }

   }
}
