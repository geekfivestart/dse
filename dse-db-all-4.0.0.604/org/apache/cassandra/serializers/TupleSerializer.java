package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TupleSerializer extends BytesSerializer {
   public final List<TypeSerializer<?>> fields;

   public TupleSerializer(List<TypeSerializer<?>> fields) {
      this.fields = fields;
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      ByteBuffer input = bytes.duplicate();

      for(int i = 0; i < this.fields.size(); ++i) {
         if(!input.hasRemaining()) {
            return;
         }

         if(input.remaining() < 4) {
            throw new MarshalException(String.format("Not enough bytes to read size of %dth component", new Object[]{Integer.valueOf(i)}));
         }

         int size = input.getInt();
         if(size >= 0) {
            if(input.remaining() < size) {
               throw new MarshalException(String.format("Not enough bytes to read %dth component", new Object[]{Integer.valueOf(i)}));
            }

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            ((TypeSerializer)this.fields.get(i)).validate(field);
         }
      }

      if(input.hasRemaining()) {
         throw new MarshalException("Invalid remaining data after end of tuple value");
      }
   }
}
