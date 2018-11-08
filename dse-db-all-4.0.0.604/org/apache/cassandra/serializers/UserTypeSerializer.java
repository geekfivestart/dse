package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UserTypeSerializer extends BytesSerializer {
   public final LinkedHashMap<String, TypeSerializer<?>> fields;

   public UserTypeSerializer(LinkedHashMap<String, TypeSerializer<?>> fields) {
      this.fields = fields;
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      ByteBuffer input = bytes.duplicate();
      int i = 0;
      Iterator var4 = this.fields.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<String, TypeSerializer<?>> entry = (Entry)var4.next();
         if(!input.hasRemaining()) {
            return;
         }

         if(input.remaining() < 4) {
            throw new MarshalException(String.format("Not enough bytes to read size of %dth field %s", new Object[]{Integer.valueOf(i), entry.getKey()}));
         }

         int size = input.getInt();
         if(size >= 0) {
            if(input.remaining() < size) {
               throw new MarshalException(String.format("Not enough bytes to read %dth field %s", new Object[]{Integer.valueOf(i), entry.getKey()}));
            }

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            ((TypeSerializer)entry.getValue()).validate(field);
            ++i;
         }
      }

      if(input.hasRemaining()) {
         throw new MarshalException("Invalid remaining data after end of UDT value");
      }
   }
}
