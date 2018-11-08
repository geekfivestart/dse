package com.datastax.bdp.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class CompositeUtil {
   public CompositeUtil() {
   }

   public static ByteBuffer createDynamicCompositeKey(Object... objects) throws IOException {
      ByteBufferOutputStream out = new ByteBufferOutputStream();
      Object[] var2 = objects;
      int var3 = objects.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Object object = var2[var4];
         ByteBuffer bytes;
         if(object instanceof String) {
            bytes = ByteBufferUtil.bytes((String)object);
            out.writeShort(-32653);
            out.writeShort((short)bytes.remaining());
            out.write(bytes);
            out.write(0);
         } else if(object instanceof UUID) {
            out.writeShort(-32652);
            out.writeShort(16);
            out.write(UUIDGen.decompose((UUID)object));
            out.write(0);
         } else {
            if(!(object instanceof ByteBuffer)) {
               throw new MarshalException(object.getClass().getName() + " is not recognized as a valid type for this composite");
            }

            bytes = ((ByteBuffer)object).duplicate();
            out.writeShort(-32670);
            out.writeShort((short)bytes.remaining());
            out.write(bytes);
            out.write(0);
         }
      }

      return out.getByteBuffer();
   }

   public static List<Object> deserialize(ByteBuffer b) throws IOException {
      b = b.duplicate();

      ArrayList components;
      for(components = new ArrayList(); b.remaining() > 0; b.get()) {
         short header = b.getShort();
         ByteBuffer value;
         if(header == -32653) {
            value = getWithShortLength(b);
            components.add(ByteBufferUtil.string(value));
         } else if(header == -32652) {
            value = getWithShortLength(b);
            components.add(UUIDGen.getUUID(value));
         } else {
            if(header != -32670) {
               throw new MarshalException((header & 255) + " is not recognized as a valid type");
            }

            components.add(getWithShortLength(b));
         }
      }

      return components;
   }

   protected static ByteBuffer getWithShortLength(ByteBuffer bb) {
      int length = getShortLength(bb);
      return getBytes(bb, length);
   }

   protected static int getShortLength(ByteBuffer bb) {
      int length = (bb.get() & 255) << 8;
      return length | bb.get() & 255;
   }

   protected static ByteBuffer getBytes(ByteBuffer bb, int length) {
      ByteBuffer copy = bb.duplicate();
      copy.limit(copy.position() + length);
      bb.position(bb.position() + length);
      return copy;
   }
}
