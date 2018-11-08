package org.apache.cassandra.index.sasi.utils;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.MarshalException;

public class TypeUtil {
   public TypeUtil() {
   }

   public static boolean isValid(ByteBuffer term, AbstractType<?> validator) {
      try {
         validator.validate(term);
         return true;
      } catch (MarshalException var3) {
         return false;
      }
   }

   public static ByteBuffer tryUpcast(ByteBuffer term, AbstractType<?> validator) {
      if(term.remaining() == 0) {
         return null;
      } else {
         try {
            if(validator instanceof Int32Type && term.remaining() == 2) {
               return Int32Type.instance.decompose(Integer.valueOf(term.getShort(term.position())));
            } else if(validator instanceof LongType) {
               long upcastToken;
               switch(term.remaining()) {
               case 2:
                  upcastToken = (long)term.getShort(term.position());
                  break;
               case 4:
                  upcastToken = (long)((Integer)Int32Type.instance.compose(term)).intValue();
                  break;
               default:
                  upcastToken = Long.parseLong(UTF8Type.instance.getString(term));
               }

               return LongType.instance.decompose(Long.valueOf(upcastToken));
            } else {
               return validator instanceof DoubleType && term.remaining() == 4?DoubleType.instance.decompose(Double.valueOf((double)((Float)FloatType.instance.compose(term)).floatValue())):validator.fromString(UTF8Type.instance.getString(term));
            }
         } catch (Exception var4) {
            return null;
         }
      }
   }
}
