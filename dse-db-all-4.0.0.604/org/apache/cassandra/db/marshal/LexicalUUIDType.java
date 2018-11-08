package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UUIDGen;

public class LexicalUUIDType extends AbstractType<UUID> {
   public static final LexicalUUIDType instance = new LexicalUUIDType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   LexicalUUIDType() {
      super(AbstractType.ComparisonType.CUSTOM, 16);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return UUIDGen.getUUID(o1).compareTo(UUIDGen.getUUID(o2));
   }

   public ByteSource asByteComparableSource(final ByteBuffer buf) {
      if(buf != null && buf.remaining() != 0) {
         final int bufstart = buf.position();
         return new ByteSource.WithToString() {
            int bufpos = 0;

            public int next() {
               if(this.bufpos + bufstart >= buf.limit()) {
                  return -1;
               } else {
                  int v = buf.get(this.bufpos + bufstart) & 255;
                  if(this.bufpos == 0 || this.bufpos == 8) {
                     v ^= 128;
                  }

                  ++this.bufpos;
                  return v;
               }
            }

            public void reset() {
               this.bufpos = 0;
            }
         };
      } else {
         return null;
      }
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         try {
            return this.decompose(UUID.fromString(source));
         } catch (IllegalArgumentException var3) {
            throw new MarshalException(String.format("unable to make UUID from '%s'", new Object[]{source}), var3);
         }
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of a uuid, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public TypeSerializer<UUID> getSerializer() {
      return UUIDSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
