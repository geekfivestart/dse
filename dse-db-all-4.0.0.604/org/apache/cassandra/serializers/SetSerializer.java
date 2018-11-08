package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SetSerializer<T> extends CollectionSerializer<Set<T>> {
   private static final ConcurrentMap<TypeSerializer<?>, SetSerializer> instances = new ConcurrentHashMap();
   public final TypeSerializer<T> elements;
   private final Comparator<ByteBuffer> comparator;

   public static <T> SetSerializer<T> getInstance(TypeSerializer<T> elements, Comparator<ByteBuffer> elementComparator) {
      SetSerializer<T> t = (SetSerializer)instances.get(elements);
      if(t == null) {
         t = (SetSerializer)instances.computeIfAbsent(elements, (k) -> {
            return new SetSerializer(k, elementComparator);
         });
      }

      return t;
   }

   private SetSerializer(TypeSerializer<T> elements, Comparator<ByteBuffer> comparator) {
      this.elements = elements;
      this.comparator = comparator;
   }

   public List<ByteBuffer> serializeValues(Set<T> values) {
      List<ByteBuffer> buffers = new ArrayList(values.size());
      Iterator var3 = values.iterator();

      while(var3.hasNext()) {
         T value = var3.next();
         buffers.add(this.elements.serialize(value));
      }

      Collections.sort(buffers, this.comparator);
      return buffers;
   }

   public int getElementCount(Set<T> value) {
      return value.size();
   }

   public void validateForNativeProtocol(ByteBuffer bytes, ProtocolVersion version) {
      try {
         ByteBuffer input = bytes.duplicate();
         int n = readCollectionSize(input, version);

         for(int i = 0; i < n; ++i) {
            this.elements.validate(readValue(input, version));
         }

         if(input.hasRemaining()) {
            throw new MarshalException("Unexpected extraneous bytes after set value");
         }
      } catch (BufferUnderflowException var6) {
         throw new MarshalException("Not enough bytes to read a set");
      }
   }

   public Set<T> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version) {
      try {
         ByteBuffer input = bytes.duplicate();
         int n = readCollectionSize(input, version);
         if(n < 0) {
            throw new MarshalException("The data cannot be deserialized as a set");
         } else {
            Set<T> l = new LinkedHashSet(Math.min(n, 256));

            for(int i = 0; i < n; ++i) {
               ByteBuffer databb = readValue(input, version);
               this.elements.validate(databb);
               l.add(this.elements.deserialize(databb));
            }

            if(input.hasRemaining()) {
               throw new MarshalException("Unexpected extraneous bytes after set value");
            } else {
               return l;
            }
         }
      } catch (BufferUnderflowException var8) {
         throw new MarshalException("Not enough bytes to read a set");
      }
   }

   public String toString(Set<T> value) {
      StringBuilder sb = new StringBuilder();
      sb.append('{');
      boolean isFirst = true;

      Object element;
      for(Iterator var4 = value.iterator(); var4.hasNext(); sb.append(this.elements.toString(element))) {
         element = var4.next();
         if(isFirst) {
            isFirst = false;
         } else {
            sb.append(", ");
         }
      }

      sb.append('}');
      return sb.toString();
   }

   public Class<Set<T>> getType() {
      return Set.class;
   }

   public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator) {
      try {
         ByteBuffer input = collection.duplicate();
         int n = readCollectionSize(input, ProtocolVersion.V3);

         for(int i = 0; i < n; ++i) {
            ByteBuffer value = readValue(input, ProtocolVersion.V3);
            int comparison = comparator.compareForCQL(value, key);
            if(comparison == 0) {
               return value;
            }

            if(comparison > 0) {
               return null;
            }
         }

         return null;
      } catch (BufferUnderflowException var9) {
         throw new MarshalException("Not enough bytes to read a set");
      }
   }

   public ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to, AbstractType<?> comparator, boolean frozen) {
      if(from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER) {
         return collection;
      } else {
         try {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for(int i = 0; i < n; ++i) {
               int pos = input.position();
               ByteBuffer value = readValue(input, ProtocolVersion.V3);
               int comparison;
               if(!inSlice) {
                  comparison = comparator.compareForCQL(from, value);
                  if(comparison > 0) {
                     continue;
                  }

                  inSlice = true;
                  startPos = pos;
               }

               comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER?-1:comparator.compareForCQL(value, to);
               if(comparison > 0) {
                  input.position(pos);
                  break;
               }

               ++count;
               if(comparison == 0) {
                  break;
               }
            }

            return count == 0 && !frozen?null:this.copyAsNewCollection(collection, count, startPos, input.position(), ProtocolVersion.V3);
         } catch (BufferUnderflowException var15) {
            throw new MarshalException("Not enough bytes to read a set");
         }
      }
   }
}
