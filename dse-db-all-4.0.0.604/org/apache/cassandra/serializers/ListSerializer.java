package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

public class ListSerializer<T> extends CollectionSerializer<List<T>> {
   private static final ConcurrentMap<TypeSerializer<?>, ListSerializer> instances = new ConcurrentHashMap();
   public final TypeSerializer<T> elements;

   public static <T> ListSerializer<T> getInstance(TypeSerializer<T> elements) {
      ListSerializer<T> t = (ListSerializer)instances.get(elements);
      if(t == null) {
         t = (ListSerializer)instances.computeIfAbsent(elements, (k) -> {
            return new ListSerializer(k);
         });
      }

      return t;
   }

   private ListSerializer(TypeSerializer<T> elements) {
      this.elements = elements;
   }


   public List<ByteBuffer> serializeValues(List<T> values) {
      ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>(values.size());
      for (T value : values) {
         buffers.add(this.elements.serialize(value));
      }
      return buffers;
   }

   public int getElementCount(List<T> value) {
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
            throw new MarshalException("Unexpected extraneous bytes after list value");
         }
      } catch (BufferUnderflowException var6) {
         throw new MarshalException("Not enough bytes to read a list");
      }
   }

   public List<T> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version) {
      try {
         ByteBuffer input = bytes.duplicate();
         int n = ListSerializer.readCollectionSize(input, version);
         if (n < 0) {
            throw new MarshalException("The data cannot be deserialized as a list");
         }
         ArrayList<T> l = new ArrayList<T>(Math.min(n, 256));
         for (int i = 0; i < n; ++i) {
            ByteBuffer databb = ListSerializer.readValue(input, version);
            if (databb != null) {
               this.elements.validate(databb);
               l.add(this.elements.deserialize(databb));
               continue;
            }
            l.add(null);
         }
         if (input.hasRemaining()) {
            throw new MarshalException("Unexpected extraneous bytes after list value");
         }
         return l;
      }
      catch (BufferUnderflowException e) {
         throw new MarshalException("Not enough bytes to read a list");
      }
   }

   public ByteBuffer getElement(ByteBuffer serializedList, int index) {
      try {
         ByteBuffer input = serializedList.duplicate();
         int n = readCollectionSize(input, ProtocolVersion.V3);
         if(n <= index) {
            return null;
         } else {
            for(int i = 0; i < index; ++i) {
               int length = input.getInt();
               input.position(input.position() + length);
            }

            return readValue(input, ProtocolVersion.V3);
         }
      } catch (BufferUnderflowException var7) {
         throw new MarshalException("Not enough bytes to read a list");
      }
   }

   public String toString(List<T> value) {
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      sb.append('[');
      for (T element : value) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(", ");
         }
         sb.append(this.elements.toString(element));
      }
      sb.append(']');
      return sb.toString();
   }

   public Class<List<T>> getType() {
      return (Class)List.class;
   }

   public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to, AbstractType<?> comparator, boolean frozen) {
      throw new UnsupportedOperationException();
   }
}
