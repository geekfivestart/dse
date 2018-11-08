package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>> {
   private static final ConcurrentMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new ConcurrentHashMap();
   public final TypeSerializer<K> keys;
   public final TypeSerializer<V> values;
   private final Comparator<Pair<ByteBuffer, ByteBuffer>> comparator;

   public static <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator) {
      Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.create(keys, values);
      MapSerializer<K, V> t = (MapSerializer)instances.get(p);
      if(t == null) {
         t = (MapSerializer)instances.computeIfAbsent(p, (k) -> {
            return new MapSerializer((TypeSerializer)k.left, (TypeSerializer)k.right, comparator);
         });
      }

      return t;
   }

   private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator) {
      this.keys = keys;
      this.values = values;
      this.comparator = (p1, p2) -> {
         return comparator.compare(p1.left, p2.left);
      };
   }

   public List<ByteBuffer> serializeValues(Map<K, V> map) {
      List<Pair<ByteBuffer, ByteBuffer>> pairs = new ArrayList(map.size());
      Iterator var3 = map.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<K, V> entry = (Entry)var3.next();
         pairs.add(Pair.create(this.keys.serialize(entry.getKey()), this.values.serialize(entry.getValue())));
      }

      Collections.sort(pairs, this.comparator);
      List<ByteBuffer> buffers = new ArrayList(pairs.size() * 2);
      Iterator var7 = pairs.iterator();

      while(var7.hasNext()) {
         Pair<ByteBuffer, ByteBuffer> p = (Pair)var7.next();
         buffers.add(p.left);
         buffers.add(p.right);
      }

      return buffers;
   }

   public int getElementCount(Map<K, V> value) {
      return value.size();
   }

   public void validateForNativeProtocol(ByteBuffer bytes, ProtocolVersion version) {
      try {
         ByteBuffer input = bytes.duplicate();
         int n = readCollectionSize(input, version);

         for(int i = 0; i < n; ++i) {
            this.keys.validate(readValue(input, version));
            this.values.validate(readValue(input, version));
         }

         if(input.hasRemaining()) {
            throw new MarshalException("Unexpected extraneous bytes after map value");
         }
      } catch (BufferUnderflowException var6) {
         throw new MarshalException("Not enough bytes to read a map");
      }
   }

   public Map<K, V> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version) {
      try {
         ByteBuffer input = bytes.duplicate();
         int n = readCollectionSize(input, version);
         if(n < 0) {
            throw new MarshalException("The data cannot be deserialized as a map");
         } else {
            Map<K, V> m = new LinkedHashMap(Math.min(n, 256));

            for(int i = 0; i < n; ++i) {
               ByteBuffer kbb = readValue(input, version);
               this.keys.validate(kbb);
               ByteBuffer vbb = readValue(input, version);
               this.values.validate(vbb);
               m.put(this.keys.deserialize(kbb), this.values.deserialize(vbb));
            }

            if(input.hasRemaining()) {
               throw new MarshalException("Unexpected extraneous bytes after map value");
            } else {
               return m;
            }
         }
      } catch (BufferUnderflowException var9) {
         throw new MarshalException("Not enough bytes to read a map");
      }
   }

   public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator) {
      try {
         ByteBuffer input = collection.duplicate();
         int n = readCollectionSize(input, ProtocolVersion.V3);

         for(int i = 0; i < n; ++i) {
            ByteBuffer kbb = readValue(input, ProtocolVersion.V3);
            int comparison = comparator.compareForCQL(kbb, key);
            if(comparison == 0) {
               return readValue(input, ProtocolVersion.V3);
            }

            if(comparison > 0) {
               return null;
            }

            skipValue(input, ProtocolVersion.V3);
         }

         return null;
      } catch (BufferUnderflowException var9) {
         throw new MarshalException("Not enough bytes to read a map");
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
               ByteBuffer kbb = readValue(input, ProtocolVersion.V3);
               int comparison;
               if(!inSlice) {
                  comparison = comparator.compareForCQL(from, kbb);
                  if(comparison > 0) {
                     skipValue(input, ProtocolVersion.V3);
                     continue;
                  }

                  inSlice = true;
                  startPos = pos;
               }

               comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER?-1:comparator.compareForCQL(kbb, to);
               if(comparison > 0) {
                  input.position(pos);
                  break;
               }

               skipValue(input, ProtocolVersion.V3);
               ++count;
               if(comparison == 0) {
                  break;
               }
            }

            return count == 0 && !frozen?null:this.copyAsNewCollection(collection, count, startPos, input.position(), ProtocolVersion.V3);
         } catch (BufferUnderflowException var15) {
            throw new MarshalException("Not enough bytes to read a map");
         }
      }
   }

   public String toString(Map<K, V> value) {
      StringBuilder sb = new StringBuilder();
      sb.append('{');
      boolean isFirst = true;
      Iterator var4 = value.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<K, V> element = (Entry)var4.next();
         if(isFirst) {
            isFirst = false;
         } else {
            sb.append(", ");
         }

         sb.append(this.keys.toString(element.getKey()));
         sb.append(": ");
         sb.append(this.values.toString(element.getValue()));
      }

      sb.append('}');
      return sb.toString();
   }

   public Class<Map<K, V>> getType() {
      return Map.class;
   }
}
