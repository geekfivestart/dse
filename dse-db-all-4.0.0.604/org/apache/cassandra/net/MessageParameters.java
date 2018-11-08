package org.apache.cassandra.net;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MessageParameters {
   public static final MessageParameters EMPTY = new MessageParameters(ImmutableMap.of());
   private static final MessageParameters.Serializer serializer = new MessageParameters.Serializer();
   private final ImmutableMap<String, byte[]> parameters;

   private MessageParameters(ImmutableMap<String, byte[]> parameters) {
      this.parameters = parameters;
   }

   public static MessageParameters from(Map<String, byte[]> map) {
      return map.isEmpty()?EMPTY:new MessageParameters(ImmutableMap.copyOf(map));
   }

   MessageParameters unionWith(MessageParameters other) {
      MessageParameters.Builder builder = builder();
      builder.builder.putAll(this.parameters);
      builder.builder.putAll(other.parameters);
      return builder.build();
   }

   public static MessageParameters.Serializer serializer() {
      return serializer;
   }

   public boolean isEmpty() {
      return this.parameters.isEmpty();
   }

   public boolean has(String key) {
      return this.parameters.containsKey(key);
   }

   public String getString(String key) {
      try {
         byte[] value = this.get(key);
         return value == null?null:ByteBufferUtil.string(ByteBuffer.wrap(value));
      } catch (CharacterCodingException var3) {
         throw new IllegalStateException();
      }
   }

   public Integer getInt(String key) {
      byte[] value = this.get(key);
      return value == null?null:Integer.valueOf(ByteBufferUtil.toInt(ByteBuffer.wrap(value)));
   }

   public Long getLong(String key) {
      byte[] value = this.get(key);
      return value == null?null:Long.valueOf(ByteBufferUtil.toLong(ByteBuffer.wrap(value)));
   }

   public byte[] get(String key) {
      return (byte[])this.parameters.get(key);
   }

   public static MessageParameters.Builder builder() {
      return new MessageParameters.Builder();
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(MessageParameters parameters, DataOutputPlus out) throws IOException {
         out.writeVInt((long)parameters.parameters.size());
         UnmodifiableIterator var3 = parameters.parameters.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<String, byte[]> parameter = (Entry)var3.next();
            out.writeUTF((String)parameter.getKey());
            out.writeVInt((long)((byte[])parameter.getValue()).length);
            out.write((byte[])parameter.getValue());
         }

      }

      public MessageParameters deserialize(DataInputPlus in) throws IOException {
         int size = (int)in.readVInt();
         MessageParameters.Builder builder = MessageParameters.builder();

         for(int i = 0; i < size; ++i) {
            String key = in.readUTF();
            byte[] value = new byte[(int)in.readVInt()];
            in.readFully(value);
            builder.put(key, value);
         }

         return builder.build();
      }

      public long serializedSize(MessageParameters parameters) {
         long size = (long)TypeSizes.sizeofVInt((long)parameters.parameters.size());

         int length;
         for(UnmodifiableIterator var4 = parameters.parameters.entrySet().iterator(); var4.hasNext(); size += (long)(TypeSizes.sizeofVInt((long)length) + length)) {
            Entry<String, byte[]> parameter = (Entry)var4.next();
            size += (long)TypeSizes.sizeof((String)parameter.getKey());
            length = ((byte[])parameter.getValue()).length;
         }

         return size;
      }
   }

   public static class Builder {
      private final com.google.common.collect.ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();

      public Builder() {
      }

      public MessageParameters.Builder putString(String key, String value) {
         return this.put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
      }

      public MessageParameters.Builder putLong(String key, long value) {
         return this.put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
      }

      public MessageParameters.Builder putInt(String key, int value) {
         return this.put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
      }

      public MessageParameters.Builder put(String key, byte[] value) {
         this.builder.put(key, value);
         return this;
      }

      public MessageParameters build() {
         return new MessageParameters(this.builder.build());
      }
   }
}
