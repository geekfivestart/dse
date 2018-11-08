package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface Serializer<T> {
   static default <T> Serializer<Optional<T>> forOptional(final Serializer<T> serializer) {
      return new Serializer<Optional<T>>() {
         public void serialize(Optional<T> t, DataOutputPlus out) throws IOException {
            out.writeBoolean(t.isPresent());
            if(t.isPresent()) {
               serializer.serialize(t.get(), out);
            }

         }

         public Optional<T> deserialize(DataInputPlus in) throws IOException {
            return in.readBoolean()?Optional.of(serializer.deserialize(in)):Optional.empty();
         }

         public long serializedSize(Optional<T> t) {
            Serializer var10002 = serializer;
            serializer.getClass();
            return 1L + ((Long)t.map(var10002::serializedSize).orElse(Long.valueOf(0L))).longValue();
         }
      };
   }

   void serialize(T var1, DataOutputPlus var2) throws IOException;

   T deserialize(DataInputPlus var1) throws IOException;

   long serializedSize(T var1);
}
