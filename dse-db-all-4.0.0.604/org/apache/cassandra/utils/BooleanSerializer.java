package org.apache.cassandra.utils;

import java.io.IOException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class BooleanSerializer implements Serializer<Boolean> {
   public static BooleanSerializer serializer = new BooleanSerializer();

   public BooleanSerializer() {
   }

   public void serialize(Boolean b, DataOutputPlus out) throws IOException {
      out.writeBoolean(b.booleanValue());
   }

   public Boolean deserialize(DataInputPlus in) throws IOException {
      return Boolean.valueOf(in.readBoolean());
   }

   public long serializedSize(Boolean aBoolean) {
      return 1L;
   }
}
