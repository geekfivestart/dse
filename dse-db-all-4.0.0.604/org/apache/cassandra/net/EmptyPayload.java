package org.apache.cassandra.net;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

public class EmptyPayload {
   public static final Serializer<EmptyPayload> serializer = new Serializer<EmptyPayload>() {
      public void serialize(EmptyPayload p, DataOutputPlus out) {
      }

      public EmptyPayload deserialize(DataInputPlus in) {
         return EmptyPayload.instance;
      }

      public long serializedSize(EmptyPayload p) {
         return 0L;
      }
   };
   public static final EmptyPayload instance = new EmptyPayload();

   private EmptyPayload() {
   }

   public Serializer<EmptyPayload> serializer() {
      return serializer;
   }
}
