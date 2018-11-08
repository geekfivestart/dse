package com.datastax.bdp.db.nodesync;

import java.io.IOException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Serializer;

public class UserValidationID {
   public static final Serializer<UserValidationID> serializer = new Serializer<UserValidationID>() {
      public void serialize(UserValidationID id, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithLength(UTF8Serializer.instance.serialize(id.toString()), out);
      }

      public UserValidationID deserialize(DataInputPlus in) throws IOException {
         return UserValidationID.from(UTF8Serializer.instance.deserialize(ByteBufferUtil.readWithLength(in)));
      }

      public long serializedSize(UserValidationID id) {
         return (long)ByteBufferUtil.serializedSizeWithLength(UTF8Serializer.instance.serialize(id.toString()));
      }
   };
   private final String id;

   private UserValidationID(String id) {
      this.id = id;
   }

   public static UserValidationID from(String id) {
      return new UserValidationID(id);
   }

   public String toString() {
      return this.id;
   }

   public final int hashCode() {
      return this.id.hashCode();
   }

   public final boolean equals(Object o) {
      if(!(o instanceof UserValidationID)) {
         return false;
      } else {
         UserValidationID that = (UserValidationID)o;
         return this.id.equals(that.id);
      }
   }
}
