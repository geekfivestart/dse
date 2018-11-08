package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.serializers.UUIDSerializer;

public abstract class UuidFcts {
   public static final Function uuidFct;

   public UuidFcts() {
   }

   public static Collection<Function> all() {
      return Collections.singleton(uuidFct);
   }

   static {
      uuidFct = new NativeScalarFunction("uuid", UUIDType.instance, new AbstractType[0]) {
         public ByteBuffer execute(Arguments arguments) {
            return UUIDSerializer.instance.serialize(UUID.randomUUID());
         }

         public boolean isDeterministic() {
            return false;
         }
      };
   }
}
