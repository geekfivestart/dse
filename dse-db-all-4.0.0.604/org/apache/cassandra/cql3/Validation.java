package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;

public abstract class Validation {
   public Validation() {
   }

   public static void validateKey(TableMetadata metadata, ByteBuffer key) {
      if(key != null && key.remaining() != 0) {
         if(key.remaining() > '\uffff') {
            throw new InvalidRequestException("Key length of " + key.remaining() + " is longer than maximum of " + '\uffff');
         } else {
            try {
               metadata.partitionKeyType.validate(key);
            } catch (MarshalException var3) {
               throw new InvalidRequestException(var3.getMessage());
            }
         }
      } else {
         throw new InvalidRequestException("Key may not be empty");
      }
   }
}
