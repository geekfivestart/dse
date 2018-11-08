package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.commons.lang3.mutable.Mutable;

public abstract class NumberType<T extends Number> extends AbstractType<T> {
   protected NumberType(AbstractType.ComparisonType comparisonType, int valueLength) {
      super(comparisonType, valueLength);
   }

   public NumberType(AbstractType.ComparisonType comparisonType, int valueLength, AbstractType.PrimitiveType primitiveType) {
      super(comparisonType, valueLength, primitiveType, 0);
   }

   public boolean isFloatingPoint() {
      return false;
   }

   public abstract ByteBuffer add(Number var1, Number var2);

   public abstract ByteBuffer substract(Number var1, Number var2);

   public abstract ByteBuffer multiply(Number var1, Number var2);

   public abstract ByteBuffer divide(Number var1, Number var2);

   public abstract ByteBuffer mod(Number var1, Number var2);

   public abstract ByteBuffer negate(Number var1);

   protected abstract class NumberArgumentDeserializer<M extends Mutable<Number>> implements ArgumentDeserializer {
      protected final M wrapper;

      public NumberArgumentDeserializer(M this$0) {
         this.wrapper = wrapper;
      }

      public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
         if(buffer != null && buffer.hasRemaining()) {
            this.setMutableValue(this.wrapper, buffer);
            return this.wrapper;
         } else {
            return null;
         }
      }

      protected abstract void setMutableValue(M var1, ByteBuffer var2);
   }
}
