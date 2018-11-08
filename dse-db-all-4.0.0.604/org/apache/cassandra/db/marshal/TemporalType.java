package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.commons.lang3.mutable.MutableLong;

public abstract class TemporalType<T> extends AbstractType<T> {
   public TemporalType(AbstractType.ComparisonType comparisonType, int fixedValueLength) {
      super(comparisonType, fixedValueLength);
   }

   public ByteBuffer now() {
      return this.fromTimeInMillis(System.currentTimeMillis());
   }

   public long toTimeInMillis(ByteBuffer value) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer fromTimeInMillis(long timeInMillis) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer addDuration(Number temporal, Duration duration) {
      long timeInMillis = temporal.longValue();
      this.validateDuration(duration);
      return this.fromTimeInMillis(duration.addTo(timeInMillis));
   }

   public ByteBuffer substractDuration(Number temporal, Duration duration) {
      long timeInMillis = temporal.longValue();
      this.validateDuration(duration);
      return this.fromTimeInMillis(duration.substractFrom(timeInMillis));
   }

   protected void validateDuration(Duration duration) {
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new ArgumentDeserializer() {
         private final MutableLong wrapper = new MutableLong();

         public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
            if(buffer != null && (buffer.hasRemaining() || !TemporalType.this.isEmptyValueMeaningless())) {
               this.wrapper.setValue(TemporalType.this.toTimeInMillis(buffer));
               return this.wrapper;
            } else {
               return null;
            }
         }
      };
   }
}
