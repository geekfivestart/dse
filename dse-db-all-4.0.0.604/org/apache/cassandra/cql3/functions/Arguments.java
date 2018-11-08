package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import org.apache.cassandra.transport.ProtocolVersion;

public interface Arguments {
   void set(int var1, ByteBuffer var2);

   boolean containsNulls();

   <T> T get(int var1);

   ProtocolVersion getProtocolVersion();

   default boolean getAsBoolean(int i) {
      return ((Boolean)this.get(i)).booleanValue();
   }

   default byte getAsByte(int i) {
      return ((Number)this.get(i)).byteValue();
   }

   default short getAsShort(int i) {
      return ((Number)this.get(i)).shortValue();
   }

   default int getAsInt(int i) {
      return ((Number)this.get(i)).intValue();
   }

   default long getAsLong(int i) {
      return ((Number)this.get(i)).longValue();
   }

   default float getAsFloat(int i) {
      return ((Number)this.get(i)).floatValue();
   }

   default double getAsDouble(int i) {
      return ((Number)this.get(i)).doubleValue();
   }

   int size();
}
