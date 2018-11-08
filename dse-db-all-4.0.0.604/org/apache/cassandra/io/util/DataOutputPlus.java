package org.apache.cassandra.io.util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.cassandra.utils.vint.VIntCoding;

public interface DataOutputPlus extends DataOutput {
   void write(ByteBuffer var1) throws IOException;

   void write(Memory var1, long var2, long var4) throws IOException;

   <R> R applyToChannel(ThrowingFunction<WritableByteChannel, R> var1) throws IOException;

   default void writeVInt(long i) throws IOException {
      VIntCoding.writeVInt(i, this);
   }

   default void writeUnsignedVInt(long i) throws IOException {
      VIntCoding.writeUnsignedVInt(i, this);
   }

   default long position() {
      throw new UnsupportedOperationException();
   }

   default boolean hasPosition() {
      return false;
   }
}
