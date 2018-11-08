package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class DataOutputBufferFixed extends DataOutputBuffer {
   public DataOutputBufferFixed() {
      this(128);
   }

   public DataOutputBufferFixed(int size) {
      super(size);
   }

   public DataOutputBufferFixed(ByteBuffer buffer) {
      super(buffer);
   }

   protected void doFlush(int count) throws IOException {
      throw new BufferOverflowException();
   }

   protected void expandToFit(long newSize) {
      throw new BufferOverflowException();
   }

   public void clear() {
      this.buffer.clear();
   }
}
