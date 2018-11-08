package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.OutputStream;

public class MemoryOutputStream extends OutputStream {
   private final Memory mem;
   private int position = 0;

   public MemoryOutputStream(Memory mem) {
      this.mem = mem;
   }

   public void write(int b) {
      this.mem.setByte((long)(this.position++), (byte)b);
   }

   public void write(byte[] b, int off, int len) throws IOException {
      this.mem.setBytes((long)this.position, b, off, len);
      this.position += len;
   }

   public int position() {
      return this.position;
   }
}
