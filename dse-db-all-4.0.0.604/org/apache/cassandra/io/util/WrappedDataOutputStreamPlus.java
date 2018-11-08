package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;

public class WrappedDataOutputStreamPlus extends UnbufferedDataOutputStreamPlus {
   protected final OutputStream out;

   public WrappedDataOutputStreamPlus(OutputStream out) {
      this.out = out;
   }

   public WrappedDataOutputStreamPlus(OutputStream out, WritableByteChannel channel) {
      super(channel);
      this.out = out;
   }

   public void write(byte[] buffer, int offset, int count) throws IOException {
      this.out.write(buffer, offset, count);
   }

   public void write(int oneByte) throws IOException {
      this.out.write(oneByte);
   }

   public void close() throws IOException {
      this.out.close();
   }

   public void flush() throws IOException {
      this.out.flush();
   }
}
