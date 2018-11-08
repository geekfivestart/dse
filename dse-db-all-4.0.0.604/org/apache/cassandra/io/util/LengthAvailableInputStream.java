package org.apache.cassandra.io.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LengthAvailableInputStream extends FilterInputStream {
   private long remainingBytes;

   public LengthAvailableInputStream(InputStream in, long totalLength) {
      super(in);
      this.remainingBytes = totalLength;
   }

   public int read() throws IOException {
      int b = this.in.read();
      --this.remainingBytes;
      return b;
   }

   public int read(byte[] b) throws IOException {
      int length = this.in.read(b);
      this.remainingBytes -= (long)length;
      return length;
   }

   public int read(byte[] b, int off, int len) throws IOException {
      int length = this.in.read(b, off, len);
      this.remainingBytes -= (long)length;
      return length;
   }

   public long skip(long n) throws IOException {
      long length = this.in.skip(n);
      this.remainingBytes -= length;
      return length;
   }

   public int available() throws IOException {
      return this.remainingBytes <= 0L?0:(this.remainingBytes > 2147483647L?2147483647:(int)this.remainingBytes);
   }

   public void close() throws IOException {
      this.in.close();
   }

   public synchronized void mark(int readlimit) {
   }

   public synchronized void reset() throws IOException {
      throw new IOException("Mark/Reset not supported");
   }

   public boolean markSupported() {
      return false;
   }
}
