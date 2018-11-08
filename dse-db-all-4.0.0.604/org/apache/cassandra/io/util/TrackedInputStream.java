package org.apache.cassandra.io.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TrackedInputStream extends FilterInputStream implements BytesReadTracker {
   private long bytesRead;

   public TrackedInputStream(InputStream source) {
      super(source);
   }

   public long getBytesRead() {
      return this.bytesRead;
   }

   public void reset(long count) {
      this.bytesRead = count;
   }

   public int read() throws IOException {
      int read = super.read();
      ++this.bytesRead;
      return read;
   }

   public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      this.bytesRead += (long)read;
      return read;
   }

   public int read(byte[] b) throws IOException {
      int read = super.read(b);
      this.bytesRead += (long)read;
      return read;
   }

   public long skip(long n) throws IOException {
      long skip = super.skip(n);
      this.bytesRead += skip;
      return skip;
   }
}
