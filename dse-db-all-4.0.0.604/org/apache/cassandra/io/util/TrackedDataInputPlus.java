package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class TrackedDataInputPlus implements DataInputPlus, BytesReadTracker {
   private long bytesRead;
   final DataInput source;

   public TrackedDataInputPlus(DataInput source) {
      this.source = source;
   }

   public long getBytesRead() {
      return this.bytesRead;
   }

   public void reset(long count) {
      this.bytesRead = count;
   }

   public boolean readBoolean() throws IOException {
      boolean bool = this.source.readBoolean();
      ++this.bytesRead;
      return bool;
   }

   public byte readByte() throws IOException {
      byte b = this.source.readByte();
      ++this.bytesRead;
      return b;
   }

   public char readChar() throws IOException {
      char c = this.source.readChar();
      this.bytesRead += 2L;
      return c;
   }

   public double readDouble() throws IOException {
      double d = this.source.readDouble();
      this.bytesRead += 8L;
      return d;
   }

   public float readFloat() throws IOException {
      float f = this.source.readFloat();
      this.bytesRead += 4L;
      return f;
   }

   public void readFully(byte[] b, int off, int len) throws IOException {
      this.source.readFully(b, off, len);
      this.bytesRead += (long)len;
   }

   public void readFully(byte[] b) throws IOException {
      this.source.readFully(b);
      this.bytesRead += (long)b.length;
   }

   public int readInt() throws IOException {
      int i = this.source.readInt();
      this.bytesRead += 4L;
      return i;
   }

   public String readLine() throws IOException {
      throw new UnsupportedOperationException();
   }

   public long readLong() throws IOException {
      long l = this.source.readLong();
      this.bytesRead += 8L;
      return l;
   }

   public short readShort() throws IOException {
      short s = this.source.readShort();
      this.bytesRead += 2L;
      return s;
   }

   public String readUTF() throws IOException {
      return DataInputStream.readUTF(this);
   }

   public int readUnsignedByte() throws IOException {
      int i = this.source.readUnsignedByte();
      ++this.bytesRead;
      return i;
   }

   public int readUnsignedShort() throws IOException {
      int i = this.source.readUnsignedShort();
      this.bytesRead += 2L;
      return i;
   }

   public int skipBytes(int n) throws IOException {
      int skipped = this.source.skipBytes(n);
      this.bytesRead += (long)skipped;
      return skipped;
   }
}
