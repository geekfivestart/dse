package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.utils.Throwables;

public class RewindableDataInputStreamPlus extends FilterInputStream implements RewindableDataInput, Closeable {
   private boolean marked = false;
   private boolean exhausted = false;
   private AtomicBoolean closed = new AtomicBoolean(false);
   protected int memAvailable = 0;
   protected int diskTailAvailable = 0;
   protected int diskHeadAvailable = 0;
   private final File spillFile;
   private final int initialMemBufferSize;
   private final int maxMemBufferSize;
   private final int maxDiskBufferSize;
   private volatile byte[] memBuffer;
   private int memBufferSize;
   private RandomAccessFile spillBuffer;
   private final DataInputPlus dataReader = new DataInputPlus.DataInputStreamPlus(this);

   public RewindableDataInputStreamPlus(InputStream in, int initialMemBufferSize, int maxMemBufferSize, File spillFile, int maxDiskBufferSize) {
      super(in);
      this.initialMemBufferSize = initialMemBufferSize;
      this.maxMemBufferSize = maxMemBufferSize;
      this.spillFile = spillFile;
      this.maxDiskBufferSize = maxDiskBufferSize;
   }

   public DataPosition mark() {
      this.mark(0);
      return new RewindableDataInputStreamPlus.RewindableDataInputPlusMark();
   }

   public void reset(DataPosition mark) throws IOException {
      this.reset();
   }

   public long bytesPastMark(DataPosition mark) {
      return (long)(this.maxMemBufferSize - this.memAvailable + (this.diskTailAvailable == -1?0:this.maxDiskBufferSize - this.diskHeadAvailable - this.diskTailAvailable));
   }

   public boolean markSupported() {
      return true;
   }

   public synchronized void mark(int readlimit) {
      if(this.marked) {
         throw new IllegalStateException("Cannot mark already marked stream.");
      } else if(this.memAvailable <= 0 && this.diskHeadAvailable <= 0 && this.diskTailAvailable <= 0) {
         this.marked = true;
         this.memAvailable = this.maxMemBufferSize;
         this.diskHeadAvailable = -1;
         this.diskTailAvailable = -1;
      } else {
         throw new IllegalStateException("Can only mark stream after reading previously marked data.");
      }
   }

   public synchronized void reset() throws IOException {
      if(!this.marked) {
         throw new IOException("Must call mark() before calling reset().");
      } else if(this.exhausted) {
         throw new IOException(String.format("Read more than capacity: %d bytes.", new Object[]{Integer.valueOf(this.maxMemBufferSize + this.maxDiskBufferSize)}));
      } else {
         this.memAvailable = this.maxMemBufferSize - this.memAvailable;
         this.memBufferSize = this.memAvailable;
         if(this.diskTailAvailable == -1) {
            this.diskHeadAvailable = 0;
            this.diskTailAvailable = 0;
         } else {
            int initialPos = this.diskTailAvailable > 0?0:(int)((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).getFilePointer();
            int diskMarkpos = initialPos + this.diskHeadAvailable;
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).seek((long)diskMarkpos);
            this.diskHeadAvailable = diskMarkpos - this.diskHeadAvailable;
            this.diskTailAvailable = this.maxDiskBufferSize - this.diskTailAvailable - diskMarkpos;
         }

         this.marked = false;
      }
   }

   public int available() throws IOException {
      return super.available() + (this.marked?0:this.memAvailable + this.diskHeadAvailable + this.diskTailAvailable);
   }

   public int read() throws IOException {
      int read = this.readOne();
      if(read == -1) {
         return read;
      } else {
         if(this.marked) {
            if(this.isExhausted(1)) {
               this.exhausted = true;
               return read;
            }

            this.writeOne(read);
         }

         return read;
      }
   }

   public int read(byte[] b, int off, int len) throws IOException {
      int readBytes = this.readMulti(b, off, len);
      if(readBytes == -1) {
         return readBytes;
      } else {
         if(this.marked) {
            if(this.isExhausted(readBytes)) {
               this.exhausted = true;
               return readBytes;
            }

            this.writeMulti(b, off, readBytes);
         }

         return readBytes;
      }
   }

   private void maybeCreateDiskBuffer() throws IOException {
      if(this.spillBuffer == null) {
         if(!this.spillFile.getParentFile().exists()) {
            this.spillFile.getParentFile().mkdirs();
         }

         this.spillFile.createNewFile();
         this.spillBuffer = new RandomAccessFile(this.spillFile, "rw");
      }

   }

   private int readOne() throws IOException {
      if(!this.marked) {
         int read;
         if(this.memAvailable > 0) {
            read = this.memBufferSize - this.memAvailable;
            --this.memAvailable;
            return ((byte[])this.getIfNotClosed(this.memBuffer))[read] & 255;
         }

         if(this.diskTailAvailable > 0 || this.diskHeadAvailable > 0) {
            read = ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).read();
            if(this.diskTailAvailable > 0) {
               --this.diskTailAvailable;
            } else if(this.diskHeadAvailable > 0) {
               ++this.diskHeadAvailable;
            }

            if(this.diskTailAvailable == 0) {
               this.spillBuffer.seek(0L);
            }

            return read;
         }
      }

      return ((InputStream)this.getIfNotClosed(this.in)).read();
   }

   private boolean isExhausted(int readBytes) {
      return this.exhausted || (long)readBytes > (long)this.memAvailable + (long)(this.diskTailAvailable == -1?this.maxDiskBufferSize:this.diskTailAvailable + this.diskHeadAvailable);
   }

   private int readMulti(byte[] b, int off, int len) throws IOException {
      int readBytes = 0;
      if(!this.marked) {
         int readFromHead;
         if(this.memAvailable > 0) {
            readBytes += this.memAvailable < len?this.memAvailable:len;
            readFromHead = this.memBufferSize - this.memAvailable;
            System.arraycopy(this.memBuffer, readFromHead, b, off, readBytes);
            this.memAvailable -= readBytes;
            off += readBytes;
            len -= readBytes;
         }

         if(len > 0 && this.diskTailAvailable > 0) {
            readFromHead = this.diskTailAvailable < len?this.diskTailAvailable:len;
            readFromHead = ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).read(b, off, readFromHead);
            readBytes += readFromHead;
            this.diskTailAvailable -= readFromHead;
            off += readFromHead;
            len -= readFromHead;
            if(this.diskTailAvailable == 0) {
               this.spillBuffer.seek(0L);
            }
         }

         if(len > 0 && this.diskHeadAvailable > 0) {
            readFromHead = this.diskHeadAvailable < len?this.diskHeadAvailable:len;
            readFromHead = ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).read(b, off, readFromHead);
            readBytes += readFromHead;
            this.diskHeadAvailable -= readFromHead;
            off += readFromHead;
            len -= readFromHead;
         }
      }

      if(len > 0) {
         readBytes += ((InputStream)this.getIfNotClosed(this.in)).read(b, off, len);
      }

      return readBytes;
   }

   private void writeMulti(byte[] b, int off, int len) throws IOException {
      int diskHeadWritten;
      if(this.memAvailable > 0) {
         if(this.memBuffer == null) {
            this.memBuffer = new byte[this.initialMemBufferSize];
         }

         diskHeadWritten = this.maxMemBufferSize - this.memAvailable;
         int memWritten = this.memAvailable < len?this.memAvailable:len;
         if(diskHeadWritten + memWritten >= ((byte[])this.getIfNotClosed(this.memBuffer)).length) {
            this.growMemBuffer(diskHeadWritten, memWritten);
         }

         System.arraycopy(b, off, this.memBuffer, diskHeadWritten, memWritten);
         off += memWritten;
         len -= memWritten;
         this.memAvailable -= memWritten;
      }

      if(len > 0) {
         if(this.diskTailAvailable == -1) {
            this.maybeCreateDiskBuffer();
            this.diskHeadAvailable = (int)this.spillBuffer.getFilePointer();
            this.diskTailAvailable = this.maxDiskBufferSize - this.diskHeadAvailable;
         }

         if(len > 0 && this.diskTailAvailable > 0) {
            diskHeadWritten = this.diskTailAvailable < len?this.diskTailAvailable:len;
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).write(b, off, diskHeadWritten);
            off += diskHeadWritten;
            len -= diskHeadWritten;
            this.diskTailAvailable -= diskHeadWritten;
            if(this.diskTailAvailable == 0) {
               this.spillBuffer.seek(0L);
            }
         }

         if(len > 0 && this.diskTailAvailable > 0) {
            diskHeadWritten = this.diskHeadAvailable < len?this.diskHeadAvailable:len;
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).write(b, off, diskHeadWritten);
         }
      }

   }

   private void writeOne(int value) throws IOException {
      if(this.memAvailable > 0) {
         if(this.memBuffer == null) {
            this.memBuffer = new byte[this.initialMemBufferSize];
         }

         int pos = this.maxMemBufferSize - this.memAvailable;
         if(pos == ((byte[])this.getIfNotClosed(this.memBuffer)).length) {
            this.growMemBuffer(pos, 1);
         }

         ((byte[])this.getIfNotClosed(this.memBuffer))[pos] = (byte)value;
         --this.memAvailable;
      } else {
         if(this.diskTailAvailable == -1) {
            this.maybeCreateDiskBuffer();
            this.diskHeadAvailable = (int)this.spillBuffer.getFilePointer();
            this.diskTailAvailable = this.maxDiskBufferSize - this.diskHeadAvailable;
         }

         if(this.diskTailAvailable > 0 || this.diskHeadAvailable > 0) {
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).write(value);
            if(this.diskTailAvailable > 0) {
               --this.diskTailAvailable;
            } else if(this.diskHeadAvailable > 0) {
               --this.diskHeadAvailable;
            }

            if(this.diskTailAvailable == 0) {
               this.spillBuffer.seek(0L);
            }

         }
      }
   }

   public int read(byte[] b) throws IOException {
      return this.read(b, 0, b.length);
   }

   private void growMemBuffer(int pos, int writeSize) {
      int newSize = Math.min(2 * (pos + writeSize), this.maxMemBufferSize);
      byte[] newBuffer = new byte[newSize];
      System.arraycopy(this.memBuffer, 0, newBuffer, 0, pos);
      this.memBuffer = newBuffer;
   }

   public long skip(long n) throws IOException {
      long skipped = 0L;
      if(!this.marked) {
         if(this.memAvailable > 0) {
            skipped += (long)this.memAvailable < n?(long)this.memAvailable:n;
            this.memAvailable = (int)((long)this.memAvailable - skipped);
            n -= skipped;
         }

         int skipFromHead;
         if(n > 0L && this.diskTailAvailable > 0) {
            skipFromHead = (long)this.diskTailAvailable < n?this.diskTailAvailable:(int)n;
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).skipBytes(skipFromHead);
            this.diskTailAvailable -= skipFromHead;
            skipped += (long)skipFromHead;
            n -= (long)skipFromHead;
            if(this.diskTailAvailable == 0) {
               this.spillBuffer.seek(0L);
            }
         }

         if(n > 0L && this.diskHeadAvailable > 0) {
            skipFromHead = (long)this.diskHeadAvailable < n?this.diskHeadAvailable:(int)n;
            ((RandomAccessFile)this.getIfNotClosed(this.spillBuffer)).skipBytes(skipFromHead);
            this.diskHeadAvailable -= skipFromHead;
            skipped += (long)skipFromHead;
            n -= (long)skipFromHead;
         }

         if(n > 0L) {
            skipped += ((InputStream)this.getIfNotClosed(this.in)).skip(n);
         }

         return skipped;
      } else {
         while(n-- > 0L && this.read() != -1) {
            ++skipped;
         }

         return skipped;
      }
   }

   private <T> T getIfNotClosed(T in) throws IOException {
      if(this.closed.get()) {
         throw new IOException("Stream closed");
      } else {
         return in;
      }
   }

   public void close() throws IOException {
      this.close(true);
   }

   public void close(boolean closeUnderlying) throws IOException {
      if(this.closed.compareAndSet(false, true)) {
         Throwable fail = null;
         if(closeUnderlying) {
            try {
               super.close();
            } catch (IOException var6) {
               fail = Throwables.merge(fail, var6);
            }
         }

         try {
            if(this.spillBuffer != null) {
               this.spillBuffer.close();
               this.spillBuffer = null;
            }
         } catch (IOException var5) {
            fail = Throwables.merge(fail, var5);
         }

         try {
            if(this.spillFile.exists()) {
               this.spillFile.delete();
            }
         } catch (Throwable var4) {
            fail = Throwables.merge(fail, var4);
         }

         Throwables.maybeFail(fail, IOException.class);
      }

   }

   public void readFully(byte[] b) throws IOException {
      this.dataReader.readFully(b);
   }

   public void readFully(byte[] b, int off, int len) throws IOException {
      this.dataReader.readFully(b, off, len);
   }

   public int skipBytes(int n) throws IOException {
      return this.dataReader.skipBytes(n);
   }

   public boolean readBoolean() throws IOException {
      return this.dataReader.readBoolean();
   }

   public byte readByte() throws IOException {
      return this.dataReader.readByte();
   }

   public int readUnsignedByte() throws IOException {
      return this.dataReader.readUnsignedByte();
   }

   public short readShort() throws IOException {
      return this.dataReader.readShort();
   }

   public int readUnsignedShort() throws IOException {
      return this.dataReader.readUnsignedShort();
   }

   public char readChar() throws IOException {
      return this.dataReader.readChar();
   }

   public int readInt() throws IOException {
      return this.dataReader.readInt();
   }

   public long readLong() throws IOException {
      return this.dataReader.readLong();
   }

   public float readFloat() throws IOException {
      return this.dataReader.readFloat();
   }

   public double readDouble() throws IOException {
      return this.dataReader.readDouble();
   }

   public String readLine() throws IOException {
      return this.dataReader.readLine();
   }

   public String readUTF() throws IOException {
      return this.dataReader.readUTF();
   }

   protected static class RewindableDataInputPlusMark implements DataPosition {
      protected RewindableDataInputPlusMark() {
      }
   }
}
