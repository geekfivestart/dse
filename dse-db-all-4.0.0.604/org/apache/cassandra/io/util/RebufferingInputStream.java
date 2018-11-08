package org.apache.cassandra.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.vint.VIntCoding;

public abstract class RebufferingInputStream extends InputStream implements DataInputPlus, Closeable {
   protected ByteBuffer buffer;

   protected RebufferingInputStream(ByteBuffer buffer) {
      Preconditions.checkArgument(buffer == null || buffer.order() == ByteOrder.BIG_ENDIAN, "Buffer must have BIG ENDIAN byte ordering");

      assert buffer != null;

      this.buffer = buffer;
   }

   protected abstract void reBuffer() throws IOException;

   public void readFully(byte[] b) throws IOException {
      this.readFully(b, 0, b.length);
   }

   public void readFully(byte[] b, int off, int len) throws IOException {
      int read = this.read(b, off, len);
      if(read < len) {
         throw new EOFException("EOF after " + read + " bytes out of " + len);
      }
   }

   public int read(byte[] b, int off, int len) throws IOException {
      if(off >= 0 && off <= b.length && len >= 0 && len <= b.length - off) {
         if(len == 0) {
            return 0;
         } else {
            int copied;
            int toCopy;
            for(copied = 0; copied < len; copied += toCopy) {
               int position = this.buffer.position();
               int remaining = this.buffer.limit() - position;
               if(remaining == 0) {
                  this.reBuffer();
                  position = this.buffer.position();
                  remaining = this.buffer.limit() - position;
                  if(remaining == 0) {
                     return copied == 0?-1:copied;
                  }
               }

               toCopy = Math.min(len - copied, remaining);
               FastByteOperations.copy(this.buffer, position, b, off + copied, toCopy);
               this.buffer.position(position + toCopy);
            }

            return copied;
         }
      } else {
         throw new IndexOutOfBoundsException();
      }
   }

   protected long readPrimitiveSlowly(int bytes) throws IOException {
      long result = 0L;

      for(int i = 0; i < bytes; ++i) {
         result = result << 8 | (long)this.readByte() & 255L;
      }

      return result;
   }

   public int skipBytes(int n) throws IOException {
      if(n <= 0) {
         return 0;
      } else {
         int requested = n;
         int position = this.buffer.position();
         int limit = this.buffer.limit();

         do {
            int remaining;
            if((remaining = limit - position) >= n) {
               this.buffer.position(position + n);
               return requested;
            }

            n -= remaining;
            this.buffer.position(limit);
            this.reBuffer();
            position = this.buffer.position();
            limit = this.buffer.limit();
         } while(position != limit);

         return requested - n;
      }
   }

   public boolean readBoolean() throws IOException {
      return this.readByte() != 0;
   }

   public byte readByte() throws IOException {
      assert this.buffer != null : "Got null buffer for " + this + " (" + this.getClass().getName() + ")";

      if(!this.buffer.hasRemaining()) {
         this.reBuffer();
         if(!this.buffer.hasRemaining()) {
            throw new EOFException();
         }
      }

      return this.buffer.get();
   }

   public int readUnsignedByte() throws IOException {
      return this.readByte() & 255;
   }

   public short readShort() throws IOException {
      return this.buffer.remaining() >= 2?this.buffer.getShort():(short)((int)this.readPrimitiveSlowly(2));
   }

   public int readUnsignedShort() throws IOException {
      return this.readShort() & '\uffff';
   }

   public char readChar() throws IOException {
      return this.buffer.remaining() >= 2?this.buffer.getChar():(char)((int)this.readPrimitiveSlowly(2));
   }

   public int readInt() throws IOException {
      return this.buffer.remaining() >= 4?this.buffer.getInt():(int)this.readPrimitiveSlowly(4);
   }

   public long readLong() throws IOException {
      return this.buffer.remaining() >= 8?this.buffer.getLong():this.readPrimitiveSlowly(8);
   }

   public long readVInt() throws IOException {
      return VIntCoding.decodeZigZag64(this.readUnsignedVInt());
   }

   public long readUnsignedVInt() throws IOException {
      if(this.buffer.remaining() < 9) {
         return VIntCoding.readUnsignedVInt(this);
      } else {
         byte firstByte = this.buffer.get();
         if(firstByte >= 0) {
            return (long)firstByte;
         } else {
            int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);
            int position = this.buffer.position();
            int extraBits = extraBytes * 8;
            long retval = UnsafeByteBufferAccess.getLong(this.buffer);
            if(this.buffer.order() == ByteOrder.LITTLE_ENDIAN) {
               retval = Long.reverseBytes(retval);
            }

            this.buffer.position(position + extraBytes);
            retval >>>= 64 - extraBits;
            firstByte = (byte)(firstByte & VIntCoding.firstByteValueMask(extraBytes));
            retval |= (long)firstByte << extraBits;
            return retval;
         }
      }
   }

   public float readFloat() throws IOException {
      return this.buffer.remaining() >= 4?this.buffer.getFloat():Float.intBitsToFloat((int)this.readPrimitiveSlowly(4));
   }

   public double readDouble() throws IOException {
      return this.buffer.remaining() >= 8?this.buffer.getDouble():Double.longBitsToDouble(this.readPrimitiveSlowly(8));
   }

   public String readLine() throws IOException {
      throw new UnsupportedOperationException();
   }

   public String readUTF() throws IOException {
      return DataInputStream.readUTF(this);
   }

   public int read() throws IOException {
      try {
         return this.readUnsignedByte();
      } catch (EOFException var2) {
         return -1;
      }
   }
}
