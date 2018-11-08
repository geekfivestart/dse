package org.apache.cassandra.io.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

public abstract class UnbufferedDataOutputStreamPlus extends DataOutputStreamPlus {
   private static final byte[] zeroBytes = new byte[2];
   private final ByteBuffer hollowBufferD = UnsafeByteBufferAccess.getHollowDirectByteBuffer();

   protected UnbufferedDataOutputStreamPlus() {
   }

   protected UnbufferedDataOutputStreamPlus(WritableByteChannel channel) {
      super(channel);
   }

   public void write(byte[] buffer) throws IOException {
      this.write(buffer, 0, buffer.length);
   }

   public abstract void write(byte[] var1, int var2, int var3) throws IOException;

   public abstract void write(int var1) throws IOException;

   public final void writeBoolean(boolean val) throws IOException {
      this.write(val?1:0);
   }

   public final void writeByte(int val) throws IOException {
      this.write(val & 255);
   }

   public final void writeBytes(String str) throws IOException {
      byte[] bytes = new byte[str.length()];

      for(int index = 0; index < str.length(); ++index) {
         bytes[index] = (byte)(str.charAt(index) & 255);
      }

      this.write(bytes);
   }

   public final void writeChar(int val) throws IOException {
      this.write(val >>> 8 & 255);
      this.write(val >>> 0 & 255);
   }

   public final void writeChars(String str) throws IOException {
      byte[] newBytes = new byte[str.length() * 2];

      for(int index = 0; index < str.length(); ++index) {
         int newIndex = index == 0?index:index * 2;
         newBytes[newIndex] = (byte)(str.charAt(index) >> 8 & 255);
         newBytes[newIndex + 1] = (byte)(str.charAt(index) & 255);
      }

      this.write(newBytes);
   }

   public final void writeDouble(double val) throws IOException {
      this.writeLong(Double.doubleToLongBits(val));
   }

   public final void writeFloat(float val) throws IOException {
      this.writeInt(Float.floatToIntBits(val));
   }

   public void writeInt(int val) throws IOException {
      this.write(val >>> 24 & 255);
      this.write(val >>> 16 & 255);
      this.write(val >>> 8 & 255);
      this.write(val >>> 0 & 255);
   }

   public void writeLong(long val) throws IOException {
      this.write((int)(val >>> 56) & 255);
      this.write((int)(val >>> 48) & 255);
      this.write((int)(val >>> 40) & 255);
      this.write((int)(val >>> 32) & 255);
      this.write((int)(val >>> 24) & 255);
      this.write((int)(val >>> 16) & 255);
      this.write((int)(val >>> 8) & 255);
      this.write((int)(val >>> 0) & 255);
   }

   public void writeShort(int val) throws IOException {
      this.writeChar(val);
   }

   public static void writeUTF(String str, DataOutput out) throws IOException {
      int length = str.length();
      if(length == 0) {
         out.write(zeroBytes);
      } else {
         int utfCount = 0;
         int maxSize = 2;

         for(int i = 0; i < length; ++i) {
            int ch = str.charAt(i);
            if(ch > 0 & ch <= 127) {
               ++utfCount;
            } else if(ch <= 2047) {
               utfCount += 2;
            } else {
               maxSize = 3;
               utfCount += 3;
            }
         }

         if(utfCount > '\uffff') {
            throw new UTFDataFormatException();
         } else {
            byte[] utfBytes = retrieveTemporaryBuffer(utfCount + 2);
            int bufferLength = utfBytes.length;
            int offset;
            int charRunLength;
            int i;
            if(utfCount == length) {
               utfBytes[0] = (byte)(utfCount >> 8);
               utfBytes[1] = (byte)utfCount;
               int firstIndex = 2;

               for(offset = 0; offset < length; offset += bufferLength) {
                  charRunLength = Math.min(bufferLength - firstIndex, length - offset) + firstIndex;
                  offset -= firstIndex;

                  for(i = firstIndex; i < charRunLength; ++i) {
                     utfBytes[i] = (byte)str.charAt(offset + i);
                  }

                  out.write(utfBytes, 0, charRunLength);
                  firstIndex = 0;
               }
            } else {
               int utfIndex = 2;
               offset = 0;
               utfBytes[0] = (byte)(utfCount >> 8);

               for(utfBytes[1] = (byte)utfCount; length > 0; length -= charRunLength) {
                  charRunLength = (utfBytes.length - utfIndex) / maxSize;
                  if(charRunLength < 128 && charRunLength < length) {
                     out.write(utfBytes, 0, utfIndex);
                     utfIndex = 0;
                  }

                  if(charRunLength > length) {
                     charRunLength = length;
                  }

                  for(i = 0; i < charRunLength; ++i) {
                     char ch = str.charAt(offset + i);
                     if(ch > 0 && ch <= 127) {
                        utfBytes[utfIndex++] = (byte)ch;
                     } else if(ch <= 2047) {
                        utfBytes[utfIndex++] = (byte)(192 | 31 & ch >> 6);
                        utfBytes[utfIndex++] = (byte)(128 | 63 & ch);
                     } else {
                        utfBytes[utfIndex++] = (byte)(224 | 15 & ch >> 12);
                        utfBytes[utfIndex++] = (byte)(128 | 63 & ch >> 6);
                        utfBytes[utfIndex++] = (byte)(128 | 63 & ch);
                     }
                  }

                  offset += charRunLength;
               }

               out.write(utfBytes, 0, utfIndex);
            }

         }
      }
   }

   public final void writeUTF(String str) throws IOException {
      writeUTF(str, this);
   }

   public void write(ByteBuffer buf) throws IOException {
      if(buf.hasArray()) {
         this.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
      } else {
         assert buf.isDirect();

         UnsafeByteBufferAccess.duplicateDirectByteBuffer(buf, this.hollowBufferD);

         while(this.hollowBufferD.hasRemaining()) {
            this.channel.write(this.hollowBufferD);
         }
      }

   }

   public void write(Memory memory, long offset, long length) throws IOException {
      ByteBuffer[] var6 = memory.asByteBuffers(offset, length);
      int var7 = var6.length;

      for(int var8 = 0; var8 < var7; ++var8) {
         ByteBuffer buffer = var6[var8];
         this.write(buffer);
      }

   }

   public <R> R applyToChannel(ThrowingFunction<WritableByteChannel, R> f) throws IOException {
      return f.apply(this.channel);
   }
}
