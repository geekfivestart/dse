package org.apache.cassandra.io.util;

import com.google.common.base.Preconditions;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.vint.VIntCoding;

public class BufferedDataOutputStreamPlus extends DataOutputStreamPlus {
   private static final int DEFAULT_BUFFER_SIZE = Integer.getInteger("cassandra.nio_data_output_stream_plus_buffer_size", '耀').intValue();
   protected ByteBuffer buffer;
   protected boolean strictFlushing;
   private final ByteBuffer hollowBuffer;

   public BufferedDataOutputStreamPlus(RandomAccessFile ras) {
      this((WritableByteChannel)ras.getChannel());
   }

   public BufferedDataOutputStreamPlus(RandomAccessFile ras, int bufferSize) {
      this((WritableByteChannel)ras.getChannel(), bufferSize);
   }

   public BufferedDataOutputStreamPlus(FileOutputStream fos) {
      this((WritableByteChannel)fos.getChannel());
   }

   public BufferedDataOutputStreamPlus(FileOutputStream fos, int bufferSize) {
      this((WritableByteChannel)fos.getChannel(), bufferSize);
   }

   public BufferedDataOutputStreamPlus(WritableByteChannel wbc) {
      this(wbc, DEFAULT_BUFFER_SIZE);
   }

   public BufferedDataOutputStreamPlus(WritableByteChannel wbc, int bufferSize) {
      this(wbc, ByteBuffer.allocateDirect(bufferSize));
      Objects.requireNonNull(wbc);
      Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accommodate a long/double");
   }

   protected BufferedDataOutputStreamPlus(WritableByteChannel channel, ByteBuffer buffer) {
      super(channel);
      this.strictFlushing = false;
      this.hollowBuffer = UnsafeByteBufferAccess.getHollowDirectByteBuffer();
      this.buffer = buffer;
   }

   protected BufferedDataOutputStreamPlus(ByteBuffer buffer) {
      this.strictFlushing = false;
      this.hollowBuffer = UnsafeByteBufferAccess.getHollowDirectByteBuffer();
      this.buffer = buffer;
   }

   public void write(byte[] b) throws IOException {
      this.write(b, 0, b.length);
   }

   public void write(byte[] b, int off, int len) throws IOException {
      if(len != 0) {
         if(this.buffer.remaining() >= len) {
            this.buffer.put(b, off, len);
         } else {
            int copied = 0;

            do {
               int remaining = this.buffer.remaining();
               if(remaining == 0) {
                  this.doFlush(len - copied);
                  remaining = this.buffer.remaining();
               }

               int toCopy = Math.min(len - copied, remaining);
               this.buffer.put(b, off + copied, toCopy);
               copied += toCopy;
            } while(copied != len);

         }
      }
   }

   public void write(ByteBuffer toWrite) throws IOException {
      if(toWrite.hasArray()) {
         this.write(toWrite.array(), toWrite.arrayOffset() + toWrite.position(), toWrite.remaining());
      } else {
         assert toWrite.isDirect();

         UnsafeByteBufferAccess.duplicateDirectByteBuffer(toWrite, this.hollowBuffer);
         int toWriteRemaining = toWrite.remaining();
         if(toWriteRemaining > this.buffer.remaining()) {
            if(this.strictFlushing) {
               this.writeExcessSlow();
            } else {
               this.doFlush(toWriteRemaining - this.buffer.remaining());

               while(this.hollowBuffer.remaining() > this.buffer.capacity()) {
                  this.channel.write(this.hollowBuffer);
               }
            }
         }

         this.buffer.put(this.hollowBuffer);
      }

   }

   private void writeExcessSlow() throws IOException {
      int originalLimit = this.hollowBuffer.limit();

      while(originalLimit - this.hollowBuffer.position() > this.buffer.remaining()) {
         this.hollowBuffer.limit(this.hollowBuffer.position() + this.buffer.remaining());
         this.buffer.put(this.hollowBuffer);
         this.doFlush(originalLimit - this.hollowBuffer.position());
      }

      this.hollowBuffer.limit(originalLimit);
   }

   public void write(int b) throws IOException {
      if(!this.buffer.hasRemaining()) {
         this.doFlush(1);
      }

      this.buffer.put((byte)b);
   }

   public void writeBoolean(boolean v) throws IOException {
      if(!this.buffer.hasRemaining()) {
         this.doFlush(1);
      }

      this.buffer.put((byte)(v?1:0));
   }

   public void writeByte(int v) throws IOException {
      this.write(v);
   }

   public void writeShort(int v) throws IOException {
      this.writeChar(v);
   }

   public void writeChar(int v) throws IOException {
      if(this.buffer.remaining() < 2) {
         this.writeSlow((long)v, 2);
      } else {
         this.buffer.putChar((char)v);
      }

   }

   public void writeInt(int v) throws IOException {
      if(this.buffer.remaining() < 4) {
         this.writeSlow((long)v, 4);
      } else {
         this.buffer.putInt(v);
      }

   }

   public void writeLong(long v) throws IOException {
      if(this.buffer.remaining() < 8) {
         this.writeSlow(v, 8);
      } else {
         this.buffer.putLong(v);
      }

   }

   public void writeVInt(long value) throws IOException {
      VIntCoding.writeVInt(value, this);
   }

   public void writeUnsignedVInt(long value) throws IOException {
      VIntCoding.writeUnsignedVInt(value, this);
   }

   public void writeFloat(float v) throws IOException {
      this.writeInt(Float.floatToRawIntBits(v));
   }

   public void writeDouble(double v) throws IOException {
      this.writeLong(Double.doubleToRawLongBits(v));
   }

   private void writeSlow(long bytes, int count) throws IOException {
      int origCount = count;
      if(ByteOrder.BIG_ENDIAN == this.buffer.order()) {
         while(count > 0) {
            --count;
            this.writeByte((int)(bytes >>> 8 * count));
         }
      } else {
         while(count > 0) {
            this.writeByte((int)(bytes >>> 8 * (origCount - count--)));
         }
      }

   }

   public void writeBytes(String s) throws IOException {
      for(int index = 0; index < s.length(); ++index) {
         this.writeByte(s.charAt(index));
      }

   }

   public void writeChars(String s) throws IOException {
      for(int index = 0; index < s.length(); ++index) {
         this.writeChar(s.charAt(index));
      }

   }

   public void writeUTF(String s) throws IOException {
      UnbufferedDataOutputStreamPlus.writeUTF(s, this);
   }

   public void write(Memory memory, long offset, long length) throws IOException {
      ByteBuffer[] var6 = memory.asByteBuffers(offset, length);
      int var7 = var6.length;

      for(int var8 = 0; var8 < var7; ++var8) {
         ByteBuffer buffer = var6[var8];
         this.write(buffer);
      }

   }

   protected void doFlush(int count) throws IOException {
      this.buffer.flip();

      while(this.buffer.hasRemaining()) {
         this.channel.write(this.buffer);
      }

      this.buffer.clear();
   }

   public void flush() throws IOException {
      this.doFlush(0);
   }

   public void close() throws IOException {
      this.doFlush(0);
      this.channel.close();
      FileUtils.clean(this.buffer);
      this.buffer = null;
   }

   public <R> R applyToChannel(ThrowingFunction<WritableByteChannel, R> f) throws IOException {
      if(this.strictFlushing) {
         throw new UnsupportedOperationException();
      } else {
         this.flush();
         return f.apply(this.channel);
      }
   }

   public BufferedDataOutputStreamPlus order(ByteOrder order) {
      this.buffer.order(order);
      return this;
   }
}
