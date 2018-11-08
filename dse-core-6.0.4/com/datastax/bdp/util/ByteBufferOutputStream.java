package com.datastax.bdp.util;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ByteBufferOutputStream extends OutputStream {
   public static final int BUFFER_SIZE = 8192;
   private List<ByteBuffer> buffers;

   public ByteBufferOutputStream() {
      this.reset();
   }

   public List<ByteBuffer> getBufferList() {
      List<ByteBuffer> result = this.buffers;
      this.reset();
      Iterator var2 = result.iterator();

      while(var2.hasNext()) {
         ByteBuffer buffer = (ByteBuffer)var2.next();
         buffer.flip();
      }

      return result;
   }

   public ByteBuffer getByteBuffer() {
      List<ByteBuffer> list = this.getBufferList();
      if(list.size() == 1) {
         return (ByteBuffer)list.get(0);
      } else {
         int size = 0;

         ByteBuffer buffer;
         for(Iterator var3 = list.iterator(); var3.hasNext(); size += buffer.remaining()) {
            buffer = (ByteBuffer)var3.next();
         }

         ByteBuffer result = ByteBuffer.allocate(size);
         Iterator var7 = list.iterator();

         while(var7.hasNext()) {
            ByteBuffer buffer = (ByteBuffer)var7.next();
            result.put(buffer);
         }

         return (ByteBuffer)result.rewind();
      }
   }

   public void prepend(List<ByteBuffer> lists) {
      Iterator var2 = lists.iterator();

      while(var2.hasNext()) {
         ByteBuffer buffer = (ByteBuffer)var2.next();
         buffer.position(buffer.limit());
      }

      this.buffers.addAll(0, lists);
   }

   public void append(List<ByteBuffer> lists) {
      Iterator var2 = lists.iterator();

      while(var2.hasNext()) {
         ByteBuffer buffer = (ByteBuffer)var2.next();
         buffer.position(buffer.limit());
      }

      this.buffers.addAll(lists);
   }

   public void reset() {
      this.buffers = new LinkedList();
      this.buffers.add(ByteBuffer.allocate(8192));
   }

   private ByteBuffer getBufferWithCapacity(int capacity) {
      ByteBuffer buffer = (ByteBuffer)this.buffers.get(this.buffers.size() - 1);
      if(buffer.remaining() < capacity) {
         buffer = ByteBuffer.allocate(8192);
         this.buffers.add(buffer);
      }

      return buffer;
   }

   public void write(int b) {
      ByteBuffer buffer = this.getBufferWithCapacity(1);
      buffer.put((byte)b);
   }

   public void writeShort(short value) {
      ByteBuffer buffer = this.getBufferWithCapacity(2);
      buffer.putShort(value);
   }

   public void writeChar(char value) {
      ByteBuffer buffer = this.getBufferWithCapacity(2);
      buffer.putChar(value);
   }

   public void writeInt(int value) {
      ByteBuffer buffer = this.getBufferWithCapacity(4);
      buffer.putInt(value);
   }

   public void writeFloat(float value) {
      ByteBuffer buffer = this.getBufferWithCapacity(4);
      buffer.putFloat(value);
   }

   public void writeLong(long value) {
      ByteBuffer buffer = this.getBufferWithCapacity(8);
      buffer.putLong(value);
   }

   public void writeDouble(double value) {
      ByteBuffer buffer = this.getBufferWithCapacity(8);
      buffer.putDouble(value);
   }

   public void write(byte[] b, int off, int len) {
      ByteBuffer buffer = (ByteBuffer)this.buffers.get(this.buffers.size() - 1);

      for(int remaining = buffer.remaining(); len > remaining; remaining = buffer.remaining()) {
         buffer.put(b, off, remaining);
         len -= remaining;
         off += remaining;
         buffer = ByteBuffer.allocate(8192);
         this.buffers.add(buffer);
      }

      buffer.put(b, off, len);
   }

   public void write(ByteBuffer buffer) {
      if(buffer.remaining() < 8192) {
         this.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
         ByteBuffer dup = buffer.duplicate();
         dup.position(buffer.limit());
         this.buffers.add(dup);
      }

   }
}
