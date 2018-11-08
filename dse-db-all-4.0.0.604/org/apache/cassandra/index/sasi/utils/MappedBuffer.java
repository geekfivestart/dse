package org.apache.cassandra.index.sasi.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;

public class MappedBuffer implements Closeable {
   private final MappedByteBuffer[] pages;
   private boolean isClosed;
   private final String path;
   private long position;
   private long limit;
   private final long capacity;
   private final int pageSize;
   private final int sizeBits;

   private MappedBuffer(MappedBuffer other) {
      this.isClosed = false;
      this.sizeBits = other.sizeBits;
      this.pageSize = other.pageSize;
      this.position = other.position;
      this.limit = other.limit;
      this.capacity = other.capacity;
      this.pages = other.pages;
      this.path = other.path;
   }

   public MappedBuffer(ChannelProxy file) {
      this(file, 30);
   }

   @VisibleForTesting
   protected MappedBuffer(ChannelProxy file, int numPageBits) {
      this.isClosed = false;
      if(numPageBits > 31) {
         throw new IllegalArgumentException("page size can't be bigger than 1G");
      } else {
         this.sizeBits = numPageBits;
         this.pageSize = 1 << this.sizeBits;
         this.position = 0L;
         this.limit = this.capacity = file.size();
         this.pages = new MappedByteBuffer[(int)(file.size() / (long)this.pageSize) + 1];

         try {
            long offset = 0L;

            for(int i = 0; i < this.pages.length; ++i) {
               long pageSize = Math.min((long)this.pageSize, this.capacity - offset);
               this.pages[i] = file.map(MapMode.READ_ONLY, offset, pageSize);
               offset += pageSize;
            }
         } finally {
            file.close();
         }

         this.path = file.filePath();
      }
   }

   public int comparePageTo(long offset, int length, AbstractType<?> comparator, ByteBuffer other) {
      return comparator.compare(this.getPageRegion(offset, length), other);
   }

   public long capacity() {
      return this.capacity;
   }

   public long position() {
      return this.position;
   }

   public MappedBuffer position(long newPosition) {
      if(newPosition >= 0L && newPosition <= this.limit) {
         this.position = newPosition;
         return this;
      } else {
         throw new IllegalArgumentException("position: " + newPosition + ", limit: " + this.limit);
      }
   }

   public long limit() {
      return this.limit;
   }

   public MappedBuffer limit(long newLimit) {
      if(newLimit >= this.position && newLimit <= this.capacity) {
         this.limit = newLimit;
         return this;
      } else {
         throw new IllegalArgumentException();
      }
   }

   public long remaining() {
      return this.limit - this.position;
   }

   public boolean hasRemaining() {
      return this.remaining() > 0L;
   }

   public byte get() {
      return this.get((long)(this.position++));
   }

   public byte get(long pos) {
      return this.getPage(pos).get(this.getPageOffset(pos));
   }

   public short getShort() {
      short value = this.getShort(this.position);
      this.position += 2L;
      return value;
   }

   public short getShort(long pos) {
      if(this.isPageAligned(pos, 2)) {
         return this.getPage(pos).getShort(this.getPageOffset(pos));
      } else {
         int ch1 = this.get(pos) & 255;
         int ch2 = this.get(pos + 1L) & 255;
         return (short)((ch1 << 8) + ch2);
      }
   }

   public int getInt() {
      int value = this.getInt(this.position);
      this.position += 4L;
      return value;
   }

   public int getInt(long pos) {
      if(this.isPageAligned(pos, 4)) {
         return this.getPage(pos).getInt(this.getPageOffset(pos));
      } else {
         int ch1 = this.get(pos) & 255;
         int ch2 = this.get(pos + 1L) & 255;
         int ch3 = this.get(pos + 2L) & 255;
         int ch4 = this.get(pos + 3L) & 255;
         return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
      }
   }

   public long getLong() {
      long value = this.getLong(this.position);
      this.position += 8L;
      return value;
   }

   public long getLong(long pos) {
      return this.isPageAligned(pos, 8)?this.getPage(pos).getLong(this.getPageOffset(pos)):((long)this.getInt(pos) << 32) + ((long)this.getInt(pos + 4L) & 4294967295L);
   }

   public ByteBuffer getPageRegion(long position, int length) {
      if(!this.isPageAligned(position, length)) {
         throw new IllegalArgumentException(String.format("range: %s-%s wraps more than one page", new Object[]{Long.valueOf(position), Integer.valueOf(length)}));
      } else {
         ByteBuffer slice = this.getPage(position).duplicate();
         int pageOffset = this.getPageOffset(position);
         slice.position(pageOffset).limit(pageOffset + length);
         return slice;
      }
   }

   public MappedBuffer duplicate() {
      return new MappedBuffer(this);
   }

   public void close() {
      this.isClosed = true;
      if(FileUtils.isCleanerAvailable) {
         try {
            MappedByteBuffer[] var1 = this.pages;
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               MappedByteBuffer segment = var1[var3];
               FileUtils.clean(segment);
            }
         } catch (Exception var5) {
            ;
         }

      }
   }

   private MappedByteBuffer getPage(long position) {
      int page = (int)(position >> this.sizeBits);
      MappedByteBuffer buffer = this.pages[page];

      assert !this.isClosed : "Page holding the position " + position + " is already unloaded in mmaped file: '" + this.path + "'";

      return buffer;
   }

   private int getPageOffset(long position) {
      return (int)(position & (long)(this.pageSize - 1));
   }

   private boolean isPageAligned(long position, int length) {
      return this.pageSize - (this.getPageOffset(position) + length) > 0;
   }
}
