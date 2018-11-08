package org.apache.cassandra.io.util;

import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.cassandra.io.compress.BufferType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomAccessReader extends RebufferingInputStream implements FileDataInput {
   private static final Logger logger = LoggerFactory.getLogger(RandomAccessReader.class);
   public static final int DEFAULT_BUFFER_SIZE = 4096;
   private long markedPointer;
   final Rebufferer rebufferer;
   private Rebufferer.BufferHolder bufferHolder;
   private final Rebufferer.ReaderConstraint constraint;

   RandomAccessReader(Rebufferer rebufferer, Rebufferer.ReaderConstraint rc) {
      super(Rebufferer.EMPTY.buffer());
      this.bufferHolder = Rebufferer.EMPTY;
      this.rebufferer = rebufferer;
      this.constraint = rc;
   }

   public void reBuffer() {
      if(!this.isEOF()) {
         this.reBufferAt(this.current());
      }
   }

   private void reBufferAt(long position) {
      Rebufferer.BufferHolder prevBufferHolder = this.bufferHolder;
      this.bufferHolder = this.rebufferer.rebuffer(position, this.constraint);
      prevBufferHolder.release();
      this.buffer = this.bufferHolder.buffer();
      this.buffer.position(Ints.checkedCast(position - this.bufferHolder.offset()));

      assert this.buffer.order() == ByteOrder.BIG_ENDIAN : "Buffer must have BIG ENDIAN byte ordering";

   }

   public long getFilePointer() {
      return this.buffer == null?this.rebufferer.fileLength():this.current();
   }

   protected long current() {
      return this.bufferHolder.offset() + (long)this.buffer.position();
   }

   public String getPath() {
      return this.getChannel().filePath();
   }

   public AsynchronousChannelProxy getChannel() {
      return this.rebufferer.channel();
   }

   public void reset() throws IOException {
      this.seek(this.markedPointer);
   }

   public boolean markSupported() {
      return true;
   }

   public long bytesPastMark() {
      long bytes = this.current() - this.markedPointer;

      assert bytes >= 0L;

      return bytes;
   }

   public DataPosition mark() {
      this.markedPointer = this.current();
      return new RandomAccessReader.BufferedRandomAccessFileMark(this.markedPointer);
   }

   public void reset(DataPosition mark) {
      assert mark instanceof RandomAccessReader.BufferedRandomAccessFileMark;

      this.seek(((RandomAccessReader.BufferedRandomAccessFileMark)mark).pointer);
   }

   public long bytesPastMark(DataPosition mark) {
      assert mark instanceof RandomAccessReader.BufferedRandomAccessFileMark;

      long bytes = this.current() - ((RandomAccessReader.BufferedRandomAccessFileMark)mark).pointer;

      assert bytes >= 0L : bytes;

      return bytes;
   }

   public boolean isEOF() {
      return this.current() == this.length();
   }

   public long bytesRemaining() {
      return this.length() - this.getFilePointer();
   }

   public int available() throws IOException {
      return Ints.saturatedCast(this.bytesRemaining());
   }

   public void close() {
      if(this.buffer != null) {
         this.bufferHolder.release();
         this.rebufferer.closeReader();
         this.buffer = null;
         this.bufferHolder = null;
      }
   }

   public String toString() {
      return this.getClass().getSimpleName() + ':' + this.rebufferer;
   }

   public void seek(long newPosition) {
      if(newPosition < 0L) {
         throw new IllegalArgumentException("new position should not be negative");
      } else if(this.buffer == null) {
         throw new IllegalStateException("Attempted to seek in a closed RAR");
      } else {
         long bufferOffset = this.bufferHolder.offset();
         if(newPosition >= bufferOffset && newPosition < bufferOffset + (long)this.buffer.limit()) {
            this.buffer.position((int)(newPosition - bufferOffset));
         } else if(newPosition > this.length()) {
            throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode", new Object[]{Long.valueOf(newPosition), this.getPath(), Long.valueOf(this.length())}));
         } else {
            this.reBufferAt(newPosition);
         }
      }
   }

   public final String readLine() throws IOException {
      StringBuilder line = new StringBuilder(80);
      boolean foundTerminator = false;
      long unreadPosition = -1L;

      while(true) {
         int nextByte = this.read();
         switch(nextByte) {
         case -1:
            return line.length() != 0?line.toString():null;
         case 10:
            return line.toString();
         case 13:
            if(foundTerminator) {
               this.seek(unreadPosition);
               return line.toString();
            }

            foundTerminator = true;
            unreadPosition = this.getPosition();
            break;
         default:
            if(foundTerminator) {
               this.seek(unreadPosition);
               return line.toString();
            }

            line.append((char)nextByte);
         }
      }
   }

   public long length() {
      return this.rebufferer.fileLength();
   }

   public long getPosition() {
      return this.current();
   }

   public double getCrcCheckChance() {
      return this.rebufferer.getCrcCheckChance();
   }

   public static RandomAccessReader open(File file) {
      AsynchronousChannelProxy channel = new AsynchronousChannelProxy(file, true);

      try {
         ChunkReader reader = new SimpleChunkReader(channel, -1L, BufferType.OFF_HEAP, 4096);
         Rebufferer rebufferer = reader.instantiateRebufferer();
         return new RandomAccessReader.RandomAccessReaderWithOwnChannel(rebufferer);
      } catch (Throwable var4) {
         channel.close();
         throw var4;
      }
   }

   static class RandomAccessReaderWithOwnChannel extends RandomAccessReader {
      RandomAccessReaderWithOwnChannel(Rebufferer rebufferer) {
         super(rebufferer, Rebufferer.ReaderConstraint.NONE);
      }

      public void close() {
         try {
            super.close();
         } finally {
            try {
               this.rebufferer.close();
            } finally {
               this.getChannel().close();
            }
         }

      }
   }

   private static class BufferedRandomAccessFileMark implements DataPosition {
      final long pointer;

      private BufferedRandomAccessFileMark(long pointer) {
         this.pointer = pointer;
      }
   }
}
