package org.apache.cassandra.io.util;

import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class DataOutputStreamPlus extends OutputStream implements DataOutputPlus {
   protected final WritableByteChannel channel;
   private static int MAX_BUFFER_SIZE = Integer.getInteger("cassandra.data_output_stream_plus_temp_buffer_size", 8192).intValue();
   private static final FastThreadLocal<byte[]> tempBuffer = new FastThreadLocal<byte[]>() {
      public byte[] initialValue() {
         return new byte[16];
      }
   };

   protected DataOutputStreamPlus() {
      this.channel = this.newDefaultChannel();
   }

   protected DataOutputStreamPlus(WritableByteChannel channel) {
      this.channel = channel;
   }

   protected static byte[] retrieveTemporaryBuffer(int minSize) {
      byte[] bytes = (byte[])tempBuffer.get();
      if(bytes.length < Math.min(minSize, MAX_BUFFER_SIZE)) {
         bytes = new byte[Math.min(MAX_BUFFER_SIZE, 2 * Integer.highestOneBit(minSize))];
         tempBuffer.set(bytes);
      }

      return bytes;
   }

   protected WritableByteChannel newDefaultChannel() {
      return new WritableByteChannel() {
         public boolean isOpen() {
            return true;
         }

         public void close() {
         }

         public int write(ByteBuffer src) throws IOException {
            int toWrite = src.remaining();
            if(src.hasArray()) {
               DataOutputStreamPlus.this.write(src.array(), src.arrayOffset() + src.position(), src.remaining());
               src.position(src.limit());
               return toWrite;
            } else {
               int totalWritten;
               if(toWrite < 16) {
                  int offset = src.position();

                  for(totalWritten = 0; totalWritten < toWrite; ++totalWritten) {
                     DataOutputStreamPlus.this.write(src.get(totalWritten + offset));
                  }

                  src.position(src.limit());
                  return toWrite;
               } else {
                  byte[] buf = DataOutputStreamPlus.retrieveTemporaryBuffer(toWrite);

                  int toWriteThisTime;
                  for(totalWritten = 0; totalWritten < toWrite; totalWritten += toWriteThisTime) {
                     toWriteThisTime = Math.min(buf.length, toWrite - totalWritten);
                     ByteBufferUtil.arrayCopy(src, src.position() + totalWritten, (byte[])buf, 0, toWriteThisTime);
                     DataOutputStreamPlus.this.write(buf, 0, toWriteThisTime);
                  }

                  src.position(src.limit());
                  return totalWritten;
               }
            }
         }
      };
   }
}
