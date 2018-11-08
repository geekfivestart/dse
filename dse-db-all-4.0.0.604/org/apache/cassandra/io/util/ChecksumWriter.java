package org.apache.cassandra.io.util;

import com.google.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;
import org.apache.cassandra.io.FSWriteError;

public class ChecksumWriter {
   private final CRC32 incrementalChecksum = new CRC32();
   private final DataOutput incrementalOut;
   private final CRC32 fullChecksum = new CRC32();

   public ChecksumWriter(DataOutput incrementalOut) {
      this.incrementalOut = incrementalOut;
   }

   public void writeChunkSize(int length) {
      try {
         this.incrementalOut.writeInt(length);
      } catch (IOException var3) {
         throw new IOError(var3);
      }
   }

   public void appendDirect(ByteBuffer bb, boolean checksumIncrementalResult) {
      try {
         ByteBuffer toAppend = bb.duplicate();
         toAppend.mark();
         this.incrementalChecksum.update(toAppend);
         toAppend.reset();
         int incrementalChecksumValue = (int)this.incrementalChecksum.getValue();
         this.incrementalOut.writeInt(incrementalChecksumValue);
         this.fullChecksum.update(toAppend);
         if(checksumIncrementalResult) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            byteBuffer.putInt(incrementalChecksumValue);

            assert byteBuffer.arrayOffset() == 0;

            this.fullChecksum.update(byteBuffer.array(), 0, byteBuffer.array().length);
         }

         this.incrementalChecksum.reset();
      } catch (IOException var6) {
         throw new IOError(var6);
      }
   }

   public void writeFullChecksum(@Nonnull File digestFile) {
      try {
         BufferedWriter out = Files.newBufferedWriter(digestFile.toPath(), Charsets.UTF_8, new OpenOption[0]);
         Throwable var3 = null;

         try {
            out.write(String.valueOf(this.fullChecksum.getValue()));
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if(out != null) {
               if(var3 != null) {
                  try {
                     out.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  out.close();
               }
            }

         }

      } catch (IOException var15) {
         throw new FSWriteError(var15, digestFile);
      }
   }
}
