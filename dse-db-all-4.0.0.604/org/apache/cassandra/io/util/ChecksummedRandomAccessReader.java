package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import org.apache.cassandra.utils.ChecksumType;

public final class ChecksummedRandomAccessReader {
   public ChecksummedRandomAccessReader() {
   }

   public static RandomAccessReader open(File file, File crcFile) throws IOException {
      AsynchronousChannelProxy channel = new AsynchronousChannelProxy(file, true);

      try {
         DataIntegrityMetadata.ChecksumValidator validator = new DataIntegrityMetadata.ChecksumValidator(ChecksumType.CRC32, RandomAccessReader.open(crcFile), file.getPath());
         Rebufferer rebufferer = new ChecksummedRebufferer(channel, validator);
         return new RandomAccessReader.RandomAccessReaderWithOwnChannel(rebufferer);
      } catch (Throwable var5) {
         channel.close();
         throw var5;
      }
   }
}
