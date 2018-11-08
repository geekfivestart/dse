package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import javax.crypto.Cipher;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.utils.FBUtilities;

public class EncryptedHintsWriter extends HintsWriter {
   private final Cipher cipher;
   private final ICompressor compressor;
   private volatile ByteBuffer byteBuffer;

   protected EncryptedHintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC) {
      super(directory, descriptor, file, channel, fd, globalCRC);
      this.cipher = descriptor.getCipher();
      this.compressor = descriptor.createCompressor();
   }

   protected void writeBuffer(ByteBuffer input) throws IOException {
      this.byteBuffer = EncryptionUtils.compress(input, this.byteBuffer, true, this.compressor);
      ByteBuffer output = EncryptionUtils.encryptAndWrite(this.byteBuffer, this.channel, true, this.cipher);
      FBUtilities.updateChecksum(this.globalCRC, output);
   }

   @VisibleForTesting
   Cipher getCipher() {
      return this.cipher;
   }

   @VisibleForTesting
   ICompressor getCompressor() {
      return this.compressor;
   }
}
