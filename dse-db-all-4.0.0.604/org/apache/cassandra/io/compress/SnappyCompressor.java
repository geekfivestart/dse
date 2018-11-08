package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

public class SnappyCompressor implements ICompressor {
   public static final SnappyCompressor instance = new SnappyCompressor();
   private static Logger logger = LoggerFactory.getLogger(SnappyCompressor.class);

   public SnappyCompressor() {
   }

   public static SnappyCompressor create(Map<String, String> compressionOptions) {
      Snappy.getNativeLibraryVersion();
      return instance;
   }

   public static boolean isAvailable() {
      try {
         create(Collections.emptyMap());
         return true;
      } catch (Exception var1) {
         JVMStabilityInspector.inspectThrowable(var1);
         return false;
      } catch (SnappyError | UnsatisfiedLinkError | NoClassDefFoundError var2) {
         return false;
      }
   }

   public Set<String> supportedOptions() {
      return Collections.emptySet();
   }

   public int initialCompressedBufferLength(int chunkLength) {
      return Snappy.maxCompressedLength(chunkLength);
   }

   public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
      int dlimit = output.limit();
      Snappy.compress(input, output);
      output.position(output.limit());
      output.limit(dlimit);
      input.position(input.limit());
   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
      return Snappy.rawUncompress(input, inputOffset, inputLength, output, outputOffset);
   }

   public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
      int dlimit = output.limit();
      Snappy.uncompress(input, output);
      output.position(output.limit());
      output.limit(dlimit);
      input.position(input.limit());
   }

   public BufferType preferredBufferType() {
      return BufferType.OFF_HEAP;
   }

   public boolean supports(BufferType bufferType) {
      return bufferType != BufferType.ON_HEAP;
   }

   static {
      if(!isAvailable()) {
         logger.warn("Cannot initialize native Snappy library. Compression on new sstables will be disabled.");
      }

   }
}
