package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

public interface ICompressor {
   int initialCompressedBufferLength(int var1);

   int uncompress(byte[] var1, int var2, int var3, byte[] var4, int var5) throws IOException;

   void compress(ByteBuffer var1, ByteBuffer var2) throws IOException;

   void uncompress(ByteBuffer var1, ByteBuffer var2) throws IOException;

   BufferType preferredBufferType();

   boolean supports(BufferType var1);

   Set<String> supportedOptions();
}
