package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.cassandra.utils.vint.VIntCoding;

public interface DataInputPlus extends DataInput {
   default long readVInt() throws IOException {
      return VIntCoding.readVInt(this);
   }

   default long readUnsignedVInt() throws IOException {
      return VIntCoding.readUnsignedVInt(this);
   }

   int skipBytes(int var1) throws IOException;

   default void skipBytesFully(int n) throws IOException {
      int skipped = this.skipBytes(n);
      if(skipped != n) {
         throw new EOFException("EOF after " + skipped + " bytes out of " + n);
      }
   }

   public static class DataInputStreamPlus extends DataInputStream implements DataInputPlus {
      public DataInputStreamPlus(InputStream is) {
         super(is);
      }
   }
}
