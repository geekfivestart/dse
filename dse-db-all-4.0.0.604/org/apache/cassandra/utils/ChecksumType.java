package org.apache.cassandra.utils;

import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public enum ChecksumType {
   Adler32 {
      Checksum getTL() {
         return (Checksum)ChecksumType.Adler32_TL.get();
      }

      public void update(Checksum checksum, ByteBuffer buf) {
         ((Adler32)checksum).update(buf);
      }
   },
   CRC32 {
      Checksum getTL() {
         return (Checksum)ChecksumType.CRC32_TL.get();
      }

      public void update(Checksum checksum, ByteBuffer buf) {
         ((CRC32)checksum).update(buf);
      }
   };

   private static final FastThreadLocal<Checksum> CRC32_TL = new FastThreadLocal<Checksum>() {
      protected Checksum initialValue() throws Exception {
         return ChecksumType.newCRC32();
      }
   };
   private static final FastThreadLocal<Checksum> Adler32_TL = new FastThreadLocal<Checksum>() {
      protected Checksum initialValue() throws Exception {
         return ChecksumType.newAdler32();
      }
   };

   private ChecksumType() {
   }

   public static CRC32 newCRC32() {
      return new CRC32();
   }

   public static Adler32 newAdler32() {
      return new Adler32();
   }

   abstract Checksum getTL();

   public abstract void update(Checksum var1, ByteBuffer var2);

   public long of(ByteBuffer buf) {
      Checksum checksum = this.getTL();
      checksum.reset();
      this.update(checksum, buf);
      return checksum.getValue();
   }

   public long of(byte[] data, int off, int len) {
      Checksum checksum = this.getTL();
      checksum.reset();
      checksum.update(data, off, len);
      return checksum.getValue();
   }
}
