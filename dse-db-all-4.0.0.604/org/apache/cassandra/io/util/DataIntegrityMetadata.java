package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Throwables;

public class DataIntegrityMetadata {
   public DataIntegrityMetadata() {
   }

   public static DataIntegrityMetadata.ChecksumValidator checksumValidator(Descriptor desc) throws IOException {
      return new DataIntegrityMetadata.ChecksumValidator(desc);
   }

   public static DataIntegrityMetadata.FileDigestValidator fileDigestValidator(Descriptor desc) throws IOException {
      return new DataIntegrityMetadata.FileDigestValidator(desc);
   }

   public static class FileDigestValidator implements Closeable {
      private final Checksum checksum;
      private final RandomAccessReader digestReader;
      private final RandomAccessReader dataReader;
      private final Descriptor descriptor;
      private long storedDigestValue;

      public FileDigestValidator(Descriptor descriptor) throws IOException {
         this.descriptor = descriptor;
         this.checksum = ChecksumType.newCRC32();
         this.digestReader = RandomAccessReader.open(new File(descriptor.filenameFor(Component.DIGEST)));
         this.dataReader = RandomAccessReader.open(new File(descriptor.filenameFor(Component.DATA)));

         try {
            this.storedDigestValue = Long.parseLong(this.digestReader.readLine());
         } catch (Exception var3) {
            this.close();
            throw new IOException("Corrupted SSTable : " + descriptor.filenameFor(Component.DATA));
         }
      }

      public void validate() throws IOException {
         CheckedInputStream checkedInputStream = new CheckedInputStream(this.dataReader, this.checksum);
         byte[] chunk = new byte[65536];

         while(checkedInputStream.read(chunk) > 0) {
            ;
         }

         long calculatedDigestValue = checkedInputStream.getChecksum().getValue();
         if(this.storedDigestValue != calculatedDigestValue) {
            throw new IOException("Corrupted SSTable : " + this.descriptor.filenameFor(Component.DATA));
         }
      }

      public void close() {
         Throwables.DiscreteAction[] var10000 = new Throwables.DiscreteAction[2];
         RandomAccessReader var10003 = this.digestReader;
         this.digestReader.getClass();
         var10000[0] = var10003::close;
         var10003 = this.dataReader;
         this.dataReader.getClass();
         var10000[1] = var10003::close;
         Throwables.perform(var10000);
      }
   }

   public static class ChecksumValidator implements Closeable {
      private final ChecksumType checksumType;
      private final RandomAccessReader reader;
      public final int chunkSize;
      private final String dataFilename;

      public ChecksumValidator(Descriptor descriptor) throws IOException {
         this(ChecksumType.CRC32, RandomAccessReader.open(new File(descriptor.filenameFor(Component.CRC))), descriptor.filenameFor(Component.DATA));
      }

      public ChecksumValidator(ChecksumType checksumType, RandomAccessReader reader, String dataFilename) throws IOException {
         this.checksumType = checksumType;
         this.reader = reader;
         this.dataFilename = dataFilename;
         this.chunkSize = reader.readInt();
      }

      public void seek(long offset) {
         long start = this.chunkStart(offset);
         this.reader.seek(start / (long)this.chunkSize * 4L + 4L);
      }

      public long getFilePointer() {
         return this.reader.getFilePointer();
      }

      public long chunkStart(long offset) {
         long startChunk = offset / (long)this.chunkSize;
         return startChunk * (long)this.chunkSize;
      }

      public void validate(byte[] bytes, int start, int end) throws IOException {
         int current = (int)this.checksumType.of(bytes, start, end);
         int actual = this.reader.readInt();
         if(current != actual) {
            throw new IOException("Corrupted File : " + this.dataFilename);
         }
      }

      public void close() {
         this.reader.close();
      }
   }
}
