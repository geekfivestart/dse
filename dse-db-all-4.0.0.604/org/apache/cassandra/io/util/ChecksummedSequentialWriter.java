package org.apache.cassandra.io.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.cassandra.io.compress.BufferType;

public class ChecksummedSequentialWriter extends SequentialWriter {
   private static final SequentialWriterOption CRC_WRITER_OPTION;
   private final SequentialWriter crcWriter;
   private final ChecksumWriter crcMetadata;
   private final Optional<File> digestFile;

   public ChecksummedSequentialWriter(File file, File crcPath, File digestFile, SequentialWriterOption option) {
      super(file, option);
      this.crcWriter = new SequentialWriter(crcPath, CRC_WRITER_OPTION);
      this.crcMetadata = new ChecksumWriter(this.crcWriter);
      this.crcMetadata.writeChunkSize(this.buffer.capacity());
      this.digestFile = Optional.ofNullable(digestFile);
   }

   protected void flushData() {
      super.flushData();
      ByteBuffer toAppend = this.buffer.duplicate();
      toAppend.position(0);
      toAppend.limit(this.buffer.position());
      this.crcMetadata.appendDirect(toAppend, false);
   }

   protected SequentialWriter.TransactionalProxy txnProxy() {
      return new ChecksummedSequentialWriter.TransactionalProxy();
   }

   static {
      CRC_WRITER_OPTION = SequentialWriterOption.newBuilder().bufferSize(8192).bufferType(BufferType.OFF_HEAP).build();
   }

   protected class TransactionalProxy extends SequentialWriter.TransactionalProxy {
      protected TransactionalProxy() {
         super();
      }

      protected Throwable doCommit(Throwable accumulate) {
         return super.doCommit(ChecksummedSequentialWriter.this.crcWriter.commit(accumulate));
      }

      protected Throwable doAbort(Throwable accumulate) {
         return super.doAbort(ChecksummedSequentialWriter.this.crcWriter.abort(accumulate));
      }

      protected void doPrepare() {
         ChecksummedSequentialWriter.this.syncInternal();
         Optional var10000 = ChecksummedSequentialWriter.this.digestFile;
         ChecksumWriter var10001 = ChecksummedSequentialWriter.this.crcMetadata;
         var10000.ifPresent(var10001::writeFullChecksum);
         ChecksummedSequentialWriter.this.crcWriter.prepareToCommit();
      }
   }
}
