package org.apache.cassandra.io.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

class ChecksummedRebufferer extends BufferManagingRebufferer {
   private final DataIntegrityMetadata.ChecksumValidator validator;

   ChecksummedRebufferer(AsynchronousChannelProxy channel, DataIntegrityMetadata.ChecksumValidator validator) {
      super(new SimpleChunkReader(channel, channel.size(), BufferType.OFF_HEAP, validator.chunkSize));
      this.validator = validator;
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      throw new UnsupportedOperationException("Not yet implemented");
   }

   public Rebufferer.BufferHolder rebuffer(long desiredPosition) {
      if(desiredPosition != this.validator.getFilePointer()) {
         this.validator.seek(desiredPosition);
      }

      Rebufferer.BufferHolder ret = this.newBufferHolder(this.alignedPosition(desiredPosition));

      try {
         this.source.readChunk(ret.offset(), ret.buffer()).join();
      } catch (CompletionException var6) {
         ret.release();
         if(var6.getCause() != null && var6.getCause() instanceof RuntimeException) {
            throw (RuntimeException)var6.getCause();
         }

         throw var6;
      }

      try {
         this.validator.validate(ByteBufferUtil.getArray(ret.buffer()), 0, ret.buffer().remaining());
         return ret;
      } catch (IOException var5) {
         ret.release();
         throw new CorruptFileException(var5, this.channel().filePath());
      }
   }

   public void close() {
      try {
         this.source.close();
      } finally {
         this.validator.close();
      }

   }

   long alignedPosition(long desiredPosition) {
      return desiredPosition / (long)this.source.chunkSize() * (long)this.source.chunkSize();
   }
}
