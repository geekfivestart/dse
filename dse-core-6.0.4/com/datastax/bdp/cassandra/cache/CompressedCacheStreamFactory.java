package com.datastax.bdp.cassandra.cache;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cache.AutoSavingCache.IStreamFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChecksummedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.streaming.messages.StreamMessage.StreamVersion;
import org.apache.cassandra.utils.Serializer;

public class CompressedCacheStreamFactory implements IStreamFactory {
   private final CompressionParams defaultParameters;
   private static final int EOF_MARKER = -1;
   private static volatile CompressionParams lastParameters = null;

   @VisibleForTesting
   public static CompressionParams getLastParameters() {
      return lastParameters != null?lastParameters.copy():null;
   }

   public CompressedCacheStreamFactory(CompressionParams defaultParameters) {
      this.defaultParameters = defaultParameters;
      lastParameters = defaultParameters;
   }

   static File getCompressionParametersFile(File file) {
      return new File(file.getAbsoluteFile() + "-compression-parameters");
   }

   static void writeCompressionParamters(File file, CompressionParams parameters) throws IOException {
      FileOutputStream outputStream = new FileOutputStream(file);
      Throwable var3 = null;

      try {
         DataOutputBuffer outputBuffer = new DataOutputBuffer();
         ((Serializer)CompressionParams.serializers.get(StreamVersion.OSS_30)).serialize(parameters, outputBuffer);
         outputStream.write(outputBuffer.getData());
         outputStream.close();
      } catch (Throwable var12) {
         var3 = var12;
         throw var12;
      } finally {
         if(outputStream != null) {
            if(var3 != null) {
               try {
                  outputStream.close();
               } catch (Throwable var11) {
                  var3.addSuppressed(var11);
               }
            } else {
               outputStream.close();
            }
         }

      }

   }

   static CompressionParams readCompressionParameters(File file) {
      try {
         FileInputStream inputStream = new FileInputStream(file);
         Throwable var2 = null;

         CompressionParams var3;
         try {
            var3 = (CompressionParams)((Serializer)CompressionParams.serializers.get(StreamVersion.OSS_30)).deserialize(new DataInputStreamPlus(inputStream));
         } catch (Throwable var13) {
            var2 = var13;
            throw var13;
         } finally {
            if(inputStream != null) {
               if(var2 != null) {
                  try {
                     inputStream.close();
                  } catch (Throwable var12) {
                     var2.addSuppressed(var12);
                  }
               } else {
                  inputStream.close();
               }
            }

         }

         return var3;
      } catch (IOException var15) {
         return null;
      }
   }

   CompressionParams getDefaultParameters() {
      return this.defaultParameters;
   }

   public InputStream getInputStream(File file, File crcPath) throws IOException {
      CompressionParams parameters = readCompressionParameters(getCompressionParametersFile(file));
      return (InputStream)(parameters == null?ChecksummedRandomAccessReader.open(file, crcPath):new CompressedCacheStreamFactory.CompressedInputStream(file, crcPath, parameters));
   }

   public OutputStream getOutputStream(File file, File crcPath) throws FileNotFoundException {
      return new CompressedCacheStreamFactory.CompressedOutputStream(file, crcPath, this.defaultParameters);
   }

   public static CompressedCacheStreamFactory create(Map<String, String> opts) throws ConfigurationException {
      CompressionParams parameters = CompressionParams.fromMap(opts);
      return new CompressedCacheStreamFactory(parameters);
   }

   static class CompressedOutputStream extends OutputStream {
      private final File file;
      private final DataOutputStream out;
      private final CompressionParams parameters;
      private final ByteBuffer buffer;
      private final int chunkLength;
      private final ICompressor compressor;
      private ByteBuffer compressedBuffer = null;

      CompressedOutputStream(File file, File crcPath, CompressionParams parameters) {
         this.file = file;
         this.parameters = parameters;
         SequentialWriterOption writerOptions = SequentialWriterOption.newBuilder().bufferSize(parameters.chunkLength()).finishOnClose(true).build();
         this.out = new DataOutputStream(new ChecksummedSequentialWriter(file, crcPath, (File)null, writerOptions));
         this.chunkLength = parameters.chunkLength();
         this.compressor = parameters.getSstableCompressor();
         this.buffer = this.compressor.preferredBufferType().allocate(parameters.chunkLength());
      }

      public void flush() throws IOException {
         if(this.buffer.position() > 0) {
            this.writeChunk();
         }

         super.flush();
      }

      public void close() throws IOException {
         this.flush();
         this.out.writeInt(-1);
         this.out.close();
         super.close();
         CompressedCacheStreamFactory.writeCompressionParamters(CompressedCacheStreamFactory.getCompressionParametersFile(this.file), this.parameters);
      }

      private void writeChunk() throws IOException {
         int uncompressedSize = this.buffer.position();
         int initialCompressedSize = this.compressor.initialCompressedBufferLength(uncompressedSize);
         if(this.compressedBuffer == null || this.compressedBuffer.capacity() < initialCompressedSize) {
            this.compressedBuffer = this.compressor.preferredBufferType().allocate(initialCompressedSize);
         }

         this.buffer.flip();
         this.parameters.getSstableCompressor().compress(this.buffer, this.compressedBuffer);
         int actualCompressedSize = this.compressedBuffer.position();
         this.out.writeInt(uncompressedSize);
         this.out.writeInt(actualCompressedSize);
         if(this.compressedBuffer.hasArray()) {
            this.out.write(this.compressedBuffer.array(), 0, actualCompressedSize);
         } else {
            byte[] data = new byte[actualCompressedSize];
            this.compressedBuffer.flip();
            this.compressedBuffer.get(data);
            this.out.write(data, 0, actualCompressedSize);
         }

         this.buffer.rewind();
         this.compressedBuffer.rewind();
      }

      public void write(int b) throws IOException {
         if(this.buffer.position() >= this.chunkLength) {
            this.writeChunk();
         }

         this.buffer.put((byte)b);
      }
   }

   static class CompressedInputStream extends InputStream {
      private final DataInputStream in;
      private volatile ByteBuffer buffer = null;
      private volatile boolean eof = false;
      private final ICompressor compressor;

      CompressedInputStream(File file, File crcPath, CompressionParams parameters) throws IOException {
         assert parameters.getSstableCompressor() != null;

         this.in = new DataInputStream(ChecksummedRandomAccessReader.open(file, crcPath));
         this.compressor = parameters.getSstableCompressor();
      }

      private void readChunk() throws IOException {
         int uncompressedSize = this.in.readInt();
         if(uncompressedSize == -1) {
            this.eof = true;
         } else {
            int compressedSize = this.in.readInt();
            ByteBuffer input = this.compressor.preferredBufferType().allocate(compressedSize);
            int read;
            if(input.hasArray()) {
               read = this.in.read(input.array());
            } else {
               byte[] inputData = new byte[compressedSize];
               read = this.in.read(inputData);
               input.put(inputData);
               input.flip();
            }

            assert read == compressedSize;

            this.buffer = this.compressor.preferredBufferType().allocate(uncompressedSize);
            this.compressor.uncompress(input, this.buffer);
            this.buffer.flip();

            assert this.buffer.remaining() == uncompressedSize;

         }
      }

      public int read() throws IOException {
         if(this.buffer == null || !this.buffer.hasRemaining()) {
            this.readChunk();
         }

         return this.eof?-1:this.buffer.get() & 255;
      }
   }
}
