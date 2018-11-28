package org.apache.cassandra.io.compress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Longs;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Transactional;

public class CompressionMetadata implements AutoCloseable {
   public final long dataLength;
   public final long compressedFileLength;
   private final Memory chunkOffsets;
   private final long chunkOffsetsSize;
   public final String indexFilePath;
   public final CompressionParams parameters;

   public static CompressionMetadata create(String dataFilePath) {
      return createWithLength(dataFilePath, (new File(dataFilePath)).length());
   }

   public static CompressionMetadata createWithLength(String dataFilePath, long compressedLength) {
      return new CompressionMetadata(Descriptor.fromFilename(dataFilePath), compressedLength);
   }

   @VisibleForTesting
   public CompressionMetadata(Descriptor desc, long compressedLength) {
      this(desc.filenameFor(Component.COMPRESSION_INFO), compressedLength, desc.version.hasMaxCompressedLength());
   }

   @VisibleForTesting
   public CompressionMetadata(String indexFilePath, long compressedLength, boolean hasMaxCompressedSize) {
      this.indexFilePath = indexFilePath;

      try {
         DataInputStream stream = new DataInputStream(Files.newInputStream(Paths.get(indexFilePath, new String[0]), new OpenOption[0]));
         Throwable var6 = null;

         try {
            String compressorName = stream.readUTF();
            int optionCount = stream.readInt();
            Map<String, String> options = new HashMap(optionCount);

            int chunkLength;
            for(chunkLength = 0; chunkLength < optionCount; ++chunkLength) {
               String key = stream.readUTF();
               String value = stream.readUTF();
               options.put(key, value);
            }

            chunkLength = stream.readInt();
            int maxCompressedSize = 2147483647;
            if(hasMaxCompressedSize) {
               maxCompressedSize = stream.readInt();
            }

            try {
               this.parameters = new CompressionParams(compressorName, chunkLength, maxCompressedSize, options);
            } catch (ConfigurationException var23) {
               throw new RuntimeException("Cannot create CompressionParams for stored parameters", var23);
            }

            this.dataLength = stream.readLong();
            this.compressedFileLength = compressedLength;
            this.chunkOffsets = this.readChunkOffsets(stream);
         } catch (Throwable var24) {
            var6 = var24;
            throw var24;
         } finally {
            if(stream != null) {
               if(var6 != null) {
                  try {
                     stream.close();
                  } catch (Throwable var22) {
                     var6.addSuppressed(var22);
                  }
               } else {
                  stream.close();
               }
            }

         }
      } catch (FileNotFoundException var26) {
         throw new RuntimeException(var26);
      } catch (IOException var27) {
         throw new CorruptSSTableException(var27, indexFilePath);
      }

      this.chunkOffsetsSize = this.chunkOffsets.size();
   }

   private CompressionMetadata(String filePath, CompressionParams parameters, SafeMemory offsets, long offsetsSize, long dataLength, long compressedLength) {
      this.indexFilePath = filePath;
      this.parameters = parameters;
      this.dataLength = dataLength;
      this.compressedFileLength = compressedLength;
      this.chunkOffsets = offsets;
      this.chunkOffsetsSize = offsetsSize;
   }

   public ICompressor compressor() {
      return this.parameters.getSstableCompressor();
   }

   public int chunkLength() {
      return this.parameters.chunkLength();
   }

   public int maxCompressedLength() {
      return this.parameters.maxCompressedLength();
   }

   public long offHeapSize() {
      return this.chunkOffsets.size();
   }

   public void addTo(Ref.IdentityCollection identities) {
      identities.add(this.chunkOffsets);
   }

   private Memory readChunkOffsets(DataInput input) {
      int chunkCount;
      try {
         chunkCount = input.readInt();
         if(chunkCount <= 0) {
            throw new IOException("Compressed file with 0 chunks encountered: " + input);
         }
      } catch (IOException var8) {
         throw new FSReadError(var8, this.indexFilePath);
      }

      Memory offsets = Memory.allocate((long)chunkCount * 8L);
      int i = 0;

      try {
         for(i = 0; i < chunkCount; ++i) {
            offsets.setLong((long)i * 8L, input.readLong());
         }

         return offsets;
      } catch (IOException var7) {
         if(offsets != null) {
            offsets.close();
         }

         if(var7 instanceof EOFException) {
            String msg = String.format("Corrupted Index File %s: read %d but expected %d chunks.", new Object[]{this.indexFilePath, Integer.valueOf(i), Integer.valueOf(chunkCount)});
            throw new CorruptSSTableException(new IOException(msg, var7), this.indexFilePath);
         } else {
            throw new FSReadError(var7, this.indexFilePath);
         }
      }
   }

   public CompressionMetadata.Chunk chunkFor(long position) {
      int idx = 8 * (int)(position / (long)this.parameters.chunkLength());
      if((long)idx >= this.chunkOffsetsSize) {
         throw new CorruptSSTableException(new EOFException(), this.indexFilePath);
      } else {
         long chunkOffset = this.chunkOffsets.getLong((long)idx);
         long nextChunkOffset = (long)(idx + 8) == this.chunkOffsetsSize?this.compressedFileLength:this.chunkOffsets.getLong((long)(idx + 8));
         return new CompressionMetadata.Chunk(chunkOffset, (int)(nextChunkOffset - chunkOffset - 4L));
      }
   }

   public long getTotalSizeForSections(Collection<Pair<Long, Long>> sections) {
      long size = 0L;
      long lastOffset = -1L;
      Iterator var6 = sections.iterator();

      while(var6.hasNext()) {
         Pair<Long, Long> section = (Pair)var6.next();
         int startIndex = (int)(((Long)section.left).longValue() / (long)this.parameters.chunkLength());
         int endIndex = (int)(((Long)section.right).longValue() / (long)this.parameters.chunkLength());
         endIndex = ((Long)section.right).longValue() % (long)this.parameters.chunkLength() == 0L?endIndex - 1:endIndex;

         for(int i = startIndex; i <= endIndex; ++i) {
            long offset = (long)i * 8L;
            long chunkOffset = this.chunkOffsets.getLong(offset);
            if(chunkOffset > lastOffset) {
               lastOffset = chunkOffset;
               long nextChunkOffset = offset + 8L == this.chunkOffsetsSize?this.compressedFileLength:this.chunkOffsets.getLong(offset + 8L);
               size += nextChunkOffset - chunkOffset;
            }
         }
      }

      return size;
   }

   public CompressionMetadata.Chunk[] getChunksForSections(Collection<Pair<Long, Long>> sections) {
      SortedSet<CompressionMetadata.Chunk> offsets = new TreeSet(new Comparator<CompressionMetadata.Chunk>() {
         public int compare(CompressionMetadata.Chunk o1, CompressionMetadata.Chunk o2) {
            return Longs.compare(o1.offset, o2.offset);
         }
      });
      Iterator var3 = sections.iterator();

      while(var3.hasNext()) {
         Pair<Long, Long> section = (Pair)var3.next();
         int startIndex = (int)(((Long)section.left).longValue() / (long)this.parameters.chunkLength());
         int endIndex = (int)(((Long)section.right).longValue() / (long)this.parameters.chunkLength());
         endIndex = ((Long)section.right).longValue() % (long)this.parameters.chunkLength() == 0L?endIndex - 1:endIndex;

         for(int i = startIndex; i <= endIndex; ++i) {
            long offset = (long)i * 8L;
            long chunkOffset = this.chunkOffsets.getLong(offset);
            long nextChunkOffset = offset + 8L == this.chunkOffsetsSize?this.compressedFileLength:this.chunkOffsets.getLong(offset + 8L);
            offsets.add(new CompressionMetadata.Chunk(chunkOffset, (int)(nextChunkOffset - chunkOffset - 4L)));
         }
      }

      return (CompressionMetadata.Chunk[])offsets.toArray(new CompressionMetadata.Chunk[0]);
   }

   public void close() {
      this.chunkOffsets.close();
   }

   static class ChunkSerializer implements Serializer<CompressionMetadata.Chunk> {
      ChunkSerializer() {
      }

      public void serialize(CompressionMetadata.Chunk chunk, DataOutputPlus out) throws IOException {
         out.writeLong(chunk.offset);
         out.writeInt(chunk.length);
      }

      public CompressionMetadata.Chunk deserialize(DataInputPlus in) throws IOException {
         return new CompressionMetadata.Chunk(in.readLong(), in.readInt());
      }

      public long serializedSize(CompressionMetadata.Chunk chunk) {
         long size = (long)TypeSizes.sizeof(chunk.offset);
         size += (long)TypeSizes.sizeof(chunk.length);
         return size;
      }
   }

   public static class Chunk {
      public static final Serializer<CompressionMetadata.Chunk> serializer = new CompressionMetadata.ChunkSerializer();
      public final long offset;
      public final int length;

      public Chunk(long offset, int length) {
         assert length > 0;

         this.offset = offset;
         this.length = length;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            CompressionMetadata.Chunk chunk = (CompressionMetadata.Chunk)o;
            return this.length == chunk.length && this.offset == chunk.offset;
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = (int)(this.offset ^ this.offset >>> 32);
         result = 31 * result + this.length;
         return result;
      }

      public String toString() {
         return String.format("Chunk<offset: %d, length: %d>", new Object[]{Long.valueOf(this.offset), Integer.valueOf(this.length)});
      }
   }

   public static class Writer extends Transactional.AbstractTransactional implements Transactional {
      private final CompressionParams parameters;
      private final String filePath;
      private int maxCount = 100;
      private SafeMemory offsets;
      private int count;
      private long dataLength;
      private long chunkCount;

      private Writer(CompressionParams parameters, String path) {
         this.offsets = new SafeMemory((long)this.maxCount * 8L);
         this.count = 0;
         this.parameters = parameters;
         this.filePath = path;
      }

      public static CompressionMetadata.Writer open(CompressionParams parameters, String path) {
         return new CompressionMetadata.Writer(parameters, path);
      }

      public void addOffset(long offset) {
         if(this.count == this.maxCount) {
            SafeMemory newOffsets = this.offsets.copy((long)(this.maxCount = (int)((long)this.maxCount * 2L)) * 8L);
            this.offsets.close();
            this.offsets = newOffsets;
         }

         this.offsets.setLong(8L * (long)(this.count++), offset);
      }

      private void writeHeader(DataOutput out, long dataLength, int chunks) {
         try {
            if(this.parameters.getSstableCompressor().getClass().getCanonicalName().startsWith("org.apache.cassandra.io.compress")) {
               out.writeUTF(this.parameters.getSstableCompressor().getClass().getSimpleName());
            } else {
               out.writeUTF(this.parameters.getSstableCompressor().getClass().getCanonicalName());
            }

            out.writeInt(this.parameters.getOtherOptions().size());
            UnmodifiableIterator var5 = this.parameters.getOtherOptions().entrySet().iterator();

            while(var5.hasNext()) {
               Entry<String, String> entry = (Entry)var5.next();
               out.writeUTF((String)entry.getKey());
               out.writeUTF((String)entry.getValue());
            }

            out.writeInt(this.parameters.chunkLength());
            out.writeInt(this.parameters.maxCompressedLength());
            out.writeLong(dataLength);
            out.writeInt(chunks);
         } catch (IOException var7) {
            throw new FSWriteError(var7, this.filePath);
         }
      }

      public CompressionMetadata.Writer finalizeLength(long dataLength, int chunkCount) {
         this.dataLength = dataLength;
         this.chunkCount = (long)chunkCount;
         return this;
      }

      public void doPrepare() {
         assert this.chunkCount == (long)this.count;

         if(this.offsets.size() != (long)this.count * 8L) {
            SafeMemory tmp = this.offsets;
            this.offsets = this.offsets.copy((long)this.count * 8L);
            tmp.free();
         }

         try {
            SeekableByteChannel fos = Files.newByteChannel((new File(this.filePath)).toPath(), new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE});
            Throwable var2 = null;

            try {
               DataOutputStream out = new DataOutputStream(new BufferedDataOutputStreamPlus(fos));
               Throwable var4 = null;

               try {
                  this.writeHeader(out, this.dataLength, this.count);

                  for(int i = 0; i < this.count; ++i) {
                     out.writeLong(this.offsets.getLong((long)i * 8L));
                  }

                  out.flush();
                  NativeLibrary.getFileDescriptor((FileChannel)fos).sync();
               } catch (Throwable var29) {
                  var4 = var29;
                  throw var29;
               } finally {
                  if(out != null) {
                     if(var4 != null) {
                        try {
                           out.close();
                        } catch (Throwable var28) {
                           var4.addSuppressed(var28);
                        }
                     } else {
                        out.close();
                     }
                  }

               }
            } catch (Throwable var31) {
               var2 = var31;
               throw var31;
            } finally {
               if(fos != null) {
                  if(var2 != null) {
                     try {
                        fos.close();
                     } catch (Throwable var27) {
                        var2.addSuppressed(var27);
                     }
                  } else {
                     fos.close();
                  }
               }

            }
         } catch (IOException var33) {
            throw Throwables.propagate(var33);
         }
      }

      public CompressionMetadata open(long dataLength, long compressedLength) {
         SafeMemory tOffsets = this.offsets.sharedCopy();
         int tCount = (int)(dataLength / (long)this.parameters.chunkLength());
         if(dataLength % (long)this.parameters.chunkLength() != 0L) {
            ++tCount;
         }

         assert tCount > 0;

         if(tCount < this.count) {
            compressedLength = tOffsets.getLong((long)tCount * 8L);
         }

         return new CompressionMetadata(this.filePath, this.parameters, tOffsets, (long)tCount * 8L, dataLength, compressedLength);
      }

      public long chunkOffsetBy(int chunkIndex) {
         return this.offsets.getLong((long)chunkIndex * 8L);
      }

      public void resetAndTruncate(int chunkIndex) {
         this.count = chunkIndex;
      }

      protected Throwable doPostCleanup(Throwable failed) {
         return this.offsets.close(failed);
      }

      protected Throwable doCommit(Throwable accumulate) {
         return accumulate;
      }

      protected Throwable doAbort(Throwable accumulate) {
         return accumulate;
      }
   }
}
