package org.apache.cassandra.io.compress;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LZ4Compressor implements ICompressor {
   private static final Logger logger = LoggerFactory.getLogger(LZ4Compressor.class);
   public static final String LZ4_FAST_COMPRESSOR = "fast";
   public static final String LZ4_HIGH_COMPRESSOR = "high";
   private static final Set<String> VALID_COMPRESSOR_TYPES = new HashSet(Arrays.asList(new String[]{"fast", "high"}));
   private static final int DEFAULT_HIGH_COMPRESSION_LEVEL = 9;
   private static final String DEFAULT_LZ4_COMPRESSOR_TYPE = "fast";
   public static final String LZ4_HIGH_COMPRESSION_LEVEL = "lz4_high_compressor_level";
   public static final String LZ4_COMPRESSOR_TYPE = "lz4_compressor_type";
   private static final int INTEGER_BYTES = 4;
   private static final ConcurrentHashMap<Pair<String, Integer>, LZ4Compressor> instances = new ConcurrentHashMap();
   private final net.jpountz.lz4.LZ4Compressor compressor;
   private final LZ4FastDecompressor decompressor;
   @VisibleForTesting
   final String compressorType;
   @VisibleForTesting
   final Integer compressionLevel;

   public static LZ4Compressor create(Map<String, String> args) throws ConfigurationException {
      String compressorType = validateCompressorType((String)args.get("lz4_compressor_type"));
      Integer compressionLevel = validateCompressionLevel((String)args.get("lz4_high_compressor_level"));
      Pair<String, Integer> compressorTypeAndLevel = Pair.create(compressorType, compressionLevel);
      LZ4Compressor instance = (LZ4Compressor)instances.get(compressorTypeAndLevel);
      if(instance == null) {
         if(compressorType.equals("fast") && args.get("lz4_high_compressor_level") != null) {
            logger.warn("'{}' parameter is ignored when '{}' is '{}'", new Object[]{"lz4_high_compressor_level", "lz4_compressor_type", "fast"});
         }

         instance = new LZ4Compressor(compressorType, compressionLevel);
         LZ4Compressor instanceFromMap = (LZ4Compressor)instances.putIfAbsent(compressorTypeAndLevel, instance);
         if(instanceFromMap != null) {
            instance = instanceFromMap;
         }
      }

      return instance;
   }

   private LZ4Compressor(String type, Integer compressionLevel) {
      this.compressorType = type;
      this.compressionLevel = compressionLevel;
      LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
      byte var5 = -1;
      switch(type.hashCode()) {
      case 3135580:
         if(type.equals("fast")) {
            var5 = 1;
         }
         break;
      case 3202466:
         if(type.equals("high")) {
            var5 = 0;
         }
      }

      switch(var5) {
      case 0:
         this.compressor = lz4Factory.highCompressor(compressionLevel.intValue());
         break;
      case 1:
      default:
         this.compressor = lz4Factory.fastCompressor();
      }

      this.decompressor = lz4Factory.fastDecompressor();
   }

   public int initialCompressedBufferLength(int chunkLength) {
      return 4 + this.compressor.maxCompressedLength(chunkLength);
   }

   public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
      int len = input.remaining();
      output.put((byte)len);
      output.put((byte)(len >>> 8));
      output.put((byte)(len >>> 16));
      output.put((byte)(len >>> 24));

      try {
         this.compressor.compress(input, output);
      } catch (LZ4Exception var5) {
         throw new IOException(var5);
      }
   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
      int decompressedLength = input[inputOffset] & 255 | (input[inputOffset + 1] & 255) << 8 | (input[inputOffset + 2] & 255) << 16 | (input[inputOffset + 3] & 255) << 24;

      int compressedLength;
      try {
         compressedLength = this.decompressor.decompress(input, inputOffset + 4, output, outputOffset, decompressedLength);
      } catch (LZ4Exception var9) {
         throw new IOException(var9);
      }

      if(compressedLength != inputLength - 4) {
         throw new IOException("Compressed lengths mismatch");
      } else {
         return decompressedLength;
      }
   }

   public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
      int decompressedLength = input.get() & 255 | (input.get() & 255) << 8 | (input.get() & 255) << 16 | (input.get() & 255) << 24;

      try {
         int compressedLength = this.decompressor.decompress(input, input.position(), output, output.position(), decompressedLength);
         input.position(input.position() + compressedLength);
         output.position(output.position() + decompressedLength);
      } catch (LZ4Exception var5) {
         throw new IOException(var5);
      }

      if(input.remaining() > 0) {
         throw new IOException("Compressed lengths mismatch - " + input.remaining() + " bytes remain");
      }
   }

   public Set<String> supportedOptions() {
      return new HashSet(Arrays.asList(new String[]{"lz4_high_compressor_level", "lz4_compressor_type"}));
   }

   public static String validateCompressorType(String compressorType) throws ConfigurationException {
      if(compressorType == null) {
         return "fast";
      } else if(!VALID_COMPRESSOR_TYPES.contains(compressorType)) {
         throw new ConfigurationException(String.format("Invalid compressor type '%s' specified for LZ4 parameter '%s'. Valid options are %s.", new Object[]{compressorType, "lz4_compressor_type", VALID_COMPRESSOR_TYPES.toString()}));
      } else {
         return compressorType;
      }
   }

   public static Integer validateCompressionLevel(String compressionLevel) throws ConfigurationException {
      if(compressionLevel == null) {
         return Integer.valueOf(9);
      } else {
         ConfigurationException ex = new ConfigurationException("Invalid value [" + compressionLevel + "] for parameter '" + "lz4_high_compressor_level" + "'. Value must be between 1 and 17.");

         Integer level;
         try {
            level = Integer.valueOf(compressionLevel);
         } catch (NumberFormatException var4) {
            throw ex;
         }

         if(level.intValue() >= 1 && level.intValue() <= 17) {
            return level;
         } else {
            throw ex;
         }
      }
   }

   public BufferType preferredBufferType() {
      return BufferType.OFF_HEAP;
   }

   public boolean supports(BufferType bufferType) {
      return true;
   }
}
