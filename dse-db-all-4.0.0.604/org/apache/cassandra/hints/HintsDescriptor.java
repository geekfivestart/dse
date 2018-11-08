package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import javax.crypto.Cipher;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.versioning.Version;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HintsDescriptor {
   private static final Logger logger = LoggerFactory.getLogger(HintsDescriptor.class);
   static final HintsVerbs.HintsVersion CURRENT_VERSION = (HintsVerbs.HintsVersion)Version.last(HintsVerbs.HintsVersion.class);
   static final String COMPRESSION = "compression";
   static final String ENCRYPTION = "encryption";
   static final Pattern pattern = Pattern.compile("^[a-fA-F0-9]{8}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{12}\\-(\\d+)\\-(\\d+)\\.hints$");
   final UUID hostId;
   final HintsVerbs.HintsVersion version;
   final long timestamp;
   final ImmutableMap<String, Object> parameters;
   final ParameterizedClass compressionConfig;
   private volatile HintsDescriptor.Statistics statistics;
   private final Cipher cipher;
   private final ICompressor compressor;
   public static HintsDescriptor.Statistics EMPTY_STATS = new HintsDescriptor.Statistics(0L);

   HintsDescriptor(UUID hostId, HintsVerbs.HintsVersion version, long timestamp, ImmutableMap<String, Object> parameters) {
      this.hostId = hostId;
      this.version = version;
      this.timestamp = timestamp;
      this.statistics = EMPTY_STATS;
      this.compressionConfig = createCompressionConfig(parameters);
      HintsDescriptor.EncryptionData encryption = createEncryption(parameters);
      if(encryption == null) {
         this.cipher = null;
         this.compressor = null;
      } else {
         if(this.compressionConfig != null) {
            throw new IllegalStateException("a hints file cannot be configured for both compression and encryption");
         }

         this.cipher = encryption.cipher;
         this.compressor = encryption.compressor;
         parameters = encryption.params;
      }

      this.parameters = parameters;
   }

   HintsDescriptor(UUID hostId, long timestamp, ImmutableMap<String, Object> parameters) {
      this(hostId, CURRENT_VERSION, timestamp, parameters);
   }

   HintsDescriptor(UUID hostId, long timestamp) {
      this(hostId, CURRENT_VERSION, timestamp, ImmutableMap.of());
   }

   static ParameterizedClass createCompressionConfig(Map<String, Object> params) {
      if(params.containsKey("compression")) {
         Map<String, Object> compressorConfig = (Map)params.get("compression");
         return new ParameterizedClass((String)compressorConfig.get("class_name"), (Map)compressorConfig.get("parameters"));
      } else {
         return null;
      }
   }

   static HintsDescriptor.EncryptionData createEncryption(ImmutableMap<String, Object> params) {
      if(params.containsKey("encryption")) {
         Map<?, ?> encryptionConfig = (Map)params.get("encryption");
         EncryptionContext encryptionContext = EncryptionContext.createFromMap(encryptionConfig, DatabaseDescriptor.getEncryptionContext());

         try {
            Cipher cipher;
            if(encryptionConfig.containsKey("encIV")) {
               cipher = encryptionContext.getDecryptor();
            } else {
               cipher = encryptionContext.getEncryptor();
               ImmutableMap<String, Object> encParams = ImmutableMap.builder().putAll(encryptionContext.toHeaderParameters()).put("encIV", Hex.bytesToHex(cipher.getIV())).build();
               Map<String, Object> map = new HashMap(params);
               map.put("encryption", encParams);
               params = ImmutableMap.builder().putAll(map).build();
            }

            return new HintsDescriptor.EncryptionData(cipher, encryptionContext.getCompressor(), params);
         } catch (IOException var6) {
            logger.warn("failed to create encyption context for hints file. ignoring encryption for hints.", var6);
            return null;
         }
      } else {
         return null;
      }
   }

   public void setStatistics(HintsDescriptor.Statistics statistics) {
      this.statistics = statistics;
   }

   public HintsDescriptor.Statistics statistics() {
      return this.statistics;
   }

   String fileName() {
      return String.format("%s-%s-%s.hints", new Object[]{this.hostId, Long.valueOf(this.timestamp), Integer.valueOf(this.version.code())});
   }

   String statisticsFileName() {
      return statisticsFileName(this.hostId, this.timestamp, this.version.code());
   }

   static String statisticsFileName(UUID hostId, long timestamp, int version) {
      return String.format("%s-%s-%s-Statistics.hints", new Object[]{hostId, Long.valueOf(timestamp), Integer.valueOf(version)});
   }

   String checksumFileName() {
      return String.format("%s-%s-%s.crc32", new Object[]{this.hostId, Long.valueOf(this.timestamp), Integer.valueOf(this.version.code())});
   }

   static boolean isHintFileName(Path path) {
      return pattern.matcher(path.getFileName().toString()).matches();
   }

   static Optional<HintsDescriptor> readFromFileQuietly(Path path) {
      try {
         RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
         Throwable var2 = null;

         Optional var3;
         try {
            var3 = Optional.of(deserialize(raf));
         } catch (Throwable var14) {
            var2 = var14;
            throw var14;
         } finally {
            if(raf != null) {
               if(var2 != null) {
                  try {
                     raf.close();
                  } catch (Throwable var13) {
                     var2.addSuppressed(var13);
                  }
               } else {
                  raf.close();
               }
            }

         }

         return var3;
      } catch (ChecksumMismatchException var16) {
         throw new FSReadError(var16, path.toFile());
      } catch (IOException var17) {
         logger.error("Failed to deserialize hints descriptor {}", path.toString(), var17);
         return Optional.empty();
      }
   }

   static HintsDescriptor readFromFile(Path path) {
      try {
         RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
         Throwable var2 = null;

         HintsDescriptor var4;
         try {
            HintsDescriptor descriptor = deserialize(raf);
            descriptor.loadStatsComponent(path.getParent().toString());
            StorageMetrics.hintsOnDisk.inc(descriptor.statistics().totalCount());
            var4 = descriptor;
         } catch (Throwable var14) {
            var2 = var14;
            throw var14;
         } finally {
            if(raf != null) {
               if(var2 != null) {
                  try {
                     raf.close();
                  } catch (Throwable var13) {
                     var2.addSuppressed(var13);
                  }
               } else {
                  raf.close();
               }
            }

         }

         return var4;
      } catch (IOException var16) {
         throw new FSReadError(var16, path.toFile());
      }
   }

   @VisibleForTesting
   void loadStatsComponent(String hintsDirectory) {
      File file = new File(hintsDirectory, this.statisticsFileName());

      try {
         RandomAccessFile statsFile = new RandomAccessFile(file, "r");
         Throwable var4 = null;

         try {
            this.statistics = HintsDescriptor.Statistics.deserialize(statsFile);
         } catch (Throwable var15) {
            var4 = var15;
            throw var15;
         } finally {
            if(statsFile != null) {
               if(var4 != null) {
                  try {
                     statsFile.close();
                  } catch (Throwable var14) {
                     var4.addSuppressed(var14);
                  }
               } else {
                  statsFile.close();
               }
            }

         }
      } catch (FileNotFoundException var17) {
         logger.warn("Cannot find stats component `{}` for hints descriptor, initialising with empty statistics.", file.toString());
         this.statistics = EMPTY_STATS;
      } catch (IOException var18) {
         logger.error("Cannot read stats component `{}` for hints descriptor, initialising with empty statistics.", file.toString(), var18);
         this.statistics = EMPTY_STATS;
      }

   }

   public boolean isCompressed() {
      return this.compressionConfig != null;
   }

   public boolean isEncrypted() {
      return this.cipher != null;
   }

   public ICompressor createCompressor() {
      return this.isCompressed()?CompressionParams.createCompressor(this.compressionConfig):(this.isEncrypted()?this.compressor:null);
   }

   public Cipher getCipher() {
      return this.isEncrypted()?this.cipher:null;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("hostId", this.hostId).add("version", this.version).add("timestamp", this.timestamp).add("parameters", this.parameters).toString();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof HintsDescriptor)) {
         return false;
      } else {
         HintsDescriptor hd = (HintsDescriptor)o;
         return Objects.equals(this.hostId, hd.hostId) && Objects.equals(this.version, hd.version) && Objects.equals(Long.valueOf(this.timestamp), Long.valueOf(hd.timestamp)) && Objects.equals(this.parameters, hd.parameters);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.hostId, this.version, Long.valueOf(this.timestamp), this.parameters});
   }

   void serialize(DataOutputPlus out) throws IOException {
      CRC32 crc = new CRC32();
      out.writeInt(this.version.code());
      FBUtilities.updateChecksumInt(crc, this.version.code());
      out.writeLong(this.timestamp);
      updateChecksumLong(crc, this.timestamp);
      out.writeLong(this.hostId.getMostSignificantBits());
      updateChecksumLong(crc, this.hostId.getMostSignificantBits());
      out.writeLong(this.hostId.getLeastSignificantBits());
      updateChecksumLong(crc, this.hostId.getLeastSignificantBits());
      byte[] paramsBytes = JSONValue.toJSONString(this.parameters).getBytes(StandardCharsets.UTF_8);
      out.writeInt(paramsBytes.length);
      FBUtilities.updateChecksumInt(crc, paramsBytes.length);
      out.writeInt((int)crc.getValue());
      out.write(paramsBytes);
      crc.update(paramsBytes, 0, paramsBytes.length);
      out.writeInt((int)crc.getValue());
   }

   int serializedSize() {
      int size = TypeSizes.sizeof(this.version.code());
      size += TypeSizes.sizeof(this.timestamp);
      size += TypeSizes.sizeof(this.hostId.getMostSignificantBits());
      size += TypeSizes.sizeof(this.hostId.getLeastSignificantBits());
      byte[] paramsBytes = JSONValue.toJSONString(this.parameters).getBytes(StandardCharsets.UTF_8);
      size += TypeSizes.sizeof(paramsBytes.length);
      size += 4;
      size += paramsBytes.length;
      size += 4;
      return size;
   }

   static HintsDescriptor deserialize(DataInput in) throws IOException {
      CRC32 crc = new CRC32();
      int version = in.readInt();
      FBUtilities.updateChecksumInt(crc, version);
      long timestamp = in.readLong();
      updateChecksumLong(crc, timestamp);
      long msb = in.readLong();
      updateChecksumLong(crc, msb);
      long lsb = in.readLong();
      updateChecksumLong(crc, lsb);
      UUID hostId = new UUID(msb, lsb);
      int paramsLength = in.readInt();
      FBUtilities.updateChecksumInt(crc, paramsLength);
      validateCRC(in.readInt(), (int)crc.getValue());
      byte[] paramsBytes = new byte[paramsLength];
      in.readFully(paramsBytes, 0, paramsLength);
      crc.update(paramsBytes, 0, paramsLength);
      validateCRC(in.readInt(), (int)crc.getValue());
      return new HintsDescriptor(hostId, HintsVerbs.HintsVersion.fromCode(version), timestamp, decodeJSONBytes(paramsBytes));
   }

   private static ImmutableMap<String, Object> decodeJSONBytes(byte[] bytes) {
      return ImmutableMap.copyOf((Map)JSONValue.parse(new String(bytes, StandardCharsets.UTF_8)));
   }

   private static void updateChecksumLong(CRC32 crc, long value) {
      FBUtilities.updateChecksumInt(crc, (int)(value & 4294967295L));
      FBUtilities.updateChecksumInt(crc, (int)(value >>> 32));
   }

   private static void validateCRC(int expected, int actual) throws IOException {
      if(expected != actual) {
         throw new ChecksumMismatchException("Hints Descriptor CRC Mismatch");
      }
   }

   public static class Statistics {
      private final long totalCount;

      public Statistics(long totalCount) {
         this.totalCount = totalCount;
      }

      public long totalCount() {
         return this.totalCount;
      }

      public void serialize(DataOutput out) throws IOException {
         out.writeLong(this.totalCount);
      }

      public static HintsDescriptor.Statistics deserialize(DataInput in) throws IOException {
         long totalCount = in.readLong();
         return new HintsDescriptor.Statistics(totalCount);
      }

      public static int serializedSize() {
         return 8;
      }
   }

   private static final class EncryptionData {
      final Cipher cipher;
      final ICompressor compressor;
      final ImmutableMap<String, Object> params;

      private EncryptionData(Cipher cipher, ICompressor compressor, ImmutableMap<String, Object> params) {
         this.cipher = cipher;
         this.compressor = compressor;
         this.params = params;
      }
   }
}
