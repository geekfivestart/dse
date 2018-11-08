package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.SystemTableEncryptionOptions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompressionParams {
   private static final Logger logger = LoggerFactory.getLogger(CompressionParams.class);
   private static volatile boolean hasLoggedSsTableCompressionWarning;
   private static volatile boolean hasLoggedChunkLengthWarning;
   private static volatile boolean hasLoggedCrcCheckChanceWarning;
   public static final int DEFAULT_CHUNK_LENGTH = 65536;
   public static final double DEFAULT_MIN_COMPRESS_RATIO = 0.0D;
   public static final Versioned<StreamMessage.StreamVersion, Serializer<CompressionParams>> serializers = StreamMessage.StreamVersion.versioned(CompressionParams.CompressionParmsSerializer::<init>);
   public static final String CLASS = "class";
   public static final String CHUNK_LENGTH_IN_KB = "chunk_length_in_kb";
   public static final String ENABLED = "enabled";
   public static final String MIN_COMPRESS_RATIO = "min_compress_ratio";
   public static final String CIPHER_ALGORITHM = "cipher_algorithm";
   public static final String SECRET_KEY_STRENGTH = "secret_key_strength";
   public static final String KEY_PROVIDER = "key_provider";
   public static final String HOST_NAME = "kmip_host";
   public static final String SECRET_KEY_FILE = "secret_key_file";
   public static final CompressionParams DEFAULT = new CompressionParams(LZ4Compressor.create(Collections.emptyMap()), 65536, calcMaxCompressedLength(65536, 0.0D), 0.0D, Collections.emptyMap());
   private static final String CRC_CHECK_CHANCE_WARNING = "The option crc_check_chance was deprecated as a compression option. You should specify it as a top-level table option instead";
   /** @deprecated */
   @Deprecated
   public static final String SSTABLE_COMPRESSION = "sstable_compression";
   /** @deprecated */
   @Deprecated
   public static final String CHUNK_LENGTH_KB = "chunk_length_kb";
   /** @deprecated */
   @Deprecated
   public static final String CRC_CHECK_CHANCE = "crc_check_chance";
   private final ICompressor sstableCompressor;
   private final int chunkLength;
   private final int maxCompressedLength;
   private final double minCompressRatio;
   private final ImmutableMap<String, String> otherOptions;
   private volatile double crcCheckChance;

   public static CompressionParams fromMap(Map<String, String> opts) {
      Map<String, String> options = copyOptions(opts);
      if(!opts.isEmpty() && isEnabled(opts) && !containsSstableCompressionClass(opts)) {
         throw new ConfigurationException(String.format("Missing sub-option '%s' for the 'compression' option.", new Object[]{"class"}));
      } else {
         String sstableCompressionClass;
         if(!removeEnabled(options)) {
            sstableCompressionClass = null;
            if(!options.isEmpty()) {
               throw new ConfigurationException(String.format("If the '%s' option is set to false no other options must be specified", new Object[]{"enabled"}));
            }
         } else {
            sstableCompressionClass = removeSstableCompressionClass(options);
         }

         int chunkLength = removeChunkLength(options);
         double minCompressRatio = removeMinCompressRatio(options);
         CompressionParams cp = new CompressionParams(sstableCompressionClass, options, chunkLength, minCompressRatio);
         cp.validate();
         return cp;
      }
   }

   public static CompressionParams forSystemTables() {
      SystemTableEncryptionOptions options = DatabaseDescriptor.getSystemTableEncryptionOptions();
      if(!options.enabled) {
         return DEFAULT;
      } else {
         Map<String, String> opts = new HashMap();
         opts.put("class", "org.apache.cassandra.io.compress.EncryptingLZ4Compressor");
         opts.put("chunk_length_in_kb", Integer.toString(options.chunk_length_kb));
         opts.put("cipher_algorithm", options.cipher_algorithm);
         opts.put("secret_key_strength", Integer.toString(options.secret_key_strength));
         opts.put("key_provider", options.key_provider);
         if(options.isKmipKeyProvider()) {
            opts.put("kmip_host", options.kmip_host);
         } else {
            opts.put("secret_key_file", String.format("%s/system/%s", new Object[]{DatabaseDescriptor.getSystemKeyDirectory(), options.key_name}));
         }

         return fromMap(opts);
      }
   }

   public Class<? extends ICompressor> klass() {
      return this.sstableCompressor.getClass();
   }

   public static CompressionParams noCompression() {
      return new CompressionParams((ICompressor)null, 65536, 2147483647, 0.0D, Collections.emptyMap());
   }

   @VisibleForTesting
   public static CompressionParams snappy() {
      return snappy(65536);
   }

   @VisibleForTesting
   public static CompressionParams snappy(int chunkLength) {
      return snappy(chunkLength, 1.1D);
   }

   @VisibleForTesting
   public static CompressionParams snappy(int chunkLength, double minCompressRatio) {
      return new CompressionParams(SnappyCompressor.instance, chunkLength, calcMaxCompressedLength(chunkLength, minCompressRatio), minCompressRatio, Collections.emptyMap());
   }

   @VisibleForTesting
   public static CompressionParams deflate() {
      return deflate(65536);
   }

   @VisibleForTesting
   public static CompressionParams deflate(int chunkLength) {
      return new CompressionParams(DeflateCompressor.instance, chunkLength, 2147483647, 0.0D, Collections.emptyMap());
   }

   @VisibleForTesting
   public static CompressionParams lz4() {
      return lz4(65536);
   }

   @VisibleForTesting
   public static CompressionParams lz4(int chunkLength) {
      return lz4(chunkLength, chunkLength);
   }

   @VisibleForTesting
   public static CompressionParams lz4(int chunkLength, int maxCompressedLength) {
      return new CompressionParams(LZ4Compressor.create(Collections.emptyMap()), chunkLength, maxCompressedLength, calcMinCompressRatio(chunkLength, maxCompressedLength), Collections.emptyMap());
   }

   public CompressionParams(String sstableCompressorClass, Map<String, String> otherOptions, int chunkLength, double minCompressRatio) throws ConfigurationException {
      this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, calcMaxCompressedLength(chunkLength, minCompressRatio), minCompressRatio, otherOptions);
   }

   static int calcMaxCompressedLength(int chunkLength, double minCompressRatio) {
      return (int)Math.ceil(Math.min((double)chunkLength / minCompressRatio, 2.147483647E9D));
   }

   public CompressionParams(String sstableCompressorClass, int chunkLength, int maxCompressedLength, Map<String, String> otherOptions) throws ConfigurationException {
      this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, maxCompressedLength, calcMinCompressRatio(chunkLength, maxCompressedLength), otherOptions);
   }

   static double calcMinCompressRatio(int chunkLength, int maxCompressedLength) {
      return maxCompressedLength == 2147483647?0.0D:(double)chunkLength * 1.0D / (double)maxCompressedLength;
   }

   private CompressionParams(ICompressor sstableCompressor, int chunkLength, int maxCompressedLength, double minCompressRatio, Map<String, String> otherOptions) throws ConfigurationException {
      this.crcCheckChance = 1.0D;
      this.sstableCompressor = sstableCompressor;
      this.chunkLength = chunkLength;
      this.otherOptions = ImmutableMap.copyOf(otherOptions);
      this.minCompressRatio = minCompressRatio;
      this.maxCompressedLength = maxCompressedLength;
   }

   public CompressionParams copy() {
      return new CompressionParams(this.sstableCompressor, this.chunkLength, this.maxCompressedLength, this.minCompressRatio, this.otherOptions);
   }

   public boolean isEnabled() {
      return this.sstableCompressor != null;
   }

   public ICompressor getSstableCompressor() {
      return this.sstableCompressor;
   }

   public ImmutableMap<String, String> getOtherOptions() {
      return this.otherOptions;
   }

   public int chunkLength() {
      return this.chunkLength;
   }

   public int maxCompressedLength() {
      return this.maxCompressedLength;
   }

   private static Class<?> parseCompressorClass(String className) throws ConfigurationException {
      if(className != null && !className.isEmpty()) {
         className = className.contains(".")?className:"org.apache.cassandra.io.compress." + className;

         try {
            return Class.forName(className);
         } catch (Exception var2) {
            throw new ConfigurationException("Could not create Compression for type " + className, var2);
         }
      } else {
         return null;
      }
   }

   private static ICompressor createCompressor(Class<?> compressorClass, Map<String, String> compressionOptions) throws ConfigurationException {
      if(compressorClass == null) {
         if(!compressionOptions.isEmpty()) {
            throw new ConfigurationException("Unknown compression options (" + compressionOptions.keySet() + ") since no compression class found");
         } else {
            return null;
         }
      } else {
         if(compressionOptions.containsKey("crc_check_chance")) {
            if(!hasLoggedCrcCheckChanceWarning) {
               logger.warn("The option crc_check_chance was deprecated as a compression option. You should specify it as a top-level table option instead");
               hasLoggedCrcCheckChanceWarning = true;
            }

            compressionOptions.remove("crc_check_chance");
         }

         try {
            Method method = compressorClass.getMethod("create", new Class[]{Map.class});
            ICompressor compressor = (ICompressor)method.invoke((Object)null, new Object[]{compressionOptions});
            Iterator var4 = compressionOptions.keySet().iterator();

            String provided;
            do {
               if(!var4.hasNext()) {
                  return compressor;
               }

               provided = (String)var4.next();
            } while(compressor.supportedOptions().contains(provided));

            throw new ConfigurationException("Unknown compression options " + provided);
         } catch (NoSuchMethodException var6) {
            throw new ConfigurationException("create method not found", var6);
         } catch (SecurityException var7) {
            throw new ConfigurationException("Access forbiden", var7);
         } catch (IllegalAccessException var8) {
            throw new ConfigurationException("Cannot access method create in " + compressorClass.getName(), var8);
         } catch (InvocationTargetException var9) {
            if(var9.getTargetException() instanceof ConfigurationException) {
               throw (ConfigurationException)var9.getTargetException();
            } else {
               Throwable cause = var9.getCause() == null?var9:var9.getCause();
               throw new ConfigurationException(String.format("%s.create() threw an error: %s %s", new Object[]{compressorClass.getSimpleName(), cause.getClass().getName(), ((Throwable)cause).getMessage()}), var9);
            }
         } catch (ExceptionInInitializerError var10) {
            throw new ConfigurationException("Cannot initialize class " + compressorClass.getName());
         }
      }
   }

   public static ICompressor createCompressor(ParameterizedClass compression) throws ConfigurationException {
      return createCompressor(parseCompressorClass(compression.class_name), copyOptions(compression.parameters));
   }

   private static Map<String, String> copyOptions(Map<? extends CharSequence, ? extends CharSequence> co) {
      if(co != null && !co.isEmpty()) {
         Map<String, String> compressionOptions = new HashMap();
         Iterator var2 = co.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<? extends CharSequence, ? extends CharSequence> entry = (Entry)var2.next();
            compressionOptions.put(((CharSequence)entry.getKey()).toString(), ((CharSequence)entry.getValue()).toString());
         }

         return compressionOptions;
      } else {
         return Collections.emptyMap();
      }
   }

   private static Integer parseChunkLength(String chLengthKB) throws ConfigurationException {
      if(chLengthKB == null) {
         return null;
      } else {
         try {
            int parsed = Integer.parseInt(chLengthKB);
            if(parsed > 2097151) {
               throw new ConfigurationException(String.format("Value of %s is too large (%s)", new Object[]{"chunk_length_in_kb", Integer.valueOf(parsed)}));
            } else {
               return Integer.valueOf(1024 * parsed);
            }
         } catch (NumberFormatException var2) {
            throw new ConfigurationException("Invalid value for chunk_length_in_kb", var2);
         }
      }
   }

   private static int removeChunkLength(Map<String, String> options) {
      if(options.containsKey("chunk_length_in_kb")) {
         if(options.containsKey("chunk_length_kb")) {
            throw new ConfigurationException(String.format("The '%s' option must not be used if the chunk length is already specified by the '%s' option", new Object[]{"chunk_length_kb", "chunk_length_in_kb"}));
         } else {
            return parseChunkLength((String)options.remove("chunk_length_in_kb")).intValue();
         }
      } else if(options.containsKey("chunk_length_kb")) {
         if(!hasLoggedChunkLengthWarning) {
            hasLoggedChunkLengthWarning = true;
            logger.warn("The {} option has been deprecated. You should use {} instead", "chunk_length_kb", "chunk_length_in_kb");
         }

         return parseChunkLength((String)options.remove("chunk_length_kb")).intValue();
      } else {
         return 65536;
      }
   }

   private static double removeMinCompressRatio(Map<String, String> options) {
      String ratio = (String)options.remove("min_compress_ratio");
      return ratio != null?Double.parseDouble(ratio):0.0D;
   }

   public static boolean containsSstableCompressionClass(Map<String, String> options) {
      return options.containsKey("class") || options.containsKey("sstable_compression");
   }

   private static String removeSstableCompressionClass(Map<String, String> options) {
      if(options.containsKey("class")) {
         if(options.containsKey("sstable_compression")) {
            throw new ConfigurationException(String.format("The '%s' option must not be used if the compression algorithm is already specified by the '%s' option", new Object[]{"sstable_compression", "class"}));
         } else {
            String clazz = (String)options.remove("class");
            if(clazz.isEmpty()) {
               throw new ConfigurationException(String.format("The '%s' option must not be empty. To disable compression use 'enabled' : false", new Object[]{"class"}));
            } else {
               return clazz;
            }
         }
      } else {
         if(options.containsKey("sstable_compression") && !hasLoggedSsTableCompressionWarning) {
            hasLoggedSsTableCompressionWarning = true;
            logger.warn("The {} option has been deprecated. You should use {} instead", "sstable_compression", "class");
         }

         return (String)options.remove("sstable_compression");
      }
   }

   public static boolean isEnabled(Map<String, String> options) {
      String enabled = (String)options.get("enabled");
      return enabled == null || Boolean.parseBoolean(enabled);
   }

   private static boolean removeEnabled(Map<String, String> options) {
      String enabled = (String)options.remove("enabled");
      return enabled == null || Boolean.parseBoolean(enabled);
   }

   public void validate() throws ConfigurationException {
      if(this.chunkLength <= 0) {
         throw new ConfigurationException("Invalid negative or null chunk_length_in_kb");
      } else if((this.chunkLength & this.chunkLength - 1) != 0) {
         throw new ConfigurationException("chunk_length_in_kb must be a power of 2");
      } else if(this.maxCompressedLength < 0) {
         throw new ConfigurationException("Invalid negative min_compress_ratio");
      } else if(this.maxCompressedLength > this.chunkLength && this.maxCompressedLength < 2147483647) {
         throw new ConfigurationException("min_compress_ratio can either be 0 or greater than or equal to 1");
      }
   }

   public Map<String, String> asMap() {
      if(!this.isEnabled()) {
         return Collections.singletonMap("enabled", "false");
      } else {
         Map<String, String> options = new HashMap(this.otherOptions);
         options.put("class", this.sstableCompressor.getClass().getName());
         options.put("chunk_length_in_kb", this.chunkLengthInKB());
         if(this.minCompressRatio != 0.0D) {
            options.put("min_compress_ratio", String.valueOf(this.minCompressRatio));
         }

         return options;
      }
   }

   public String chunkLengthInKB() {
      return String.valueOf(this.chunkLength() / 1024);
   }

   public void setCrcCheckChance(double crcCheckChance) {
      this.crcCheckChance = crcCheckChance;
   }

   public double getCrcCheckChance() {
      return this.crcCheckChance;
   }

   public boolean shouldCheckCrc() {
      double checkChance = this.getCrcCheckChance();
      return checkChance >= 1.0D || checkChance > 0.0D && checkChance > ThreadLocalRandom.current().nextDouble();
   }

   public boolean equals(Object obj) {
      if(obj == this) {
         return true;
      } else if(obj != null && obj.getClass() == this.getClass()) {
         CompressionParams cp = (CompressionParams)obj;
         return (new EqualsBuilder()).append(this.sstableCompressor, cp.sstableCompressor).append(this.chunkLength(), cp.chunkLength()).append(this.otherOptions, cp.otherOptions).isEquals();
      } else {
         return false;
      }
   }

   public int hashCode() {
      return (new HashCodeBuilder(29, 1597)).append(this.sstableCompressor).append(this.chunkLength()).append(this.otherOptions).toHashCode();
   }

   static class CompressionParmsSerializer extends VersionDependent<StreamMessage.StreamVersion> implements Serializer<CompressionParams> {
      CompressionParmsSerializer(StreamMessage.StreamVersion version) {
         super(version);
      }

      public void serialize(CompressionParams parameters, DataOutputPlus out) throws IOException {
         out.writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
         out.writeInt(parameters.otherOptions.size());
         UnmodifiableIterator var3 = parameters.otherOptions.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<String, String> entry = (Entry)var3.next();
            out.writeUTF((String)entry.getKey());
            out.writeUTF((String)entry.getValue());
         }

         out.writeInt(parameters.chunkLength());
         if(((StreamMessage.StreamVersion)this.version).compareTo(StreamMessage.StreamVersion.OSS_40) >= 0) {
            out.writeInt(parameters.maxCompressedLength);
         } else if(parameters.maxCompressedLength != 2147483647) {
            throw new UnsupportedOperationException("Cannot stream SSTables with uncompressed chunks to pre-4.0 nodes.");
         }

      }

      public CompressionParams deserialize(DataInputPlus in) throws IOException {
         String compressorName = in.readUTF();
         int optionCount = in.readInt();
         Map<String, String> options = new HashMap();

         int chunkLength;
         for(chunkLength = 0; chunkLength < optionCount; ++chunkLength) {
            String key = in.readUTF();
            String value = in.readUTF();
            options.put(key, value);
         }

         chunkLength = in.readInt();
         int minCompressRatio = 2147483647;
         if(((StreamMessage.StreamVersion)this.version).compareTo(StreamMessage.StreamVersion.OSS_40) >= 0) {
            minCompressRatio = in.readInt();
         }

         try {
            CompressionParams parameters = new CompressionParams(compressorName, chunkLength, minCompressRatio, options);
            return parameters;
         } catch (ConfigurationException var9) {
            throw new RuntimeException("Cannot create CompressionParams for parameters", var9);
         }
      }

      public long serializedSize(CompressionParams parameters) {
         long size = (long)TypeSizes.sizeof(parameters.sstableCompressor.getClass().getSimpleName());
         size += (long)TypeSizes.sizeof(parameters.otherOptions.size());

         Entry entry;
         for(UnmodifiableIterator var4 = parameters.otherOptions.entrySet().iterator(); var4.hasNext(); size += (long)TypeSizes.sizeof((String)entry.getValue())) {
            entry = (Entry)var4.next();
            size += (long)TypeSizes.sizeof((String)entry.getKey());
         }

         size += (long)TypeSizes.sizeof(parameters.chunkLength());
         if(((StreamMessage.StreamVersion)this.version).compareTo(StreamMessage.StreamVersion.OSS_40) >= 0) {
            size += (long)TypeSizes.sizeof(parameters.maxCompressedLength());
         }

         return size;
      }
   }
}
