package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.json.simple.JSONValue;

public class CommitLogDescriptor {
   private static final String SEPARATOR = "-";
   private static final String FILENAME_PREFIX = "CommitLog-";
   private static final String FILENAME_EXTENSION = ".log";
   private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog-((\\d+)(-\\d+)?).log");
   static final String COMPRESSION_PARAMETERS_KEY = "compressionParameters";
   static final String COMPRESSION_CLASS_KEY = "compressionClass";
   @VisibleForTesting
   public static final CommitLogDescriptor.CommitLogVersion current_version;
   final CommitLogDescriptor.CommitLogVersion version;
   public final long id;
   public final ParameterizedClass compression;
   private final EncryptionContext encryptionContext;

   public CommitLogDescriptor(CommitLogDescriptor.CommitLogVersion version, long id, ParameterizedClass compression, EncryptionContext encryptionContext) {
      this.version = version;
      this.id = id;
      this.compression = compression;
      this.encryptionContext = encryptionContext;
   }

   public CommitLogDescriptor(long id, ParameterizedClass compression, EncryptionContext encryptionContext) {
      this(current_version, id, compression, encryptionContext);
   }

   public static void writeHeader(ByteBuffer out, CommitLogDescriptor descriptor) {
      writeHeader(out, descriptor, Collections.emptyMap());
   }

   public static void writeHeader(ByteBuffer out, CommitLogDescriptor descriptor, Map<String, String> additionalHeaders) {
      CRC32 crc = new CRC32();
      out.putInt(descriptor.version.code);
      FBUtilities.updateChecksumInt(crc, descriptor.version.code);
      out.putLong(descriptor.id);
      FBUtilities.updateChecksumInt(crc, (int)(descriptor.id & 4294967295L));
      FBUtilities.updateChecksumInt(crc, (int)(descriptor.id >>> 32));
      String parametersString = constructParametersString(descriptor.compression, descriptor.encryptionContext, additionalHeaders);
      byte[] parametersBytes = parametersString.getBytes(StandardCharsets.UTF_8);
      if(parametersBytes.length != ((short)parametersBytes.length & '\uffff')) {
         throw new ConfigurationException(String.format("Compression parameters too long, length %d cannot be above 65535.", new Object[]{Integer.valueOf(parametersBytes.length)}));
      } else {
         out.putShort((short)parametersBytes.length);
         FBUtilities.updateChecksumInt(crc, parametersBytes.length);
         out.put(parametersBytes);
         crc.update(parametersBytes, 0, parametersBytes.length);
         out.putInt((int)crc.getValue());
      }
   }

   @VisibleForTesting
   static String constructParametersString(ParameterizedClass compression, EncryptionContext encryptionContext, Map<String, String> additionalHeaders) {
      Map<String, Object> params = new TreeMap();
      if(compression != null) {
         params.put("compressionParameters", compression.parameters);
         params.put("compressionClass", compression.class_name);
      }

      if(encryptionContext != null) {
         params.putAll(encryptionContext.toHeaderParameters());
      }

      params.putAll(additionalHeaders);
      return JSONValue.toJSONString(params);
   }

   public static CommitLogDescriptor fromHeader(File file, EncryptionContext encryptionContext) {
      try {
         RandomAccessFile raf = new RandomAccessFile(file, "r");
         Throwable var3 = null;

         CommitLogDescriptor var4;
         try {
            assert raf.getFilePointer() == 0L;

            var4 = readHeader(raf, encryptionContext);
         } catch (Throwable var15) {
            var3 = var15;
            throw var15;
         } finally {
            if(raf != null) {
               if(var3 != null) {
                  try {
                     raf.close();
                  } catch (Throwable var14) {
                     var3.addSuppressed(var14);
                  }
               } else {
                  raf.close();
               }
            }

         }

         return var4;
      } catch (EOFException var17) {
         throw new RuntimeException(var17);
      } catch (IOException var18) {
         throw new FSReadError(var18, file);
      }
   }

   public static CommitLogDescriptor readHeader(DataInput input, EncryptionContext encryptionContext) throws IOException {
      CRC32 checkcrc = new CRC32();
      int rawVersion = input.readInt();
      CommitLogDescriptor.CommitLogVersion version = CommitLogDescriptor.CommitLogVersion.fromCode(rawVersion);
      FBUtilities.updateChecksumInt(checkcrc, rawVersion);
      long id = input.readLong();
      FBUtilities.updateChecksumInt(checkcrc, (int)(id & 4294967295L));
      FBUtilities.updateChecksumInt(checkcrc, (int)(id >>> 32));
      int parametersLength = input.readShort() & '\uffff';
      FBUtilities.updateChecksumInt(checkcrc, parametersLength);
      byte[] parametersBytes = new byte[parametersLength];
      input.readFully(parametersBytes);
      checkcrc.update(parametersBytes, 0, parametersBytes.length);
      int crc = input.readInt();
      if(crc == (int)checkcrc.getValue()) {
         Map<?, ?> map = (Map)JSONValue.parse(new String(parametersBytes, StandardCharsets.UTF_8));
         return new CommitLogDescriptor(version, id, parseCompression(map), EncryptionContext.createFromMap(map, encryptionContext));
      } else {
         return null;
      }
   }

   @VisibleForTesting
   static ParameterizedClass parseCompression(Map<?, ?> params) {
      if(params != null && !params.isEmpty()) {
         String className = (String)params.get("compressionClass");
         if(className == null) {
            return null;
         } else {
            Map<String, String> cparams = (Map)params.get("compressionParameters");
            return new ParameterizedClass(className, cparams);
         }
      } else {
         return null;
      }
   }

   public static CommitLogDescriptor fromFileName(String name) {
      Matcher matcher;
      if(!(matcher = COMMIT_LOG_FILE_PATTERN.matcher(name)).matches()) {
         throw new RuntimeException("Cannot parse the version of the file: " + name);
      } else if(matcher.group(3) == null) {
         throw new UnsupportedOperationException("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
      } else {
         long id = Long.parseLong(matcher.group(3).split("-")[1]);
         return new CommitLogDescriptor(CommitLogDescriptor.CommitLogVersion.fromCode(Integer.parseInt(matcher.group(2))), id, (ParameterizedClass)null, new EncryptionContext());
      }
   }

   public String fileName() {
      return "CommitLog-" + this.version.code + "-" + this.id + ".log";
   }

   public static boolean isValid(String filename) {
      return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
   }

   public EncryptionContext getEncryptionContext() {
      return this.encryptionContext;
   }

   public String toString() {
      return "(" + this.version + "," + this.id + (this.compression != null?"," + this.compression:"") + ")";
   }

   public boolean equals(Object that) {
      return that instanceof CommitLogDescriptor && this.equals((CommitLogDescriptor)that);
   }

   public boolean equalsIgnoringCompression(CommitLogDescriptor that) {
      return this.version == that.version && this.id == that.id;
   }

   public boolean equals(CommitLogDescriptor that) {
      return this.equalsIgnoringCompression(that) && Objects.equals(this.compression, that.compression) && Objects.equals(this.encryptionContext, that.encryptionContext);
   }

   static {
      current_version = CommitLogDescriptor.CommitLogVersion.DSE_60;
   }

   public static enum CommitLogVersion implements Version<CommitLogDescriptor.CommitLogVersion> {
      OSS_30(6, EncodingVersion.OSS_30),
      DSE_60(600, EncodingVersion.OSS_30);

      final int code;
      final EncodingVersion encodingVersion;

      private CommitLogVersion(int code, EncodingVersion encodingVersion) {
         this.code = code;
         this.encodingVersion = encodingVersion;
      }

      static CommitLogDescriptor.CommitLogVersion fromCode(int code) {
         CommitLogDescriptor.CommitLogVersion[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            CommitLogDescriptor.CommitLogVersion version = var1[var3];
            if(version.code == code) {
               return version;
            }
         }

         throw new IllegalArgumentException(String.format("Unsupported commit log version %d found; cannot read.", new Object[]{Integer.valueOf(code)}));
      }
   }
}
