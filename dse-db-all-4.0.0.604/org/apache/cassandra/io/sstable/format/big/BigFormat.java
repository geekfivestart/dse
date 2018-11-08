package org.apache.cassandra.io.sstable.format.big;

import com.google.common.collect.ImmutableSet;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;

public class BigFormat implements SSTableFormat {
   public static final BigFormat instance = new BigFormat();
   public static final Version latestVersion = new BigFormat.BigVersion("mc");
   static final BigFormat.ReaderFactory readerFactory = new BigFormat.ReaderFactory();
   private static final Pattern VALIDATION = Pattern.compile("[a-z]+");
   static Set<Component> REQUIRED_COMPONENTS;

   BigFormat() {
   }

   public Version getLatestVersion() {
      return latestVersion;
   }

   public Version getVersion(String version) {
      return new BigFormat.BigVersion(version);
   }

   public boolean validateVersion(String ver) {
      return ver != null && VALIDATION.matcher(ver).matches();
   }

   public SSTableWriter.Factory getWriterFactory() {
      throw new UnsupportedOperationException("Cannot write SSTables in format BIG.");
   }

   public SSTableReader.Factory getReaderFactory() {
      return readerFactory;
   }

   static {
      REQUIRED_COMPONENTS = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.SUMMARY, Component.STATS);
   }

   static class BigVersion extends Version {
      public static final String current_version = "mc";
      public static final String earliest_supported_version = "ma";
      private final boolean isLatestVersion;
      private final EncodingVersion encodingVersion;
      private final boolean hasCommitLogLowerBound;
      private final boolean hasCommitLogIntervals;
      public final boolean hasMaxCompressedLength;
      private final boolean hasPendingRepair;
      private final boolean hasMetadataChecksum;

      BigVersion(String version) {
         super(BigFormat.instance, version);
         this.isLatestVersion = version.compareTo("mc") == 0;
         this.encodingVersion = EncodingVersion.OSS_30;
         this.hasCommitLogLowerBound = version.compareTo("mb") >= 0;
         this.hasCommitLogIntervals = version.compareTo("mc") >= 0;
         this.hasMaxCompressedLength = version.compareTo("na") >= 0;
         this.hasPendingRepair = version.compareTo("na") >= 0;
         this.hasMetadataChecksum = version.compareTo("na") >= 0;
      }

      public boolean isLatestVersion() {
         return this.isLatestVersion;
      }

      public boolean hasCommitLogLowerBound() {
         return this.hasCommitLogLowerBound;
      }

      public boolean hasCommitLogIntervals() {
         return this.hasCommitLogIntervals;
      }

      public boolean hasPendingRepair() {
         return this.hasPendingRepair;
      }

      public EncodingVersion encodingVersion() {
         return this.encodingVersion;
      }

      public boolean hasMetadataChecksum() {
         return this.hasMetadataChecksum;
      }

      public boolean isCompatible() {
         return this.version.compareTo("ma") >= 0 && this.version.charAt(0) <= "mc".charAt(0);
      }

      public boolean isCompatibleForStreaming() {
         return this.isCompatible() && this.version.charAt(0) == "mc".charAt(0);
      }

      public boolean hasMaxCompressedLength() {
         return this.hasMaxCompressedLength;
      }
   }

   static class ReaderFactory extends SSTableReader.Factory {
      ReaderFactory() {
      }

      public BigTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
         return new BigTableReader(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
      }

      public PartitionIndexIterator keyIterator(Descriptor descriptor, TableMetadata metadata) {
         try {
            FileHandle.Builder iBuilder = SSTableReader.indexFileHandleBuilder(descriptor, metadata, Component.PRIMARY_INDEX);
            Throwable var4 = null;

            Object var7;
            try {
               FileHandle iFile = iBuilder.complete();
               Throwable var6 = null;

               try {
                  var7 = new PartitionIterator(iFile, metadata.partitioner, (BigRowIndexEntry.IndexSerializer)null, descriptor.version);
               } catch (Throwable var32) {
                  var7 = var32;
                  var6 = var32;
                  throw var32;
               } finally {
                  if(iFile != null) {
                     if(var6 != null) {
                        try {
                           iFile.close();
                        } catch (Throwable var31) {
                           var6.addSuppressed(var31);
                        }
                     } else {
                        iFile.close();
                     }
                  }

               }
            } catch (Throwable var34) {
               var4 = var34;
               throw var34;
            } finally {
               if(iBuilder != null) {
                  if(var4 != null) {
                     try {
                        iBuilder.close();
                     } catch (Throwable var30) {
                        var4.addSuppressed(var30);
                     }
                  } else {
                     iBuilder.close();
                  }
               }

            }

            return (PartitionIndexIterator)var7;
         } catch (IOException var36) {
            throw new RuntimeException(var36);
         }
      }

      public Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException {
         File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
         if(!summariesFile.exists()) {
            return null;
         } else {
            DataInputStream iStream = new DataInputStream(Files.newInputStream(summariesFile.toPath(), new OpenOption[0]));
            Throwable var5 = null;

            Pair var6;
            try {
               var6 = (new IndexSummary.IndexSummarySerializer()).deserializeFirstLastKey(iStream, partitioner);
            } catch (Throwable var15) {
               var5 = var15;
               throw var15;
            } finally {
               if(iStream != null) {
                  if(var5 != null) {
                     try {
                        iStream.close();
                     } catch (Throwable var14) {
                        var5.addSuppressed(var14);
                     }
                  } else {
                     iStream.close();
                  }
               }

            }

            return var6;
         }
      }

      public Set<Component> requiredComponents() {
         return BigFormat.REQUIRED_COMPONENTS;
      }
   }
}
