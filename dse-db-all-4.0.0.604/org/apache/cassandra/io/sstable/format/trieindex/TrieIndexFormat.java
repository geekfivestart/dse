package org.apache.cassandra.io.sstable.format.trieindex;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;

public class TrieIndexFormat implements SSTableFormat {
   public static final TrieIndexFormat instance = new TrieIndexFormat();
   public static final Version latestVersion = new TrieIndexFormat.TrieIndexVersion("aa");
   static final TrieIndexFormat.ReaderFactory readerFactory = new TrieIndexFormat.ReaderFactory();
   static final TrieIndexFormat.WriterFactory writerFactory = new TrieIndexFormat.WriterFactory();
   private static final Pattern VALIDATION = Pattern.compile("[a-z]+");
   static Set<Component> REQUIRED_COMPONENTS;

   private TrieIndexFormat() {
   }

   public Version getLatestVersion() {
      return latestVersion;
   }

   public Version getVersion(String version) {
      return new TrieIndexFormat.TrieIndexVersion(version);
   }

   public boolean validateVersion(String ver) {
      return ver != null && VALIDATION.matcher(ver).matches();
   }

   public SSTableWriter.Factory getWriterFactory() {
      return writerFactory;
   }

   public SSTableReader.Factory getReaderFactory() {
      return readerFactory;
   }

   static {
      REQUIRED_COMPONENTS = ImmutableSet.of(Component.DATA, Component.PARTITION_INDEX, Component.ROW_INDEX, Component.STATS);
   }

   static class TrieIndexVersion extends Version {
      public static final String current_version = "aa";
      public static final String earliest_supported_version = "aa";
      public static final EncodingVersion latestVersion = EncodingVersion.last();
      private final boolean isLatestVersion;

      TrieIndexVersion(String version) {
         super(TrieIndexFormat.instance, version);
         this.isLatestVersion = version.compareTo("aa") == 0;
      }

      public boolean isLatestVersion() {
         return this.isLatestVersion;
      }

      public boolean hasCommitLogLowerBound() {
         return true;
      }

      public boolean hasCommitLogIntervals() {
         return true;
      }

      public boolean hasMaxCompressedLength() {
         return true;
      }

      public boolean hasPendingRepair() {
         return true;
      }

      public boolean hasMetadataChecksum() {
         return true;
      }

      public EncodingVersion encodingVersion() {
         return latestVersion;
      }

      public boolean isCompatible() {
         return this.version.compareTo("aa") >= 0 && this.version.charAt(0) <= "aa".charAt(0);
      }

      public boolean isCompatibleForStreaming() {
         return this.isCompatible() && this.version.charAt(0) == "aa".charAt(0);
      }
   }

   static class ReaderFactory extends SSTableReader.Factory {
      ReaderFactory() {
      }

      public TrieIndexSSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
         return new TrieIndexSSTableReader(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
      }

      public PartitionIndexIterator keyIterator(Descriptor desc, TableMetadata metadata) {
         IPartitioner partitioner = metadata.partitioner;
         boolean compressedData = (new File(desc.filenameFor(Component.COMPRESSION_INFO))).exists();

         try {
            FileHandle.Builder piBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.PARTITION_INDEX);
            Throwable var6 = null;

            Object var17;
            try {
               FileHandle.Builder riBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.ROW_INDEX);
               Throwable var8 = null;

               try {
                  FileHandle.Builder dBuilder = SSTableReader.dataFileHandleBuilder(desc, metadata, compressedData);
                  Throwable var10 = null;

                  try {
                     PartitionIndex index = PartitionIndex.load(piBuilder, partitioner, false);
                     Throwable var12 = null;

                     try {
                        FileHandle dFile = dBuilder.complete();
                        Throwable var14 = null;

                        try {
                           FileHandle riFile = riBuilder.complete();
                           Throwable var16 = null;

                           try {
                              var17 = (new PartitionIterator(index.sharedCopy(), partitioner, riFile.sharedCopy(), dFile.sharedCopy(), Rebufferer.ReaderConstraint.NONE)).closeHandles();
                           } catch (Throwable var172) {
                              var17 = var172;
                              var16 = var172;
                              throw var172;
                           } finally {
                              if(riFile != null) {
                                 if(var16 != null) {
                                    try {
                                       riFile.close();
                                    } catch (Throwable var171) {
                                       var16.addSuppressed(var171);
                                    }
                                 } else {
                                    riFile.close();
                                 }
                              }

                           }
                        } catch (Throwable var174) {
                           var14 = var174;
                           throw var174;
                        } finally {
                           if(dFile != null) {
                              if(var14 != null) {
                                 try {
                                    dFile.close();
                                 } catch (Throwable var170) {
                                    var14.addSuppressed(var170);
                                 }
                              } else {
                                 dFile.close();
                              }
                           }

                        }
                     } catch (Throwable var176) {
                        var12 = var176;
                        throw var176;
                     } finally {
                        if(index != null) {
                           if(var12 != null) {
                              try {
                                 index.close();
                              } catch (Throwable var169) {
                                 var12.addSuppressed(var169);
                              }
                           } else {
                              index.close();
                           }
                        }

                     }
                  } catch (Throwable var178) {
                     var10 = var178;
                     throw var178;
                  } finally {
                     if(dBuilder != null) {
                        if(var10 != null) {
                           try {
                              dBuilder.close();
                           } catch (Throwable var168) {
                              var10.addSuppressed(var168);
                           }
                        } else {
                           dBuilder.close();
                        }
                     }

                  }
               } catch (Throwable var180) {
                  var8 = var180;
                  throw var180;
               } finally {
                  if(riBuilder != null) {
                     if(var8 != null) {
                        try {
                           riBuilder.close();
                        } catch (Throwable var167) {
                           var8.addSuppressed(var167);
                        }
                     } else {
                        riBuilder.close();
                     }
                  }

               }
            } catch (Throwable var182) {
               var6 = var182;
               throw var182;
            } finally {
               if(piBuilder != null) {
                  if(var6 != null) {
                     try {
                        piBuilder.close();
                     } catch (Throwable var166) {
                        var6.addSuppressed(var166);
                     }
                  } else {
                     piBuilder.close();
                  }
               }

            }

            return (PartitionIndexIterator)var17;
         } catch (IOException var184) {
            throw new RuntimeException(var184);
         }
      }

      public Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException {
         File indexFile = new File(descriptor.filenameFor(Component.PARTITION_INDEX));
         if(!indexFile.exists()) {
            return null;
         } else {
            FileHandle.Builder fhBuilder = SSTableReader.indexFileHandleBuilder(descriptor, TableMetadata.minimal(descriptor.ksname, descriptor.cfname), Component.PARTITION_INDEX);
            Throwable var5 = null;

            Object var8;
            try {
               PartitionIndex pIndex = PartitionIndex.load(fhBuilder, partitioner, false);
               Throwable var7 = null;

               try {
                  var8 = Pair.create(pIndex.firstKey(), pIndex.lastKey());
               } catch (Throwable var31) {
                  var8 = var31;
                  var7 = var31;
                  throw var31;
               } finally {
                  if(pIndex != null) {
                     if(var7 != null) {
                        try {
                           pIndex.close();
                        } catch (Throwable var30) {
                           var7.addSuppressed(var30);
                        }
                     } else {
                        pIndex.close();
                     }
                  }

               }
            } catch (Throwable var33) {
               var5 = var33;
               throw var33;
            } finally {
               if(fhBuilder != null) {
                  if(var5 != null) {
                     try {
                        fhBuilder.close();
                     } catch (Throwable var29) {
                        var5.addSuppressed(var29);
                     }
                  } else {
                     fhBuilder.close();
                  }
               }

            }

            return (Pair)var8;
         }
      }

      public Set<Component> requiredComponents() {
         return TrieIndexFormat.REQUIRED_COMPONENTS;
      }
   }

   static class WriterFactory extends SSTableWriter.Factory {
      WriterFactory() {
      }

      public SSTableWriter open(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, TableMetadataRef metadata, MetadataCollector metadataCollector, SerializationHeader header, Collection<SSTableFlushObserver> observers, LifecycleTransaction txn) {
         return new TrieIndexSSTableWriter(descriptor, keyCount, repairedAt, pendingRepair, metadata, metadataCollector, header, observers, txn);
      }
   }
}
