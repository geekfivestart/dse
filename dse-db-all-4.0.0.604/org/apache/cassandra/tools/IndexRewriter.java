package org.apache.cassandra.tools;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndexBuilder;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class IndexRewriter {
   private static final String KEY_RENAME = "r";
   private static final Options options = new Options();
   private static CommandLine cmd;

   public IndexRewriter() {
   }

   public static void main(String[] args) throws ConfigurationException {
      PosixParser parser = new PosixParser();

      try {
         cmd = parser.parse(options, args);
      } catch (ParseException var6) {
         System.err.println(var6.getMessage());
         printUsage();
         System.exit(1);
      }

      if(cmd.getArgs().length != 1) {
         System.err.println("You must supply exactly one sstable");
         printUsage();
         System.exit(1);
      }

      String ssTableFileName = (new File(cmd.getArgs()[0])).getAbsolutePath();
      File srcFile = new File(ssTableFileName);
      if(!srcFile.exists()) {
         System.err.println("Cannot find file " + ssTableFileName);
         System.exit(1);
      }

      try {
         if(srcFile.isFile()) {
            rewriteIndex(ssTableFileName);
         } else {
            Iterator var4 = ((Set)Arrays.stream(srcFile.listFiles()).filter(File::isFile).map(File::getPath).map(Descriptor::fromFilename).collect(Collectors.toSet())).iterator();

            while(var4.hasNext()) {
               Descriptor desc = (Descriptor)var4.next();
               rewriteIndex(desc.filenameFor(Component.PARTITION_INDEX));
            }
         }
      } catch (IOException var7) {
         var7.printStackTrace(System.err);
      }

      System.exit(0);
   }

   static void rewriteIndex(String ssTableFileName) throws IOException {
      System.out.format("Rewriting %s\n", new Object[]{ssTableFileName});
      Descriptor desc = Descriptor.fromFilename(ssTableFileName);
      TableMetadata metadata = Util.metadataFromSSTable(desc);
      File destFile = new File(desc.filenameFor(Component.PARTITION_INDEX) + ".rewrite");
      SequentialWriter writer = new SequentialWriter(destFile);
      Throwable var5 = null;

      try {
         PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, (FileHandle.Builder)null);
         Throwable var7 = null;

         try {
            IndexRewriter.KeyIterator iter = IndexRewriter.KeyIterator.create(desc, metadata);
            Throwable var9 = null;

            try {
               int nextPrint = 1;

               while(iter.hasNext()) {
                  DecoratedKey key = iter.next();
                  long pos = iter.getIndexPosition();
                  builder.addEntry(key, pos);
                  if(iter.getBytesRead() * 100L / iter.getTotalBytes() >= (long)nextPrint) {
                     System.out.print('.');
                     if(nextPrint % 10 == 0) {
                        System.out.print(nextPrint + "%\n");
                     }

                     ++nextPrint;
                  }
               }

               builder.complete();
               if(cmd.hasOption("r")) {
                  File srcFile = new File(desc.filenameFor(Component.PARTITION_INDEX));
                  srcFile.renameTo(new File(srcFile.getPath() + ".original"));
                  destFile.renameTo(srcFile);
                  System.out.print(".Renamed");
               }

               System.out.println(".Done");
            } catch (Throwable var56) {
               var9 = var56;
               throw var56;
            } finally {
               if(iter != null) {
                  if(var9 != null) {
                     try {
                        iter.close();
                     } catch (Throwable var55) {
                        var9.addSuppressed(var55);
                     }
                  } else {
                     iter.close();
                  }
               }

            }
         } catch (Throwable var58) {
            var7 = var58;
            throw var58;
         } finally {
            if(builder != null) {
               if(var7 != null) {
                  try {
                     builder.close();
                  } catch (Throwable var54) {
                     var7.addSuppressed(var54);
                  }
               } else {
                  builder.close();
               }
            }

         }
      } catch (Throwable var60) {
         var5 = var60;
         throw var60;
      } finally {
         if(writer != null) {
            if(var5 != null) {
               try {
                  writer.close();
               } catch (Throwable var53) {
                  var5.addSuppressed(var53);
               }
            } else {
               writer.close();
            }
         }

      }

   }

   private static void printUsage() {
      String usage = String.format("IndexRewriter <options> <sstable file or directory>%n", new Object[0]);
      String header = "Recreate a partition index trie.";
      (new HelpFormatter()).printHelp(usage, header, options, "");
   }

   static {
      DatabaseDescriptor.toolInitialization();
      Option optKey = new Option("r", false, "Rename files after completion so that rewritten one becomes active.");
      options.addOption(optKey);
   }

   static class KeyIterator extends PartitionIndex.IndexPosIterator implements CloseableIterator<DecoratedKey> {
      private final PartitionIndex index;
      private final FileHandle dFile;
      private final FileHandle riFile;
      private final IPartitioner partitioner;
      private long indexPosition;
      private long dataPosition;

      public static IndexRewriter.KeyIterator create(Descriptor desc, TableMetadata metadata) {
         IPartitioner partitioner = metadata.partitioner;
         boolean compressedData = (new File(desc.filenameFor(Component.COMPRESSION_INFO))).exists();

         try {
            FileHandle.Builder piBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.PARTITION_INDEX);
            Throwable var5 = null;

            IndexRewriter.KeyIterator var13;
            try {
               FileHandle.Builder riBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.ROW_INDEX);
               Throwable var7 = null;

               try {
                  FileHandle.Builder dBuilder = SSTableReader.dataFileHandleBuilder(desc, metadata, compressedData);
                  Throwable var9 = null;

                  try {
                     PartitionIndex index = PartitionIndex.load(piBuilder, partitioner, false);
                     FileHandle dFile = dBuilder.complete();
                     FileHandle riFile = riBuilder.complete();
                     var13 = new IndexRewriter.KeyIterator(index, dFile, riFile, partitioner, Rebufferer.ReaderConstraint.NONE);
                  } catch (Throwable var60) {
                     var9 = var60;
                     throw var60;
                  } finally {
                     if(dBuilder != null) {
                        if(var9 != null) {
                           try {
                              dBuilder.close();
                           } catch (Throwable var59) {
                              var9.addSuppressed(var59);
                           }
                        } else {
                           dBuilder.close();
                        }
                     }

                  }
               } catch (Throwable var62) {
                  var7 = var62;
                  throw var62;
               } finally {
                  if(riBuilder != null) {
                     if(var7 != null) {
                        try {
                           riBuilder.close();
                        } catch (Throwable var58) {
                           var7.addSuppressed(var58);
                        }
                     } else {
                        riBuilder.close();
                     }
                  }

               }
            } catch (Throwable var64) {
               var5 = var64;
               throw var64;
            } finally {
               if(piBuilder != null) {
                  if(var5 != null) {
                     try {
                        piBuilder.close();
                     } catch (Throwable var57) {
                        var5.addSuppressed(var57);
                     }
                  } else {
                     piBuilder.close();
                  }
               }

            }

            return var13;
         } catch (IOException var66) {
            throw new RuntimeException(var66);
         }
      }

      private KeyIterator(PartitionIndex index, FileHandle dFile, FileHandle riFile, IPartitioner partitioner, Rebufferer.ReaderConstraint rc) {
         super(index, rc);
         this.partitioner = partitioner;
         this.dFile = dFile;
         this.riFile = riFile;
         this.index = index;
      }

      public boolean hasNext() {
         try {
            this.indexPosition = this.nextIndexPos();
            return this.indexPosition != -9223372036854775808L;
         } catch (IOException var2) {
            throw new RuntimeException(var2);
         }
      }

      public DecoratedKey next() {
         Rebufferer.ReaderConstraint rc = Rebufferer.ReaderConstraint.NONE;

         assert this.indexPosition != -9223372036854775808L;

         try {
            FileDataInput in = this.indexPosition >= 0L?this.riFile.createReader(this.indexPosition, rc):this.dFile.createReader(~this.indexPosition, rc);
            Throwable var3 = null;

            DecoratedKey var5;
            try {
               DecoratedKey key = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
               if(this.indexPosition >= 0L) {
                  this.dataPosition = TrieIndexEntry.deserialize(in, in.getFilePointer()).position;
               } else {
                  this.dataPosition = ~this.indexPosition;
               }

               var5 = key;
            } catch (Throwable var15) {
               var3 = var15;
               throw var15;
            } finally {
               if(in != null) {
                  if(var3 != null) {
                     try {
                        in.close();
                     } catch (Throwable var14) {
                        var3.addSuppressed(var14);
                     }
                  } else {
                     in.close();
                  }
               }

            }

            return var5;
         } catch (IOException var17) {
            throw new RuntimeException(var17);
         }
      }

      public void close() {
         FileUtils.closeQuietly((Closeable)this.index);
         FileUtils.closeQuietly((AutoCloseable)this.dFile);
         FileUtils.closeQuietly((AutoCloseable)this.riFile);
      }

      public long getBytesRead() {
         return this.dataPosition;
      }

      public long getTotalBytes() {
         return this.dFile.dataLength();
      }

      public long getIndexPosition() {
         return this.indexPosition;
      }

      public long getDataPosition() {
         return this.dataPosition;
      }
   }
}
