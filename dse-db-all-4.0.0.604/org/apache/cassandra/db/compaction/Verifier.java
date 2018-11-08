package org.apache.cassandra.db.compaction;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongPredicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

public class Verifier implements Closeable {
   private final SSTableReader sstable;
   private final CompactionController controller;
   private final RandomAccessReader dataFile;
   private final Verifier.VerifyInfo verifyInfo;
   private int goodRows;
   private final OutputHandler outputHandler;
   private DataIntegrityMetadata.FileDigestValidator validator;

   public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, boolean isOffline) {
      this(cfs, sstable, new OutputHandler.LogOutput(), isOffline);
   }

   public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline) {
      this.sstable = sstable;
      this.outputHandler = outputHandler;
      this.controller = new Verifier.VerifyController(cfs);
      this.dataFile = isOffline?sstable.openDataReader(FileAccessType.FULL_FILE):sstable.openDataReader(CompactionManager.instance.getRateLimiter(), FileAccessType.FULL_FILE);
      this.verifyInfo = new Verifier.VerifyInfo(this.dataFile, sstable);
   }

   public void verify(boolean extended) throws IOException {
      long rowStart = 0L;
      this.outputHandler.output(String.format("Verifying %s (%s)", new Object[]{this.sstable, FBUtilities.prettyPrintMemory(this.dataFile.length())}));
      this.outputHandler.output(String.format("Deserializing sstable metadata for %s ", new Object[]{this.sstable}));

      try {
         EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
         Map<MetadataType, MetadataComponent> sstableMetadata = this.sstable.descriptor.getMetadataSerializer().deserialize(this.sstable.descriptor, types);
         if(sstableMetadata.containsKey(MetadataType.VALIDATION) && !((ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION)).partitioner.equals(this.sstable.getPartitioner().getClass().getCanonicalName())) {
            throw new IOException("Partitioner does not match validation metadata");
         }
      } catch (Throwable var68) {
         this.outputHandler.debug(var68.getMessage());
         this.markAndThrow(false);
      }

      this.outputHandler.output(String.format("Checking computed hash of %s ", new Object[]{this.sstable}));

      try {
         this.validator = null;
         if((new File(this.sstable.descriptor.filenameFor(Component.DIGEST))).exists()) {
            this.validator = DataIntegrityMetadata.fileDigestValidator(this.sstable.descriptor);
            this.validator.validate();
         } else {
            this.outputHandler.output("Data digest missing, assuming extended verification of disk values");
            extended = true;
         }
      } catch (IOException var66) {
         this.outputHandler.debug(var66.getMessage());
         this.markAndThrow();
      } finally {
         FileUtils.closeQuietly((Closeable)this.validator);
      }

      if(extended) {
         this.outputHandler.output("Extended Verify requested, proceeding to inspect values");

         try {
            PartitionIndexIterator indexIterator = this.sstable.allKeysIterator();
            Throwable var74 = null;

            try {
               DecoratedKey nextIndexKey = indexIterator.key();
               long firstRowPositionFromIndex = indexIterator.dataPosition();
               if(firstRowPositionFromIndex != 0L) {
                  this.markAndThrow();
               }

               DecoratedKey prevKey = null;

               while(!this.dataFile.isEOF()) {
                  if(this.verifyInfo.isStopRequested()) {
                     throw new CompactionInterruptedException(this.verifyInfo.getCompactionInfo());
                  }

                  rowStart = this.dataFile.getFilePointer();
                  this.outputHandler.debug("Reading row at " + rowStart);
                  DecoratedKey key = null;

                  try {
                     key = this.sstable.decorateKey(ByteBufferUtil.readWithShortLength(this.dataFile));
                  } catch (Throwable var65) {
                     this.throwIfFatal(var65);
                  }

                  long nextRowPositionFromIndex = 0L;

                  try {
                     indexIterator.advance();
                     nextIndexKey = indexIterator.key();
                     nextRowPositionFromIndex = nextIndexKey != null?indexIterator.dataPosition():this.dataFile.length();
                  } catch (Throwable var64) {
                     this.markAndThrow();
                  }

                  long dataStart = this.dataFile.getFilePointer();
                  long dataStartFromIndex = nextIndexKey == null?-1L:rowStart + 2L + (long)nextIndexKey.getKey().remaining();
                  long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                  String keyName = key == null?"(unreadable key)":ByteBufferUtil.bytesToHex(key.getKey());
                  this.outputHandler.debug(String.format("row %s is %s", new Object[]{keyName, FBUtilities.prettyPrintMemory(dataSize)}));
                  if(key == null || dataSize > this.dataFile.length()) {
                     this.markAndThrow();
                  }

                  UnfilteredRowIterator iterator = SSTableIdentityIterator.create(this.sstable, this.dataFile, key);
                  Object var22 = null;
                  if(iterator != null) {
                     if(var22 != null) {
                        try {
                           iterator.close();
                        } catch (Throwable var63) {
                           ((Throwable)var22).addSuppressed(var63);
                        }
                     } else {
                        iterator.close();
                     }
                  }

                  if(prevKey != null && prevKey.compareTo((PartitionPosition)key) > 0 || !key.equals(nextIndexKey) || dataStart != dataStartFromIndex) {
                     this.markAndThrow();
                  }

                  ++this.goodRows;
                  prevKey = key;
                  this.outputHandler.debug(String.format("Row %s at %s valid, moving to next row at %s ", new Object[]{Integer.valueOf(this.goodRows), Long.valueOf(rowStart), Long.valueOf(nextRowPositionFromIndex)}));
                  this.dataFile.seek(nextRowPositionFromIndex);
               }
            } catch (Throwable var69) {
               var74 = var69;
               throw var69;
            } finally {
               if(indexIterator != null) {
                  if(var74 != null) {
                     try {
                        indexIterator.close();
                     } catch (Throwable var62) {
                        var74.addSuppressed(var62);
                     }
                  } else {
                     indexIterator.close();
                  }
               }

            }
         } catch (Throwable var71) {
            this.markAndThrow();
         } finally {
            this.controller.close();
         }

         this.outputHandler.output("Verify of " + this.sstable + " succeeded. All " + this.goodRows + " rows read successfully");
      }
   }

   public void close() {
      FileUtils.closeQuietly((Closeable)this.dataFile);
   }

   private void throwIfFatal(Throwable th) {
      if(th instanceof Error && !(th instanceof AssertionError) && !(th instanceof IOError)) {
         throw (Error)th;
      }
   }

   private void markAndThrow() throws IOException {
      this.markAndThrow(true);
   }

   private void markAndThrow(boolean mutateRepaired) {
      if(mutateRepaired) {
         try {
            this.sstable.descriptor.getMetadataSerializer().mutateRepaired(this.sstable.descriptor, 0L, this.sstable.getSSTableMetadata().pendingRepair);
            this.sstable.reloadSSTableMetadata();
            this.controller.cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(this.sstable));
         } catch (IOException var3) {
            this.outputHandler.output("Error mutating repairedAt for SSTable " + this.sstable.getFilename() + ", as part of markAndThrow");
         }
      }

      throw new CorruptSSTableException(new Exception(String.format("Invalid SSTable %s, please force %srepair", new Object[]{this.sstable.getFilename(), mutateRepaired?"":"a full "})), this.sstable.getFilename());
   }

   public CompactionInfo.Holder getVerifyInfo() {
      return this.verifyInfo;
   }

   private static class VerifyController extends CompactionController {
      public VerifyController(ColumnFamilyStore cfs) {
         super(cfs, 2147483647);
      }

      public LongPredicate getPurgeEvaluator(DecoratedKey key) {
         return (time) -> {
            return false;
         };
      }
   }

   private static class VerifyInfo extends CompactionInfo.Holder {
      private final RandomAccessReader dataFile;
      private final SSTableReader sstable;
      private final UUID verificationCompactionId;

      public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable) {
         this.dataFile = dataFile;
         this.sstable = sstable;
         this.verificationCompactionId = UUIDGen.getTimeUUID();
      }

      public CompactionInfo getCompactionInfo() {
         try {
            return new CompactionInfo(this.sstable.metadata(), OperationType.VERIFY, this.dataFile.getFilePointer(), this.dataFile.length(), this.verificationCompactionId);
         } catch (Exception var2) {
            throw new RuntimeException();
         }
      }
   }
}
