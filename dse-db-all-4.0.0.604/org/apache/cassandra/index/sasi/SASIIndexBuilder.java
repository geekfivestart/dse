package org.apache.cassandra.index.sasi;

import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

class SASIIndexBuilder extends SecondaryIndexBuilder {
   private final ColumnFamilyStore cfs;
   private final UUID compactionId = UUIDGen.getTimeUUID();
   private final SortedMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> sstables;
   private long bytesProcessed = 0L;
   private final long totalSizeInBytes;

   public SASIIndexBuilder(ColumnFamilyStore cfs, SortedMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> sstables) {
      long totalIndexBytes = 0L;

      SSTableReader sstable;
      for(Iterator var5 = sstables.keySet().iterator(); var5.hasNext(); totalIndexBytes += this.getDataLength(sstable)) {
         sstable = (SSTableReader)var5.next();
      }

      this.cfs = cfs;
      this.sstables = sstables;
      this.totalSizeInBytes = totalIndexBytes;
   }

   public void build() {
      AbstractType<?> keyValidator = this.cfs.metadata().partitionKeyType;
      Iterator var2 = this.sstables.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> e = (Entry)var2.next();
         SSTableReader sstable = (SSTableReader)e.getKey();
         Multimap<ColumnMetadata, ColumnIndex> indexes = (Multimap)e.getValue();
         RandomAccessReader dataFile = sstable.openDataReader();
         Throwable var7 = null;

         try {
            PerSSTableIndexWriter indexWriter = SASIIndex.newWriter(keyValidator, sstable.descriptor, indexes, OperationType.COMPACTION);
            long previousDataPosition = 0L;

            try {
               PartitionIndexIterator keys = sstable.allKeysIterator();
               Throwable var12 = null;

               try {
                  while(keys.key() != null) {
                     if(this.isStopRequested()) {
                        throw new CompactionInterruptedException(this.getCompactionInfo());
                     }

                     DecoratedKey key = keys.key();
                     long dataPosition = keys.dataPosition();
                     indexWriter.startPartition(key, dataPosition);

                     try {
                        dataFile.seek(dataPosition);
                        ByteBufferUtil.skipShortLength(dataFile);
                        SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key);
                        Throwable var17 = null;

                        try {
                           if(this.cfs.metadata().hasStaticColumns()) {
                              indexWriter.nextUnfilteredCluster(partition.staticRow());
                           }

                           while(partition.hasNext()) {
                              indexWriter.nextUnfilteredCluster(partition.next());
                           }
                        } catch (Throwable var67) {
                           var17 = var67;
                           throw var67;
                        } finally {
                           if(partition != null) {
                              if(var17 != null) {
                                 try {
                                    partition.close();
                                 } catch (Throwable var66) {
                                    var17.addSuppressed(var66);
                                 }
                              } else {
                                 partition.close();
                              }
                           }

                        }
                     } catch (IOException var69) {
                        throw new FSReadError(var69, sstable.getFilename());
                     }

                     this.bytesProcessed += dataPosition - previousDataPosition;
                     previousDataPosition = dataPosition;
                     keys.advance();
                  }

                  this.completeSSTable(indexWriter, sstable, indexes.values());
               } catch (Throwable var70) {
                  var12 = var70;
                  throw var70;
               } finally {
                  if(keys != null) {
                     if(var12 != null) {
                        try {
                           keys.close();
                        } catch (Throwable var65) {
                           var12.addSuppressed(var65);
                        }
                     } else {
                        keys.close();
                     }
                  }

               }
            } catch (IOException var72) {
               throw new FSReadError(var72, sstable.getFilename());
            }
         } catch (Throwable var73) {
            var7 = var73;
            throw var73;
         } finally {
            if(dataFile != null) {
               if(var7 != null) {
                  try {
                     dataFile.close();
                  } catch (Throwable var64) {
                     var7.addSuppressed(var64);
                  }
               } else {
                  dataFile.close();
               }
            }

         }
      }

   }

   public CompactionInfo getCompactionInfo() {
      return new CompactionInfo(this.cfs.metadata(), OperationType.INDEX_BUILD, this.bytesProcessed, this.totalSizeInBytes, this.compactionId);
   }

   private long getDataLength(SSTable sstable) {
      File primaryIndex = new File(sstable.getFilename());
      return primaryIndex.exists()?primaryIndex.length():0L;
   }

   private void completeSSTable(PerSSTableIndexWriter indexWriter, SSTableReader sstable, Collection<ColumnIndex> indexes) {
      indexWriter.complete();
      Iterator var4 = indexes.iterator();

      while(var4.hasNext()) {
         ColumnIndex index = (ColumnIndex)var4.next();
         File tmpIndex = new File(sstable.descriptor.filenameFor(index.getComponent()));
         if(tmpIndex.exists()) {
            index.update(Collections.emptyList(), Collections.singletonList(sstable));
         }
      }

   }
}
