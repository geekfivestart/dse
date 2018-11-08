package org.apache.cassandra.db.compaction;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.LongPredicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

public class Upgrader {
   private final ColumnFamilyStore cfs;
   private final SSTableReader sstable;
   private final LifecycleTransaction transaction;
   private final File directory;
   private final CompactionController controller;
   private final CompactionStrategyManager strategyManager;
   private final long estimatedRows;
   private final OutputHandler outputHandler;
   private final SSTableFormat.Type upgradeToType;

   public Upgrader(ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler) {
      this(cfs, txn, outputHandler, SSTableFormat.Type.current());
   }

   public Upgrader(ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler, SSTableFormat.Type upgradeToType) {
      this.cfs = cfs;
      this.transaction = txn;
      this.sstable = txn.onlyOne();
      this.outputHandler = outputHandler;
      this.directory = (new File(this.sstable.getFilename())).getParentFile();
      this.controller = new Upgrader.UpgradeController(cfs);
      this.upgradeToType = upgradeToType;
      this.strategyManager = cfs.getCompactionStrategyManager();
      long estimatedTotalKeys = Math.max((long)cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(Arrays.asList(new SSTableReader[]{this.sstable})));
      long estimatedSSTables = Math.max(1L, SSTableReader.getTotalBytes(Arrays.asList(new SSTableReader[]{this.sstable})) / this.strategyManager.getMaxSSTableBytes());
      this.estimatedRows = (long)Math.ceil((double)estimatedTotalKeys / (double)estimatedSSTables);
   }

   private SSTableWriter createCompactionWriter(long repairedAt, UUID parentRepair) {
      MetadataCollector sstableMetadataCollector = new MetadataCollector(this.cfs.getComparator());
      sstableMetadataCollector.sstableLevel(this.sstable.getSSTableLevel());
      return SSTableWriter.create(this.cfs.newSSTableDescriptor(this.directory, this.upgradeToType), Long.valueOf(this.estimatedRows), Long.valueOf(repairedAt), parentRepair, this.cfs.metadata, sstableMetadataCollector, SerializationHeader.make(this.cfs.metadata(), Sets.newHashSet(new SSTableReader[]{this.sstable})), this.cfs.indexManager.listIndexes(), this.transaction);
   }

   public void upgrade(boolean keepOriginals) {
      this.outputHandler.output("Upgrading " + this.sstable);
      int nowInSec = FBUtilities.nowInSeconds();

      try {
         SSTableRewriter writer = SSTableRewriter.construct(this.cfs, this.transaction, keepOriginals, CompactionTask.getMaxDataAge(this.transaction.originals()));
         Throwable var4 = null;

         try {
            AbstractCompactionStrategy.ScannerList scanners = this.strategyManager.getScanners(this.transaction.originals());
            Throwable var6 = null;

            try {
               CompactionIterator iter = new CompactionIterator(this.transaction.opType(), scanners.scanners, this.controller, nowInSec, UUIDGen.getTimeUUID());
               Throwable var8 = null;

               try {
                  StatsMetadata metadata = this.sstable.getSSTableMetadata();
                  writer.switchWriter(this.createCompactionWriter(metadata.repairedAt, metadata.pendingRepair));

                  while(iter.hasNext()) {
                     writer.append(iter.next());
                  }

                  writer.finish();
                  this.outputHandler.output("Upgrade of " + this.sstable + " complete.");
               } catch (Throwable var73) {
                  var8 = var73;
                  throw var73;
               } finally {
                  if(iter != null) {
                     if(var8 != null) {
                        try {
                           iter.close();
                        } catch (Throwable var72) {
                           var8.addSuppressed(var72);
                        }
                     } else {
                        iter.close();
                     }
                  }

               }
            } catch (Throwable var75) {
               var6 = var75;
               throw var75;
            } finally {
               if(scanners != null) {
                  if(var6 != null) {
                     try {
                        scanners.close();
                     } catch (Throwable var71) {
                        var6.addSuppressed(var71);
                     }
                  } else {
                     scanners.close();
                  }
               }

            }
         } catch (Throwable var77) {
            var4 = var77;
            throw var77;
         } finally {
            if(writer != null) {
               if(var4 != null) {
                  try {
                     writer.close();
                  } catch (Throwable var70) {
                     var4.addSuppressed(var70);
                  }
               } else {
                  writer.close();
               }
            }

         }
      } catch (Exception var79) {
         throw Throwables.propagate(var79);
      } finally {
         this.controller.close();
      }
   }

   private static class UpgradeController extends CompactionController {
      public UpgradeController(ColumnFamilyStore cfs) {
         super(cfs, 2147483647);
      }

      public LongPredicate getPurgeEvaluator(DecoratedKey key) {
         return (time) -> {
            return false;
         };
      }
   }
}
