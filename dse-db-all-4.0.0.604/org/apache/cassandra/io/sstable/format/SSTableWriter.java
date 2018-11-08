package org.apache.cassandra.io.sstable.format;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Transactional;

public abstract class SSTableWriter extends SSTable implements Transactional {
   protected long repairedAt;
   protected UUID pendingRepair;
   protected long maxDataAge = -1L;
   protected final long keyCount;
   protected final MetadataCollector metadataCollector;
   protected final SerializationHeader header;
   protected final SSTableWriter.TransactionalProxy txnProxy = this.txnProxy();
   protected final Collection<SSTableFlushObserver> observers;

   protected abstract SSTableWriter.TransactionalProxy txnProxy();

   protected SSTableWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, TableMetadataRef metadata, MetadataCollector metadataCollector, SerializationHeader header, Collection<SSTableFlushObserver> observers) {
      super(descriptor, components(metadata.get()), metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
      this.keyCount = keyCount;
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.metadataCollector = metadataCollector;
      this.header = header;
      this.observers = (Collection)(observers == null?Collections.emptySet():observers);
   }

   public static SSTableWriter create(Descriptor descriptor, Long keyCount, Long repairedAt, UUID pendingRepair, TableMetadataRef metadata, MetadataCollector metadataCollector, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      SSTableWriter.Factory writerFactory = descriptor.getFormat().getWriterFactory();
      return writerFactory.open(descriptor, keyCount.longValue(), repairedAt.longValue(), pendingRepair, metadata, metadataCollector, header, observers(descriptor, indexes, txn.opType()), txn);
   }

   public static SSTableWriter create(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      TableMetadataRef metadata = Schema.instance.getTableMetadataRef(descriptor);
      return create(metadata, descriptor, keyCount, repairedAt, pendingRepair, sstableLevel, header, indexes, txn);
   }

   public static SSTableWriter create(TableMetadataRef metadata, Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      MetadataCollector collector = (new MetadataCollector(metadata.get().comparator)).sstableLevel(sstableLevel);
      return create(descriptor, Long.valueOf(keyCount), Long.valueOf(repairedAt), pendingRepair, metadata, collector, header, indexes, txn);
   }

   @VisibleForTesting
   public static SSTableWriter create(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      return create(descriptor, keyCount, repairedAt, pendingRepair, 0, header, indexes, txn);
   }

   private static Set<Component> components(TableMetadata metadata) {
      Set<Component> components = new HashSet(Arrays.asList(new Component[]{Component.DATA, Component.PARTITION_INDEX, Component.ROW_INDEX, Component.STATS, Component.TOC, Component.DIGEST}));
      if(metadata.params.bloomFilterFpChance < 1.0D) {
         components.add(Component.FILTER);
      }

      if(metadata.params.compression.isEnabled()) {
         components.add(Component.COMPRESSION_INFO);
      } else {
         components.add(Component.CRC);
      }

      return components;
   }

   private static Collection<SSTableFlushObserver> observers(Descriptor descriptor, Collection<Index> indexes, OperationType operationType) {
      if(indexes == null) {
         return Collections.emptyList();
      } else {
         List<SSTableFlushObserver> observers = new ArrayList(indexes.size());
         Iterator var4 = indexes.iterator();

         while(var4.hasNext()) {
            Index index = (Index)var4.next();
            SSTableFlushObserver observer = index.getFlushObserver(descriptor, operationType);
            if(observer != null) {
               observer.begin();
               observers.add(observer);
            }
         }

         return ImmutableList.copyOf(observers);
      }
   }

   public abstract void mark();

   public abstract RowIndexEntry append(UnfilteredRowIterator var1);

   public abstract long getFilePointer();

   public abstract long getOnDiskFilePointer();

   public long getEstimatedOnDiskBytesWritten() {
      return this.getOnDiskFilePointer();
   }

   public abstract void resetAndTruncate();

   public SSTableWriter setRepairedAt(long repairedAt) {
      if(repairedAt > 0L) {
         this.repairedAt = repairedAt;
      }

      return this;
   }

   public SSTableWriter setMaxDataAge(long maxDataAge) {
      this.maxDataAge = maxDataAge;
      return this;
   }

   public SSTableWriter setOpenResult(boolean openResult) {
      this.txnProxy.openResult = openResult;
      return this;
   }

   public abstract boolean openEarly(Consumer<SSTableReader> var1);

   public abstract SSTableReader openFinalEarly();

   public SSTableReader finish(long repairedAt, long maxDataAge, boolean openResult) {
      if(repairedAt > 0L) {
         this.repairedAt = repairedAt;
      }

      this.maxDataAge = maxDataAge;
      return this.finish(openResult);
   }

   public SSTableReader finish(boolean openResult) {
      this.setOpenResult(openResult);
      this.txnProxy.finish();
      this.observers.forEach(SSTableFlushObserver::complete);
      return this.finished();
   }

   public SSTableReader finished() {
      return this.txnProxy.finalReader;
   }

   public final void prepareToCommit() {
      this.txnProxy.prepareToCommit();
   }

   public final Throwable commit(Throwable accumulate) {
      Throwable var2;
      try {
         var2 = this.txnProxy.commit(accumulate);
      } finally {
         this.observers.forEach(SSTableFlushObserver::complete);
      }

      return var2;
   }

   public final Throwable abort(Throwable accumulate) {
      return this.txnProxy.abort(accumulate);
   }

   public final void close() {
      this.txnProxy.close();
   }

   public final void abort() {
      this.txnProxy.abort();
   }

   protected Map<MetadataType, MetadataComponent> finalizeMetadata() {
      return this.metadataCollector.finalizeMetadata(this.getPartitioner().getClass().getCanonicalName(), this.metadata().params.bloomFilterFpChance, this.repairedAt, this.pendingRepair, this.header);
   }

   protected StatsMetadata statsMetadata() {
      return (StatsMetadata)this.finalizeMetadata().get(MetadataType.STATS);
   }

   public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components) {
      List<Pair<File, File>> renames = new ArrayList();
      Iterator var4 = Sets.difference(components, ImmutableSet.of(Component.DATA)).iterator();

      while(var4.hasNext()) {
         Component component = (Component)var4.next();
         renames.add(Pair.create(new File(tmpdesc.filenameFor(component)), new File(newdesc.filenameFor(component))));
      }

      renames.add(Pair.create(new File(tmpdesc.filenameFor(Component.DATA)), new File(newdesc.filenameFor(Component.DATA))));
      List<File> nonExisting = null;
      Iterator var8 = renames.iterator();

      Pair rename;
      while(var8.hasNext()) {
         rename = (Pair)var8.next();
         if(!((File)rename.left).exists()) {
            if(nonExisting == null) {
               nonExisting = new ArrayList();
            }

            nonExisting.add(rename.left);
         }
      }

      if(nonExisting != null) {
         throw new AssertionError("One or more of the required components for the rename does not exist: " + nonExisting);
      } else {
         var8 = renames.iterator();

         while(var8.hasNext()) {
            rename = (Pair)var8.next();
            FileUtils.renameWithConfirm((File)rename.left, (File)rename.right);
         }

      }
   }

   public abstract static class Factory {
      public Factory() {
      }

      public abstract SSTableWriter open(Descriptor var1, long var2, long var4, UUID var6, TableMetadataRef var7, MetadataCollector var8, SerializationHeader var9, Collection<SSTableFlushObserver> var10, LifecycleTransaction var11);
   }

   protected abstract class TransactionalProxy extends Transactional.AbstractTransactional {
      protected SSTableReader finalReader;
      protected boolean openResult;

      protected TransactionalProxy() {
      }
   }
}
