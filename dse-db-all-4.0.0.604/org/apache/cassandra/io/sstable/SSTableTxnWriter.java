package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Transactional;

public class SSTableTxnWriter extends Transactional.AbstractTransactional implements Transactional {
   private final LifecycleTransaction txn;
   private final SSTableMultiWriter writer;

   public SSTableTxnWriter(LifecycleTransaction txn, SSTableMultiWriter writer) {
      this.txn = txn;
      this.writer = writer;
   }

   public boolean append(UnfilteredRowIterator iterator) {
      return this.writer.append(iterator);
   }

   public String getFilename() {
      return this.writer.getFilename();
   }

   public long getFilePointer() {
      return this.writer.getFilePointer();
   }

   protected Throwable doCommit(Throwable accumulate) {
      return this.writer.commit(this.txn.commit(accumulate));
   }

   protected Throwable doAbort(Throwable accumulate) {
      return this.txn.abort(this.writer.abort(accumulate));
   }

   protected void doPrepare() {
      this.writer.prepareToCommit();
      this.txn.prepareToCommit();
   }

   protected Throwable doPostCleanup(Throwable accumulate) {
      this.txn.close();
      this.writer.close();
      return super.doPostCleanup(accumulate);
   }

   public Collection<SSTableReader> finish(boolean openResult) {
      this.writer.setOpenResult(openResult);
      this.finish();
      return this.writer.finished();
   }

   public static SSTableTxnWriter create(ColumnFamilyStore cfs, Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header) {
      LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
      SSTableMultiWriter writer = cfs.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, sstableLevel, header, txn);
      return new SSTableTxnWriter(txn, writer);
   }

   public static SSTableTxnWriter createRangeAware(TableMetadataRef metadata, long keyCount, long repairedAt, UUID pendingRepair, SSTableFormat.Type type, int sstableLevel, SerializationHeader header) {
      ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
      LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

      RangeAwareSSTableWriter writer;
      try {
         writer = new RangeAwareSSTableWriter(cfs, keyCount, repairedAt, pendingRepair, type, sstableLevel, 0L, txn, header);
      } catch (IOException var13) {
         throw new RuntimeException(var13);
      }

      return new SSTableTxnWriter(txn, writer);
   }

   public static SSTableTxnWriter create(TableMetadataRef metadata, Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, Collection<Index> indexes) {
      LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
      MetadataCollector collector = (new MetadataCollector(metadata.get().comparator)).sstableLevel(sstableLevel);
      SSTableMultiWriter writer = SimpleSSTableMultiWriter.create(descriptor, keyCount, repairedAt, pendingRepair, metadata, collector, header, indexes, txn);
      return new SSTableTxnWriter(txn, writer);
   }

   public static SSTableTxnWriter create(ColumnFamilyStore cfs, Descriptor desc, long keyCount, long repairedAt, UUID pendingRepair, SerializationHeader header) {
      return create(cfs, desc, keyCount, repairedAt, pendingRepair, 0, header);
   }
}
