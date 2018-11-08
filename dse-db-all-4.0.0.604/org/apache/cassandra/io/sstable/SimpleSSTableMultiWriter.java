package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;

public class SimpleSSTableMultiWriter implements SSTableMultiWriter {
   private final SSTableWriter writer;
   private final LifecycleTransaction txn;

   protected SimpleSSTableMultiWriter(SSTableWriter writer, LifecycleTransaction txn) {
      this.txn = txn;
      this.writer = writer;
   }

   public boolean append(UnfilteredRowIterator partition) {
      RowIndexEntry indexEntry = this.writer.append(partition);
      return indexEntry != null;
   }

   public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult) {
      return Collections.singleton(this.writer.finish(repairedAt, maxDataAge, openResult));
   }

   public Collection<SSTableReader> finish(boolean openResult) {
      return Collections.singleton(this.writer.finish(openResult));
   }

   public Collection<SSTableReader> finished() {
      return Collections.singleton(this.writer.finished());
   }

   public SSTableMultiWriter setOpenResult(boolean openResult) {
      this.writer.setOpenResult(openResult);
      return this;
   }

   public String getFilename() {
      return this.writer.getFilename();
   }

   public long getFilePointer() {
      return this.writer.getFilePointer();
   }

   public TableId getTableId() {
      return this.writer.metadata().id;
   }

   public Throwable commit(Throwable accumulate) {
      return this.writer.commit(accumulate);
   }

   public Throwable abort(Throwable accumulate) {
      this.txn.untrackNew(this.writer);
      return this.writer.abort(accumulate);
   }

   public void prepareToCommit() {
      this.writer.prepareToCommit();
   }

   public void close() {
      this.writer.close();
   }

   public static SSTableMultiWriter create(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, TableMetadataRef metadata, MetadataCollector metadataCollector, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      SSTableWriter writer = SSTableWriter.create(descriptor, Long.valueOf(keyCount), Long.valueOf(repairedAt), pendingRepair, metadata, metadataCollector, header, indexes, txn);
      return new SimpleSSTableMultiWriter(writer, txn);
   }
}
