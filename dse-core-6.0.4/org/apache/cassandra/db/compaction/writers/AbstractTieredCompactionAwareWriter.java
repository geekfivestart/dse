package org.apache.cassandra.db.compaction.writers;

import com.datastax.bdp.cassandra.db.tiered.RangeAwareWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredRangeAwareSSTableWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredRowWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageStrategy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional.State;

public abstract class AbstractTieredCompactionAwareWriter extends CompactionAwareWriter {
   private final AbstractTieredCompactionAwareWriter.CompactionRowWriter rowWriter;

   public AbstractTieredCompactionAwareWriter(TieredStorageStrategy strategy, ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals) {
      super(cfs, directories, cfs.getTracker().tryModify(Collections.emptyList(), OperationType.COMPACTION), nonExpiredSSTables, keepOriginals);
      this.rowWriter = this.createRowWriter(strategy, txn);
      Throwables.maybeFail(this.sstableWriter.abort((Throwable)null));
   }

   protected abstract AbstractTieredCompactionAwareWriter.CompactionRowWriter createRowWriter(TieredStorageStrategy var1, LifecycleTransaction var2);

   protected boolean realAppend(UnfilteredRowIterator partition) {
      return this.rowWriter.append(partition);
   }

   protected void switchCompactionLocation(DataDirectory dataDirectory) {
      assert this.sstableWriter.currentWriter() == null;

   }

   protected Throwable doAbort(Throwable accumulate) {
      return this.rowWriter.abort(accumulate);
   }

   protected Throwable doCommit(Throwable accumulate) {
      return this.rowWriter.commit(accumulate);
   }

   protected void doPrepare() {
      this.rowWriter.prepareToCommit();
   }

   public Collection<SSTableReader> finish() {
      assert this.sstableWriter.currentWriter() == null;

      this.prepareToCommit();
      this.commit();
      return this.rowWriter.finish();
   }

   protected abstract static class CompactionRowWriter extends TieredRowWriter {
      protected final LifecycleTransaction transaction;
      protected final Set<SSTableReader> readers = new HashSet();

      public CompactionRowWriter(TieredStorageStrategy strategy, LifecycleTransaction transaction) {
         super(strategy);
         this.transaction = transaction;
      }

      protected void doPrepare() {
         super.doPrepare();
         Iterator var1 = this.writersForTxn().iterator();

         while(true) {
            RangeAwareWriter writer;
            do {
               if(!var1.hasNext()) {
                  return;
               }

               writer = (RangeAwareWriter)var1.next();
            } while(!(writer instanceof TieredRangeAwareSSTableWriter));

            Collection<SSTableReader> readers = ((TieredRangeAwareSSTableWriter)writer).finished();
            Iterator var4 = readers.iterator();

            while(var4.hasNext()) {
               SSTableReader reader = (SSTableReader)var4.next();
               this.transaction.update(reader, false);
               readers.add(reader);
            }
         }
      }

      public Set<SSTableReader> finish() {
         assert this.state() == State.COMMITTED || this.state() == State.READY_TO_COMMIT;

         return this.readers;
      }
   }
}
