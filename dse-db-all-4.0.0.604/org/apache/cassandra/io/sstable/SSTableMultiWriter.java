package org.apache.cassandra.io.sstable;

import java.util.Collection;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

public interface SSTableMultiWriter extends Transactional {
   boolean append(UnfilteredRowIterator var1);

   Collection<SSTableReader> finish(long var1, long var3, boolean var5);

   Collection<SSTableReader> finish(boolean var1);

   Collection<SSTableReader> finished();

   SSTableMultiWriter setOpenResult(boolean var1);

   String getFilename();

   long getFilePointer();

   TableId getTableId();

   static default void abortOrDie(SSTableMultiWriter writer) {
      Throwables.maybeFail(writer.abort((Throwable)null));
   }
}
