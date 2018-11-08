package org.apache.cassandra.io.sstable;

import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadataRef;

class SSTableSimpleWriter extends AbstractSSTableSimpleWriter {
   protected DecoratedKey currentKey;
   protected PartitionUpdate update;
   private SSTableTxnWriter writer;

   protected SSTableSimpleWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns) {
      super(directory, metadata, columns);
   }

   private SSTableTxnWriter getOrCreateWriter() {
      if(this.writer == null) {
         this.writer = this.createWriter();
      }

      return this.writer;
   }

   PartitionUpdate getUpdateFor(DecoratedKey key) throws IOException {
      assert key != null;

      if(!key.equals(this.currentKey)) {
         if(this.update != null) {
            this.writePartition(this.update);
         }

         this.currentKey = key;
         this.update = new PartitionUpdate(this.metadata.get(), this.currentKey, this.columns, 4);
      }

      assert this.update != null;

      return this.update;
   }

   public void close() {
      try {
         if(this.update != null) {
            this.writePartition(this.update);
         }

         if(this.writer != null) {
            this.writer.finish(false);
         }

      } catch (Throwable var2) {
         throw Throwables.propagate(this.writer == null?var2:this.writer.abort(var2));
      }
   }

   private void writePartition(PartitionUpdate update) throws IOException {
      this.getOrCreateWriter().append(update.unfilteredIterator());
   }
}
