package org.apache.cassandra.streaming;

import org.apache.cassandra.schema.TableId;

public abstract class StreamTask {
   protected final StreamSession session;
   protected final TableId tableId;

   protected StreamTask(StreamSession session, TableId tableId) {
      this.session = session;
      this.tableId = tableId;
   }

   public abstract int getTotalNumberOfFiles();

   public abstract long getTotalSize();

   public abstract void abort();

   public StreamSummary getSummary() {
      return new StreamSummary(this.tableId, this.getTotalNumberOfFiles(), this.getTotalSize());
   }
}
