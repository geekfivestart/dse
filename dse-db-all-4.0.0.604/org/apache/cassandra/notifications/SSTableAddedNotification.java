package org.apache.cassandra.notifications;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableAddedNotification implements INotification {
   public final Iterable<SSTableReader> added;
   @Nullable
   private final Memtable memtable;
   public final boolean fromStream;

   public SSTableAddedNotification(Iterable<SSTableReader> added, @Nullable Memtable memtable, boolean fromStream) {
      this.added = added;
      this.fromStream = fromStream;
      this.memtable = memtable;
   }

   public Optional<Memtable> memtable() {
      return Optional.ofNullable(this.memtable);
   }
}
