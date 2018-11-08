package org.apache.cassandra.notifications;

import org.apache.cassandra.db.Memtable;

public class MemtableDiscardedNotification implements INotification {
   public final Memtable memtable;

   public MemtableDiscardedNotification(Memtable discarded) {
      this.memtable = discarded;
   }
}
