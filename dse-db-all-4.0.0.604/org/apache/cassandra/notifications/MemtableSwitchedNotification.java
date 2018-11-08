package org.apache.cassandra.notifications;

import org.apache.cassandra.db.Memtable;

public class MemtableSwitchedNotification implements INotification {
   public final Memtable memtable;

   public MemtableSwitchedNotification(Memtable switched) {
      this.memtable = switched;
   }
}
