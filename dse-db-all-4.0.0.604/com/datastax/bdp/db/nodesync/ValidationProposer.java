package com.datastax.bdp.db.nodesync;

import org.apache.cassandra.schema.TableMetadata;

abstract class ValidationProposer {
   protected final TableState state;

   ValidationProposer(TableState state) {
      this.state = state;
   }

   NodeSyncService service() {
      return this.state.service();
   }

   public TableMetadata table() {
      return this.state.table();
   }

   abstract boolean isCancelled();

   abstract boolean cancel();
}
