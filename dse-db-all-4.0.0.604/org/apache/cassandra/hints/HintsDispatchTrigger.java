package org.apache.cassandra.hints;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.Schema;

final class HintsDispatchTrigger implements Runnable {
   private final HintsCatalog catalog;
   private final HintsWriteExecutor writeExecutor;
   private final HintsDispatchExecutor dispatchExecutor;
   private final AtomicBoolean isPaused;

   HintsDispatchTrigger(HintsCatalog catalog, HintsWriteExecutor writeExecutor, HintsDispatchExecutor dispatchExecutor, AtomicBoolean isPaused) {
      this.catalog = catalog;
      this.writeExecutor = writeExecutor;
      this.dispatchExecutor = dispatchExecutor;
      this.isPaused = isPaused;
   }

   public void run() {
      if(!this.isPaused.get()) {
         this.catalog.stores().filter((store) -> {
            return !this.isScheduled(store);
         }).filter(HintsStore::isReadyToDispatchHints).filter((store) -> {
            return store.isWriting() || store.hasFiles();
         }).filter((store) -> {
            return Schema.instance.isSameVersion(Gossiper.instance.getSchemaVersion(store.address()));
         }).forEach(this::schedule);
      }
   }

   private void schedule(HintsStore store) {
      if(store.hasFiles()) {
         this.dispatchExecutor.dispatch(store);
      }

      if(store.isWriting()) {
         this.writeExecutor.closeWriter(store);
      }

   }

   private boolean isScheduled(HintsStore store) {
      return this.dispatchExecutor.isScheduled(store);
   }
}
